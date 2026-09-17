# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Operational functions for MWAA Serverless workflow management."""

import base64
import json
import logging
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3
import yaml

log = logging.getLogger(__name__)

_client = None

# Terminal run states. Note SUCCESS here means "the run finished", NOT "every task
# succeeded" — verify_run_tasks() exists because those are different things.
TERMINAL_RUN_STATES = {"SUCCESS", "FAILED", "STOPPED", "TIMEOUT"}

# Objects written to the caller's bucket are encrypted with SSE-S3 by default. A
# sample must not write unencrypted objects, and the header is free.
_SSE_ALGORITHM = "AES256"

# Guard on the code bundle before it is decoded: base64 inflates by 4/3, and this
# function has 512 MB of memory, so an oversized payload is a cheap way to OOM it.
#
# Two separate ceilings apply and they are an order of magnitude apart. MWAA Serverless
# accepts 250 MB of code, but a bundle passed INLINE has to fit in the request carrying
# it, and a Lambda request is capped at 6 MB
# (https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html). Enforcing only the
# service quota means a remote caller gets a transport failure instead of a clear error,
# somewhere around 4 MB of actual zip. Anything larger belongs in S3, referenced by
# code_s3_key — which is the right pattern for a real bundle regardless.
_MWAA_MAX_CODE_BYTES = 250 * 1024 * 1024
_LAMBDA_SYNC_PAYLOAD = 6 * 1024 * 1024
_MAX_INLINE_CODE_BYTES = int((_LAMBDA_SYNC_PAYLOAD - 256 * 1024) * 3 / 4)

# Name prefix for the throwaway workflow preflight_definition creates.
#
# This is an IAM CONTRACT, not a cosmetic choice. template.yaml grants DeleteWorkflow on
# workflow/preflight-* WITHOUT gating it on AllowWorkflowDeletion, so that preflight can
# clean up after itself even on a deployment where the general delete tool is switched
# off. Change this prefix and preflight silently loses the right to delete its own
# throwaway, which leaks one workflow per call against the 100-per-account quota.
# tests/test_security.py asserts the two stay in step.
_PREFLIGHT_NAME_PREFIX = "preflight-"

# How many runs a scanning tool will walk per workflow. Each run inspected costs extra
# API calls (run detail, log streams), so these tools are bounded rather than complete.
# Every result built with this cap carries a truncation flag: a bound nobody can see is
# indistinguishable from "there was nothing there".
_MAX_RUNS_SCANNED = 200


def _configured_bucket_owner():
    """Account id the deployment says must own the target bucket.

    Set from the stack (EXPECTED_BUCKET_OWNER in template.yaml). This is the value that
    matters: a caller-supplied ExpectedBucketOwner is the caller asserting a claim about
    a bucket it chose, which guards nothing. When the stack sets this, it is the
    deployment asserting the claim, and a caller cannot weaken it.
    """
    return os.environ.get("EXPECTED_BUCKET_OWNER", "").strip()


def _configured_bucket():
    """Bucket this deployment is scoped to, if any (WORKFLOW_BUCKET in template.yaml)."""
    return os.environ.get("WORKFLOW_BUCKET", "").strip()


def _put_object(s3, bucket, key, body, expected_bucket_owner=""):
    """Write to S3 with encryption and, when known, an owner assertion.

    ExpectedBucketOwner is the standard confused-deputy guard: without it, a caller
    who can reach this function can direct it to write into any bucket the execution
    role happens to have access to.

    The stack's configured owner WINS over the caller's argument. S3 ARNs carry no
    account id, so an identity policy naming a bucket says nothing about who owns it;
    if the deployment has declared the owning account, that is the assertion to send,
    and a caller must not be able to substitute its own.
    """
    kwargs = {"Bucket": bucket, "Key": key, "Body": body,
              "ServerSideEncryption": _SSE_ALGORITHM}
    owner = _configured_bucket_owner() or expected_bucket_owner
    if owner:
        kwargs["ExpectedBucketOwner"] = owner
    return s3.put_object(**kwargs)

# Upper bound for a single poll call, kept below the Lambda timeout (120s in
# template.yaml) so the function returns the current status instead of being killed.
# Configurable via mcp_config.json / MWAA_MCP_MAX_POLL_SECONDS.
def _poll_limits():
    import config
    cfg = config.load()
    return cfg["default_poll_seconds"], cfg["max_poll_seconds"]

# TaskInstances come back as strings shaped 'ex_<uuid>_<task_id>_<attempt>'.
_TASK_INSTANCE_RE = re.compile(
    r"^ex_[0-9a-fA-F-]{36}_(?P<task_id>.+)_(?P<attempt>\d+)$"
)


def _client_error_message(exc) -> str:
    return (
        f"This runtime's boto3 does not know the 'mwaa-serverless' service, so no MWAA "
        f"Serverless client can be created. Install boto3 >= 1.40 "
        f"(see src/requirements.txt). Underlying error: {exc}\n\n"
        f"Note: older botocore does expose a legacy 'airflow-serverless' service, but it "
        f"resolves to airflow-serverless.<region>.amazonaws.com, which does not exist — the "
        f"live endpoint is airflow-serverless.<region>.api.aws. Falling back to it produces "
        f"confusing DNS/EndpointConnectionError failures, so it is deliberately not used."
    )


def _boto_config():
    """Explicit timeout and retry budget for every client this module creates.

    Without this, botocore defaults apply: a 60-second read timeout with retries on
    top. Inside a 120-second Lambda that is enough for one hung call to consume the
    whole invocation and be killed mid-flight, returning nothing — the caller sees a
    transport error rather than a status. Bounding the per-call budget means a slow
    dependency degrades into a reported timeout instead of a dead invocation.

    Adaptive retry mode also respects the service's throttling signals rather than
    hammering through them, which matters when several tools poll at once.
    """
    from botocore.config import Config

    return Config(
        connect_timeout=5,
        read_timeout=20,
        retries={"max_attempts": 3, "mode": "adaptive"},
    )


def _remaining_seconds(default):
    """Seconds left in this Lambda invocation, or `default` when not in Lambda.

    The poll budget used to be a constant chosen to sit under the configured Lambda
    timeout, which is a guess in two ways: it does not know how much of the invocation
    is already spent, and it goes stale the moment Timeout changes in template.yaml.
    The runtime knows the real answer, so ask it.
    """
    ctx = _LAMBDA_CONTEXT.get("context")
    if ctx is None or not hasattr(ctx, "get_remaining_time_in_millis"):
        return default
    try:
        # Leave headroom for serialising the response and for the error path; a budget
        # that consumes every remaining millisecond still gets killed.
        remaining = ctx.get_remaining_time_in_millis() / 1000.0
        return max(5, remaining - _RESPONSE_HEADROOM_SECONDS)
    except Exception:  # noqa: BLE001 - a broken context must not break polling
        return default


# Set by the Lambda handler so operations can see the real deadline. A module global
# rather than a parameter threaded through a dozen signatures, because only the poll
# loops need it and Lambda gives one invocation at a time per container.
_LAMBDA_CONTEXT = {}
_RESPONSE_HEADROOM_SECONDS = 10


def set_lambda_context(context):
    """Record the current invocation's context. Called by the Lambda handler.

    Local stdio mode never calls this, so _remaining_seconds falls back to the
    configured budget — correct, because a local process has no invocation deadline.
    """
    _LAMBDA_CONTEXT["context"] = context


def _get_client():
    """Cached boto3 client for MWAA Serverless.

    Requires a boto3 that registers the 'mwaa-serverless' service (>= 1.40), which is
    also the version that added the `Code` parameter needed for Python/Bash tasks.
    """
    global _client
    if _client is None:
        try:
            _client = boto3.client("mwaa-serverless", config=_boto_config())
        except Exception as e:  # UnknownServiceError on older botocore
            raise RuntimeError(_client_error_message(e)) from e
    return _client


def _supports_code_param() -> bool:
    """Whether the bundled botocore model exposes CreateWorkflow.Code."""
    try:
        shape = _get_client().meta.service_model.operation_model("CreateWorkflow").input_shape
        return "Code" in shape.members
    except Exception:
        return False


def _parse_task_instance(entry):
    """Normalise one TaskInstances entry into {task_id, attempt, raw}."""
    if isinstance(entry, dict):
        return {
            "task_id": entry.get("TaskId") or entry.get("task_id") or "",
            "status": entry.get("Status") or entry.get("state") or "",
            "error": entry.get("Error") or entry.get("ErrorMessage") or "",
            "attempt": entry.get("Attempt"),
            "raw": entry,
        }
    if isinstance(entry, str):
        m = _TASK_INSTANCE_RE.match(entry)
        if m:
            return {"task_id": m.group("task_id"), "status": "",
                    "error": "", "attempt": int(m.group("attempt")), "raw": entry}
        return {"task_id": entry, "status": "", "error": "", "attempt": None, "raw": entry}
    return {"task_id": str(entry), "status": "", "error": "", "attempt": None, "raw": entry}


def _new_client():
    """A fresh client for use inside a thread (boto3 clients are not thread-safe)."""
    try:
        return boto3.client("mwaa-serverless", config=_boto_config())
    except Exception as e:
        raise RuntimeError(_client_error_message(e)) from e


def _failed_tasks_in_run(workflow_arn: str, run_id: str, limit: int = 20) -> list:
    """Task ids whose log stream ends in final_state=failed.

    Cheap variant of verify_run_tasks used when scanning many runs: it only looks
    for the authoritative 'Task finished' marker and the first exception value.
    """
    logs_client = boto3.client("logs", config=_boto_config())
    wf_id = workflow_arn.rsplit("/", 1)[-1] if "/" in workflow_arn else workflow_arn
    log_group = _log_group_for(workflow_arn)
    out = []
    try:
        streams = logs_client.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=f"workflow_id={wf_id}/run_id={run_id}/",
            limit=limit,
        ).get("logStreams", [])
    except Exception as e:  # noqa: BLE001 - logged; caller sees an empty list
        log.warning("Could not list log streams for run %s in %s: %s", run_id, log_group, e)
        return out

    for stream in streams:
        sname = stream.get("logStreamName", "")
        task_id = next((p.split("=", 1)[1] for p in sname.split("/") if p.startswith("task_id=")), "")
        if not task_id:
            continue
        try:
            events = logs_client.get_log_events(
                logGroupName=log_group, logStreamName=sname, limit=200, startFromHead=False
            ).get("events", [])
        except Exception as e:  # noqa: BLE001 - logged; this stream is skipped
            log.warning("Could not read log stream %s: %s", sname, e)
            continue
        state, err = None, None
        for ev in events:
            try:
                p = json.loads(ev.get("message", ""))
            except (json.JSONDecodeError, TypeError):
                # Airflow also emits plain-text lines; only the JSON ones carry
                # final_state, so a non-JSON line is expected and not an error.
                continue
            if "final_state" in p:
                state = p.get("final_state")
            if p.get("level") == "error" and err is None:
                for d in (p.get("error_detail") or []):
                    if isinstance(d, dict) and d.get("exc_value"):
                        err = f"{d.get('exc_type')}: {str(d['exc_value'])[:250]}"
                        break
        if state == "failed":
            out.append({"task_id": task_id, "error": err or "see CloudWatch logs"})
    return out


def _log_group_for(workflow_arn: str) -> str:
    """CloudWatch log group for a workflow.

    Must be derived from the ARN: the ARN carries the suffixed workflow name
    (my-wf-a1b2c3d4e5) which is what the log group uses, whereas ListWorkflows
    returns the bare name and would give the wrong group.
    """
    wf_id = workflow_arn.rsplit("/", 1)[-1] if "/" in workflow_arn else workflow_arn
    return f"/aws/mwaa-serverless/{wf_id}/"


def _list_all_pages(client, operation, result_key, cap=None, **kwargs):
    """Follow pagination on any list API, returning (items, truncated).

    Only list_workflows was paginated. Runs and versions were read one page at a time
    in ten places, so any judgement built on that history — the latest run, whether a
    workflow has ever failed, whether it is idle enough to delete — silently used a
    prefix of the truth.

    `cap` bounds the walk for callers that only need recent history. When it stops
    early, `truncated` is True so the caller can say so rather than presenting a
    partial answer as complete. A safety cap that is invisible in the result is how a
    partial scan becomes a false negative.
    """
    items = []
    token = None
    truncated = False
    while True:
        call = dict(kwargs)
        if token:
            call["NextToken"] = token
        resp = getattr(client, operation)(**call)
        items.extend(resp.get(result_key) or [])
        token = resp.get("NextToken")
        if cap is not None and len(items) >= cap:
            truncated = bool(token)
            return items[:cap], truncated
        if not token:
            return items, truncated


def _list_runs(client, arn, cap=None):
    """Every run of a workflow, newest first, following pagination."""
    runs, truncated = _list_all_pages(
        client, "list_workflow_runs", "WorkflowRuns", cap=cap, WorkflowArn=arn
    )
    runs.sort(
        key=lambda r: str(r.get("RunDetailSummary", {}).get("CreatedOn", "")),
        reverse=True,
    )
    return runs, truncated


def _list_all_workflows(client=None) -> list:
    """Every workflow in the account, following pagination.

    Only the list_workflows tool used a paginator before; every name-resolution path
    called the single-page API, so past one page a workflow that exists reported as
    "No workflow found matching X" — intermittently, depending on where it landed.
    """
    client = client or _get_client()
    workflows = []
    try:
        paginator = client.get_paginator("list_workflows")
    except Exception:  # noqa: BLE001 - older botocore may not model the paginator
        log.debug("list_workflows has no paginator in this botocore; using NextToken manually")
        paginator = None

    if paginator is not None:
        for page in paginator.paginate():
            workflows.extend(page.get("Workflows", []))
        return workflows

    token = None
    while True:
        kwargs = {"NextToken": token} if token else {}
        resp = client.list_workflows(**kwargs)
        workflows.extend(resp.get("Workflows", []))
        token = resp.get("NextToken")
        if not token:
            return workflows


def _match_workflows(all_wf, workflow_name):
    """(exact_matches, partial_matches) for a name."""
    exact = [w for w in all_wf if w.get("Name", "") == workflow_name]
    lowered = workflow_name.lower()
    partial = [w for w in all_wf
               if lowered in w.get("Name", "").lower() and w.get("Name", "") != workflow_name]
    return exact, partial


def _resolve_workflow_arn(workflow_name: str, require_unambiguous: bool = False) -> tuple:
    """Resolve a workflow name to (arn, name, all_workflows).

    Returns (None, error_message, None) on failure.

    require_unambiguous=True is used by every MUTATING caller. Previously a partial
    match silently took match[0], so `stop_run("prod")` could kill the wrong
    workflow's run and `redeploy("prod")` could overwrite the wrong definition.
    """
    all_wf = _list_all_workflows()
    exact, partial = _match_workflows(all_wf, workflow_name)

    if exact:
        return exact[0]["WorkflowArn"], exact[0].get("Name", ""), all_wf

    if not partial:
        near = sorted(w.get("Name", "") for w in all_wf)[:10]
        hint = f" Existing workflows: {', '.join(near)}." if near else ""
        return None, f"No workflow found matching '{workflow_name}'.{hint}", None

    if len(partial) > 1 or require_unambiguous:
        names = sorted(w.get("Name", "") for w in partial)
        if len(partial) == 1 and require_unambiguous:
            return None, (
                f"'{workflow_name}' is not an exact workflow name. The only partial match is "
                f"'{names[0]}'. This operation modifies a workflow, so pass the exact name."
            ), None
        return None, (
            f"'{workflow_name}' matches {len(partial)} workflows: {', '.join(names)}. "
            f"Pass the exact name."
        ), None

    return partial[0]["WorkflowArn"], partial[0].get("Name", ""), all_wf


def _iter_tasks(dag):

    """Iterate tasks from a parsed DAG YAML, handling both list and dict formats.
    Also looks inside dag_id-keyed structures."""
    # Direct tasks key
    tasks = dag.get("tasks", [])
    if isinstance(tasks, list):
        yield from tasks
    elif isinstance(tasks, dict):
        for tid, tcfg in tasks.items():
            if isinstance(tcfg, dict):
                t = dict(tcfg)
                t.setdefault("task_id", tid)
                yield t
    # Check for dag_id-keyed structure: {dag_id: {tasks: ...}}
    for key, val in dag.items():
        if key == "tasks":
            continue
        if isinstance(val, dict) and "tasks" in val:
            inner = val["tasks"]
            if isinstance(inner, list):
                yield from inner
            elif isinstance(inner, dict):
                for tid, tcfg in inner.items():
                    if isinstance(tcfg, dict):
                        t = dict(tcfg)
                        t.setdefault("task_id", tid)
                        yield t


def list_workflows(name_contains: str = "", status: str = "") -> dict:
    """List all workflows, optionally filtered by name substring or status."""
    workflows = _list_all_workflows()

    if name_contains:
        name_lower = name_contains.lower()
        workflows = [w for w in workflows if name_lower in w.get("Name", "").lower()]
    if status:
        status_upper = status.upper()
        workflows = [w for w in workflows if w.get("WorkflowStatus", "").upper() == status_upper]

    result = [{
        "name": w.get("Name", ""),
        "status": w.get("WorkflowStatus", ""),
        "trigger_mode": w.get("TriggerMode", ""),
        "arn": w.get("WorkflowArn", ""),
        "modified": w.get("ModifiedAt", ""),
    } for w in workflows]

    return {"count": len(result), "workflows": result}


def get_workflow(workflow_name: str) -> dict:
    """Get detailed info about a workflow including its DAG YAML definition."""
    client = _get_client()

    arn, name, _ = _resolve_workflow_arn(workflow_name)
    if arn is None:
        return {"error": name}
    detail = client.get_workflow(WorkflowArn=arn)

    info = {
        "name": detail.get("Name", ""),
        "arn": arn,
        "status": detail.get("WorkflowStatus", ""),
        "trigger_mode": detail.get("TriggerMode", ""),
        "version": detail.get("WorkflowVersion", ""),
        "created": str(detail.get("CreatedAt", "")),
        "modified": str(detail.get("ModifiedAt", "")),
        "execution_role_arn": detail.get("RoleArn", detail.get("ExecutionRoleArn", "")),
    }

    # Prefer the inline WorkflowDefinition: it is the immutable snapshot the
    # workflow actually runs, needs no s3:GetObject, and stays correct even if
    # the S3 object has since been overwritten.
    s3_loc = detail.get("DefinitionS3Location", {})
    if s3_loc:
        info["definition_s3"] = {"bucket": s3_loc.get("Bucket", ""), "key": s3_loc.get("ObjectKey", "")}

    code_loc = (detail.get("Code") or {}).get("S3Location") or {}
    if code_loc:
        info["code_s3"] = {"bucket": code_loc.get("Bucket", ""), "key": code_loc.get("ObjectKey", "")}
        info["has_code_bundle"] = True
    if detail.get("CodeSnapshottedAt"):
        info["code_snapshotted_at"] = str(detail["CodeSnapshottedAt"])

    info["log_group"] = _log_group_for(arn)

    yaml_content = detail.get("WorkflowDefinition")
    if yaml_content:
        info["definition_source"] = "GetWorkflow.WorkflowDefinition (immutable snapshot)"
    elif s3_loc:
        try:
            s3 = boto3.client("s3", config=_boto_config())
            obj = s3.get_object(Bucket=s3_loc["Bucket"], Key=s3_loc["ObjectKey"])
            yaml_content = obj["Body"].read().decode("utf-8")
            info["definition_source"] = "S3 object (may differ from the deployed snapshot)"
        except Exception as e:
            info["dag_yaml_error"] = str(e)

    if yaml_content:
        info["dag_yaml"] = yaml_content
        try:
            dag = yaml.safe_load(yaml_content)
            operators = set()
            for t in _iter_tasks(dag):
                op = t.get("operator", "")
                if op:
                    operators.add(op)
            info["operators_used"] = sorted(operators)
        except yaml.YAMLError as e:
            info["operators_used_error"] = f"deployed definition does not parse: {e}"
        try:
            import validator
            v = validator.validate(yaml_content)
            if not v["valid"]:
                info["definition_validation_errors"] = v["errors"]
            if v["warnings"]:
                info["definition_validation_warnings"] = v["warnings"]
        except Exception as e:  # noqa: BLE001 - reported, not swallowed
            log.warning("Could not validate the deployed definition for %s: %s", name, e)
            info["definition_validation_error"] = f"validation could not run: {e}"

    return info


def start_workflow_run(workflow_name: str) -> dict:
    """Start a workflow run by name."""
    client = _get_client()

    # Mutating (it costs money and can double-run a pipeline), so require an
    # unambiguous name rather than silently taking the first substring match.
    arn, name, _ = _resolve_workflow_arn(workflow_name, require_unambiguous=True)
    if arn is None:
        return {"error": name}

    resp = client.start_workflow_run(WorkflowArn=arn)

    return {
        "workflow_name": name,
        "workflow_arn": arn,
        "run_id": resp.get("RunId", resp.get("WorkflowRunId", "")),
        "status": "STARTED",
    }


def _attempt_number(attempt) -> int:
    """Attempt as an int for ordering. Log stream names carry it as a string."""
    try:
        return int(str(attempt).strip() or 0)
    except (TypeError, ValueError):
        return 0


def _parse_timestamp(value):
    """Parse an ISO-ish timestamp into an aware datetime, or None.

    boto3 returns datetimes, but these paths stringify them first, and a run whose
    timestamp cannot be parsed must not be silently treated as old.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _sort_runs_newest_first(runs):
    return sorted(
        runs,
        key=lambda r: str(r.get("RunDetailSummary", {}).get("CreatedOn", r.get("StartedAt", ""))),
        reverse=True,
    )


def _latest_run_id(client, arn, statuses=None):
    """(run_id, error). Newest run, optionally restricted to certain statuses."""
    try:
        runs, _ = _list_runs(client, arn)
    except Exception as e:  # noqa: BLE001 - surfaced to the caller
        log.warning("list_workflow_runs failed for %s: %s", arn, e)
        return None, f"Failed to list runs: {e}"
    if statuses:
        runs = [r for r in runs
                if r.get("RunDetailSummary", {}).get("Status", "") in statuses]
    if not runs:
        return None, None
    return _sort_runs_newest_first(runs)[0].get("RunId", ""), None


def get_workflow_run_status(workflow_name: str, run_id: str = "") -> dict:
    """Get the status of a workflow run. If no run_id, gets the latest run."""
    client = _get_client()

    arn, name, _ = _resolve_workflow_arn(workflow_name)
    if arn is None:
        return {"error": name}

    if not run_id:
        run_id, err = _latest_run_id(client, arn)
        if err:
            return {"error": err}
        if not run_id:
            return {"workflow_name": name, "message": "No runs found"}

    try:
        run = client.get_workflow_run(WorkflowArn=arn, RunId=run_id)
    except Exception as e:  # noqa: BLE001 - surfaced to the caller
        return {"error": f"Failed to get run: {e}"}

    detail = run.get("RunDetail", {})
    result = {
        "workflow_name": name,
        "run_id": run_id,
        "status": detail.get("RunState", run.get("Status", "")),
        "started_at": str(detail.get("CreatedAt", run.get("StartedAt", ""))),
        "ended_at": str(detail.get("ModifiedAt", run.get("EndedAt", ""))),
    }

    if detail.get("ErrorMessage"):
        result["error_message"] = detail["ErrorMessage"]

    tasks = detail.get("TaskInstances", [])
    if tasks:
        if isinstance(tasks[0], str):
            result["task_instances"] = tasks
        else:
            result["tasks"] = []
            for t in tasks:
                task_info = {
                    "task_id": t.get("TaskId", t.get("task_id", "")),
                    "status": t.get("Status", t.get("state", "")),
                }
                if t.get("Error") or t.get("ErrorMessage"):
                    task_info["error"] = t.get("Error", t.get("ErrorMessage", ""))
                result["tasks"].append(task_info)

    if run.get("FailureReason"):
        result["failure_reason"] = run["FailureReason"]

    return result


def stop_workflow_run(workflow_name: str, run_id: str = "") -> dict:
    """Stop a running workflow. If no run_id, stops the latest active run."""
    client = _get_client()

    # Mutating: an exact name is required so a substring cannot kill another
    # workflow's run.
    arn, name, _ = _resolve_workflow_arn(workflow_name, require_unambiguous=True)
    if arn is None:
        return {"error": name}

    if not run_id:
        run_id, err = _latest_run_id(client, arn,
                                     statuses=("RUNNING", "STARTING", "QUEUED"))
        if err:
            return {"error": err}
        if not run_id:
            return {"workflow_name": name, "message": "No active runs to stop"}

    client.stop_workflow_run(WorkflowArn=arn, RunId=run_id)
    return {"workflow_name": name, "run_id": run_id, "status": "STOPPING"}


def find_workflows_using_service(service_keyword: str) -> dict:
    """Find workflows that use operators for a specific AWS service by inspecting DAG YAML definitions."""
    # Map common service names to operator substrings (short names + FQN fragments)
    service_map = {
        "step_functions": ["StepFunction", "step_function"],
        "step functions": ["StepFunction", "step_function"],
        "sfn": ["StepFunction", "step_function"],
        "s3": ["S3", ".s3."],
        "glue": ["Glue", ".glue."],
        "athena": ["Athena", ".athena."],
        "bedrock": ["Bedrock", ".bedrock."],
        "lambda": ["Lambda", ".lambda_function."],
        "emr": ["Emr", ".emr."],
        "batch": ["Batch", ".batch."],
        "ecs": ["Ecs", ".ecs."],
        "eks": ["Eks", ".eks."],
        "sqs": ["Sqs", ".sqs."],
        "sns": ["Sns", ".sns."],
        "redshift": ["Redshift", ".redshift"],
        "sagemaker": ["SageMaker", ".sagemaker."],
        "rds": ["Rds", ".rds."],
        "ec2": ["Ec2", ".ec2."],
        "eventbridge": ["EventBridge", ".eventbridge."],
        "cloudformation": ["CloudFormation", ".cloud_formation."],
        "comprehend": ["Comprehend", ".comprehend."],
        "dms": ["Dms", ".dms."],
        "neptune": ["Neptune", ".neptune."],
        "dynamodb": ["DynamoDB", ".dynamodb."],
        "datasync": ["DataSync", ".datasync."],
        "glacier": ["Glacier", ".glacier."],
        "appflow": ["Appflow", ".appflow."],
        "quicksight": ["QuickSight", ".quicksight."],
        "opensearch": ["OpenSearch", ".opensearch."],
        "kinesis": ["Kinesis", ".kinesis."],
    }

    keywords = service_map.get(service_keyword.lower(), [service_keyword])
    all_wf = _list_all_workflows()

    def _inspect(wf):
        arn = wf.get("WorkflowArn", "")
        try:
            detail = _new_client().get_workflow(WorkflowArn=arn)
            # Prefer the immutable snapshot; fall back to S3 only if it is absent.
            text = detail.get("WorkflowDefinition")
            if not text:
                s3_loc = detail.get("DefinitionS3Location", {})
                if not s3_loc:
                    return None, None
                obj = boto3.client("s3", config=_boto_config()).get_object(
                    Bucket=s3_loc["Bucket"], Key=s3_loc["ObjectKey"])
                text = obj["Body"].read().decode("utf-8")
            dag = yaml.safe_load(text)
            found = [
                {"task_id": t.get("task_id", ""), "operator": t.get("operator", "")}
                for t in _iter_tasks(dag)
                if any(kw.lower() in t.get("operator", "").lower() for kw in keywords)
            ]
            if found:
                return {"name": wf.get("Name", ""), "arn": arn, "matching_tasks": found}, None
        except Exception as e:  # noqa: BLE001 - reported, never swallowed
            log.warning("Could not inspect workflow %s: %s", wf.get("Name", arn), e)
            return None, f"{wf.get('Name', arn)}: {e}"
        return None, None

    matches, errors = [], []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_inspect, wf): wf for wf in all_wf}
        for f in as_completed(futures):
            result, err = f.result()
            if result:
                matches.append(result)
            if err:
                errors.append(err)

    out = {"service": service_keyword, "workflows_scanned": len(all_wf),
           "count": len(matches), "workflows": matches}
    if errors:
        # Without this, a permissions problem was indistinguishable from "no matches".
        out["not_inspected"] = errors
        out["incomplete_scan_warning"] = (
            f"{len(errors)} of {len(all_wf)} workflow(s) could not be read, so this result may "
            f"be missing matches. Fix the errors in not_inspected and re-run."
        )
    return out


def deploy_and_run(workflow_name: str, yaml_content: str, s3_bucket: str, execution_role_arn: str,
                   s3_key: str = "", code_zip_base64: str = "", code_s3_key: str = "",
                   trigger_mode: str = "", start_run: bool = True,
                   expected_bucket_owner: str = "") -> dict:
    """Upload the definition (and optional code bundle) to S3, create or update the
    workflow, and start a run.

    Pass code_zip_base64 when the DAG has PythonOperator or BashOperator tasks —
    their code is a separate S3 object referenced by the CreateWorkflow `Code`
    parameter, not part of the YAML.
    """
    client = _get_client()
    s3 = boto3.client("s3", config=_boto_config())

    if not s3_key:
        s3_key = f"workflows/{workflow_name}.yaml"

    # Validate before touching AWS — a bad definition is rejected anyway, and the
    # local error message is far more actionable than the service's.
    pre = None
    try:
        import validator
        pre = validator.validate(yaml_content)
    except Exception as e:  # noqa: BLE001 - a validator failure must not block deploy
        log.warning("Local validation could not run: %s", e)
    if pre and not pre["valid"]:
        return {
            "error": "Definition failed local validation; nothing was deployed.",
            "validation_errors": pre["errors"],
            "hint": "Call repair_dag_yaml to fix the mechanical problems, then redeploy.",
        }

    # Payload guards go here, BEFORE the first AWS call. They are pure local checks, and
    # running them after the definition upload meant an oversized bundle still left a
    # stray object in the caller's bucket on the way to failing.
    if code_zip_base64:
        # Size first. An oversized payload is refused whatever the runtime supports, and
        # the encoded length is checked BEFORE decoding: base64 expands by 4/3, and this
        # function has 512 MB, so decoding first is an easy way to OOM it.
        #
        # The bound is the TRANSPORT's, not the service's. A bundle between this and the
        # 250 MB MWAA quota is legal for MWAA but cannot fit in the request that would
        # carry it here, and failing with a clear message beats a truncated request.
        if len(code_zip_base64) > _MAX_INLINE_CODE_BYTES * 4 // 3:
            return {
                "error": (
                    f"code_zip_base64 decodes to more than "
                    f"{_MAX_INLINE_CODE_BYTES // (1024 * 1024)} MB, which is the largest bundle "
                    f"that can be passed INLINE. MWAA Serverless itself accepts up to "
                    f"{_MWAA_MAX_CODE_BYTES // (1024 * 1024)} MB — the smaller limit is the "
                    f"6 MB cap on a Lambda request, which base64 fills a third faster."
                ),
                "fix": (
                    "Upload the zip to S3 yourself and pass code_s3_key instead of "
                    "code_zip_base64. The workflow references the object, so nothing large "
                    "travels through this server. This is the recommended path for any "
                    "bundle big enough to hit this."
                ),
            }
        if not _supports_code_param():
            return {"error": "This runtime's botocore does not support the CreateWorkflow `Code` "
                             "parameter. Upgrade boto3 to >= 1.40 to deploy Python/Bash tasks."}
        try:
            blob = base64.b64decode(code_zip_base64, validate=True)
        except Exception as e:  # noqa: BLE001 - surfaced to the caller
            return {"error": f"code_zip_base64 is not valid base64: {e}"}
    else:
        blob = None

    try:
        _put_object(s3, s3_bucket, s3_key, yaml_content.encode("utf-8"), expected_bucket_owner)
    except Exception as e:  # noqa: BLE001 - surfaced to the caller
        return {"error": f"S3 upload of the definition failed: {e}"}

    s3_loc = {"Bucket": s3_bucket, "ObjectKey": s3_key}

    # Optional code bundle for Python/Bash tasks. Already validated and decoded above,
    # before any AWS call.
    code_arg = {}
    if code_zip_base64:
        ckey = code_s3_key or f"workflows/code/{workflow_name}.zip"
        try:
            _put_object(s3, s3_bucket, ckey, blob, expected_bucket_owner)
        except Exception as e:  # noqa: BLE001 - surfaced to the caller
            return {"error": f"S3 upload of the code bundle failed: {e}"}
        code_arg = {"Code": {"S3Location": {"Bucket": s3_bucket, "ObjectKey": ckey}}}
    elif code_s3_key:
        code_arg = {"Code": {"S3Location": {"Bucket": s3_bucket, "ObjectKey": code_s3_key}}}

    extra = dict(code_arg)
    if trigger_mode:
        extra["TriggerMode"] = trigger_mode

    warnings = []
    try:
        resp = client.create_workflow(
            Name=workflow_name,
            DefinitionS3Location=s3_loc,
            RoleArn=execution_role_arn,
            **extra,
        )
        arn = resp.get("WorkflowArn", "")
        warnings = resp.get("Warnings") or []
        action = "created"
    except client.exceptions.ConflictException:
        all_wf = _list_all_workflows(client)
        existing = [w for w in all_wf if w.get("Name", "") == workflow_name]
        if not existing:
            return {"error": f"Workflow '{workflow_name}' conflict but not found in list"}
        arn = existing[0]["WorkflowArn"]
        try:
            uresp = client.update_workflow(
                WorkflowArn=arn,
                DefinitionS3Location=s3_loc,
                RoleArn=execution_role_arn,
                **extra,
            )
            warnings = uresp.get("Warnings") or []
            action = "updated"
        except Exception as e:
            return {"error": f"Update failed: {str(e)}"}
    except Exception as e:
        return {"error": f"Create failed: {str(e)}",
                "hint": "The service validates the definition on create. The message above names "
                        "the offending task or argument."}

    result = {
        "workflow_name": workflow_name,
        "workflow_arn": arn,
        "action": action,
        "s3_location": s3_loc,
        "log_group": _log_group_for(arn),
    }
    if code_arg:
        result["code_location"] = code_arg["Code"]["S3Location"]
    if warnings:
        result["service_warnings"] = warnings
        result["service_warnings_note"] = (
            "The service accepted the definition but silently dropped these attributes. "
            "Remove them so the YAML reflects what actually runs."
        )
    if pre and pre.get("warnings"):
        result["validation_warnings"] = pre["warnings"]

    if not start_run:
        result["status"] = "DEPLOYED"
        return result

    try:
        run_resp = client.start_workflow_run(WorkflowArn=arn)
        result["run_id"] = run_resp.get("RunId", "")
        result["status"] = "STARTED"
        result["next_step"] = (
            "Poll with mwaa_poll_run, then ALWAYS confirm with mwaa_verify_run_tasks — a run can "
            "report SUCCESS while individual tasks failed."
        )
    except Exception as e:
        result["error"] = f"Workflow {action} but starting the run failed: {str(e)}"
    return result


def preflight_definition(yaml_content: str, s3_bucket: str, execution_role_arn: str,
                         code_zip_base64: str = "", expected_bucket_owner: str = "") -> dict:
    """Have MWAA Serverless itself validate a definition, then clean up.

    Creates a throwaway workflow to exercise the service's own validator, reports
    the verdict (including the Warnings list), and deletes it again. This catches
    anything the local validator cannot know about — such as an operator argument
    the installed provider version does not accept.

    `verdict` is three-state and is the field to branch on:
        "valid"         the service accepted the exact artifact you passed
        "invalid"       local or service validation rejected it; see the errors
        "indeterminate" the check could not be completed, so nothing is known.
                        `why_indeterminate` says what was missed.
    `valid` is a strict boolean alias: it is True only for "valid". It is never set
    from the LOCAL result — reporting a local pass as the verdict when the service
    never saw the definition is what makes automation deploy unvalidated YAML.

    Side effects the caller should know about: it writes (and deletes) two objects in
    s3_bucket, and it consumes one of the 100 workflows-per-account quota slots for
    the duration. If the delete fails, `cleanup` says so and names the leftover.
    One residue is NOT cleaned up: MWAA Serverless creates a CloudWatch log group
    for the throwaway workflow, and that log group outlives the workflow. It is left
    behind empty (0 bytes, no streams, no retention policy), so it costs nothing to
    store, but it accumulates one group per preflight call. `log_group_residue` in
    the result names it so you can delete it if you care.
    """
    import validator

    local = validator.validate(yaml_content)
    out = {"local_validation": {"valid": local["valid"], "errors": local["errors"],
                                "warnings": local["warnings"], "hints": local["hints"]}}
    if not local["valid"]:
        out["service_validation"] = "skipped — fix the local errors first"
        out["verdict"] = "invalid"
        out["valid"] = False
        return out

    client = _get_client()
    s3 = boto3.client("s3", config=_boto_config())
    probe = f"{_PREFLIGHT_NAME_PREFIX}{uuid.uuid4().hex[:8]}"
    key = f"preflight/{probe}.yaml"
    ckey = None
    arn = None
    try:
        _put_object(s3, s3_bucket, key, yaml_content.encode("utf-8"), expected_bucket_owner)
    except Exception as e:  # noqa: BLE001 - surfaced to the caller
        # FAIL CLOSED. The local result is not a substitute for the service's verdict,
        # and reporting the local one as `valid` here told automation the service had
        # accepted a definition it never saw.
        out["service_validation"] = f"skipped — S3 upload failed: {e}"
        out["verdict"] = "indeterminate"
        out["valid"] = False
        out["why_indeterminate"] = (
            f"The definition could not be staged in s3://{s3_bucket}, so MWAA Serverless "
            f"never validated it. Local validation passed, but local checks cannot know "
            f"what the installed provider versions accept — that is the whole reason to "
            f"preflight. Fix the S3 access and re-run before treating this as deployable."
        )
        return out

    extra = {}
    code_bundle_unstaged = False
    if code_zip_base64 and _supports_code_param():
        ckey = f"preflight/{probe}.zip"
        try:
            _put_object(s3, s3_bucket, ckey, base64.b64decode(code_zip_base64, validate=True),
                        expected_bucket_owner)
            extra["Code"] = {"S3Location": {"Bucket": s3_bucket, "ObjectKey": ckey}}
        except Exception as e:  # noqa: BLE001 - reported, never swallowed
            log.warning("Preflight code bundle upload failed: %s", e)
            ckey = None
            code_bundle_unstaged = True
            out["code_bundle_warning"] = (
                f"The code bundle could not be staged ({e}), so the service validated the "
                f"definition WITHOUT it. Python/Bash tasks were not checked."
            )

    try:
        resp = client.create_workflow(
            Name=probe,
            DefinitionS3Location={"Bucket": s3_bucket, "ObjectKey": key},
            RoleArn=execution_role_arn,
            **extra,
        )
        arn = resp.get("WorkflowArn")
        out["service_validation"] = "accepted"
        out["service_warnings"] = resp.get("Warnings") or []
        if code_bundle_unstaged:
            # The service accepted the DEFINITION, but not the artifact the caller
            # actually meant to check. Reporting that as valid would greenlight a
            # deployment whose Python and Bash tasks were never validated.
            out["verdict"] = "indeterminate"
            out["valid"] = False
            out["why_indeterminate"] = (
                "The service accepted the definition, but the code bundle was not staged, "
                "so the PythonOperator/BashOperator tasks in it were NOT validated. This is "
                "not a pass — stage the bundle and re-run."
            )
        else:
            out["verdict"] = "valid"
            out["valid"] = True
    except Exception as e:  # noqa: BLE001 - the rejection IS the result here
        msg = str(e)
        out["service_validation"] = "rejected"
        out["service_error"] = msg.split("Workflow validation failed:")[-1].strip() or msg
        out["verdict"] = "invalid"
        out["valid"] = False
    finally:
        if arn:
            out["log_group_residue"] = _log_group_for(arn)
            try:
                client.delete_workflow(WorkflowArn=arn)
                out["cleanup"] = "throwaway workflow deleted"
            except Exception as e:  # noqa: BLE001 - reported so the leftover is visible
                log.warning("Could not delete preflight workflow %s: %s", arn, e)
                out["cleanup"] = (
                    f"could not delete throwaway workflow {arn}: {e}. Delete it manually — it "
                    f"counts against the 100-workflow quota."
                )
                if "AccessDenied" in str(e) or "not authorized" in str(e):
                    out["cleanup_fix"] = (
                        f"This is an IAM gap, not a service error: the caller lacks "
                        f"airflow-serverless:DeleteWorkflow on {arn}. On the Lambda deployment "
                        f"that grant is the DeletePreflightThrowawayOnly statement, scoped to "
                        f"workflow/{_PREFLIGHT_NAME_PREFIX}* and NOT gated on "
                        f"AllowWorkflowDeletion. If it is missing, redeploy the current "
                        f"template.yaml. Every preflight call leaks one workflow until then, "
                        f"and 100 of them exhaust the account quota."
                    )
        leftovers = []
        for stale_key in [k for k in (key, ckey) if k]:
            try:
                s3.delete_object(Bucket=s3_bucket, Key=stale_key)
            except Exception as e:  # noqa: BLE001 - reported so the leftover is visible
                log.warning("Could not delete preflight object %s: %s", stale_key, e)
                leftovers.append(f"s3://{s3_bucket}/{stale_key} ({e})")
        if leftovers:
            out["cleanup_s3"] = f"could not delete: {', '.join(leftovers)}"

    if out.get("service_warnings"):
        out["service_warnings_note"] = (
            "The service will silently drop these attributes. Remove them from the definition."
        )
    if out.get("log_group_residue"):
        out["log_group_residue_note"] = (
            "MWAA Serverless created this log group for the throwaway workflow and it outlives "
            "the workflow. It is empty and costs nothing to store; delete it if you do not want "
            "one log group per preflight call."
        )
    return out


def poll_workflow_run(workflow_name: str, run_id: str = "", max_seconds: int = 0) -> dict:
    """Poll a workflow run until terminal state or timeout. Returns final status with error details."""
    client = _get_client()

    # Leave headroom under the Lambda timeout so the function returns the current
    # status rather than being killed mid-poll. The Function URL transport allows a
    # far longer request than API Gateway's old 29s cap, so a single call can now
    # wait out most task transitions instead of forcing the client to re-poll.
    #
    # The ceiling comes from the RUNTIME, not from a constant: max_poll_seconds was
    # chosen to sit under a 120s Timeout, which is a guess that ignores time already
    # spent in this invocation and goes stale if Timeout changes. Asking the context
    # for the time actually remaining is both correct and self-maintaining. Locally
    # there is no invocation deadline, so the configured value stands.
    _default_poll, _max_poll = _poll_limits()
    budget = max(5, min(int(max_seconds or _default_poll), _max_poll))
    deadline_budget = _remaining_seconds(budget)
    if deadline_budget < budget:
        log.info("Poll budget trimmed from %ss to %.0fs by the invocation deadline",
                 budget, deadline_budget)
        budget = int(deadline_budget)

    arn, name, _ = _resolve_workflow_arn(workflow_name)
    if arn is None:
        return {"error": name}

    # Resolve run_id if not provided
    if not run_id:
        try:
            runs, _ = _list_runs(client, arn)
            if not runs:
                return {"workflow_name": name, "message": "No runs found"}
            run_id = runs[0].get("RunId", "")
        except Exception as e:
            return {"error": f"Failed to list runs: {str(e)}"}

    start_time = time.time()
    poll_interval = 3
    last_status = {}

    while time.time() - start_time < budget:
        try:
            run = client.get_workflow_run(WorkflowArn=arn, RunId=run_id)
            detail = run.get("RunDetail", {})
            state = detail.get("RunState", "")

            last_status = {
                "workflow_name": name,
                "run_id": run_id,
                "status": state,
                "started_at": str(detail.get("CreatedAt", "")),
                "duration": detail.get("Duration"),
            }

            if detail.get("ErrorMessage"):
                last_status["error_message"] = detail["ErrorMessage"]

            tasks = detail.get("TaskInstances", [])
            if tasks:
                parsed = [_parse_task_instance(t) for t in tasks]
                last_status["task_instances"] = [p["task_id"] for p in parsed]

            if state in TERMINAL_RUN_STATES:
                last_status["completed"] = True
                if detail.get("CompletedOn"):
                    last_status["ended_at"] = str(detail["CompletedOn"])
                if state == "SUCCESS":
                    last_status["important"] = (
                        "RunState=SUCCESS means the run finished, NOT that every task succeeded. "
                        "Call mwaa_verify_run_tasks to confirm each task's outcome."
                    )
                return last_status

        except Exception as e:
            last_status = {"workflow_name": name, "run_id": run_id, "error": str(e)}

        time.sleep(poll_interval)

    last_status["completed"] = False
    last_status["message"] = f"Still running after {budget}s of polling. Call again to continue."
    return last_status


# Operators that run no code on a worker, so MWAA Serverless creates NO CloudWatch log
# stream for them. Verified end to end: a DAG of EmptyOperator -> BashOperator ->
# EmptyOperator produced exactly one stream (the Bash task). Treating a missing stream
# as "not flushed yet" therefore reported a perfectly healthy run as incomplete forever.
_NO_LOG_STREAM_OPERATORS = {"EmptyOperator"}


def _tasks_without_log_streams(client, arn) -> set:
    """Declared tasks that legitimately never write a log stream.

    Read from the workflow's immutable definition snapshot. Returns an empty set if the
    definition cannot be read — better to report a task as indeterminate than to claim
    it succeeded on the strength of a failed lookup.
    """
    try:
        detail = client.get_workflow(WorkflowArn=arn)
        text = detail.get("WorkflowDefinition")
        if not text:
            return set()
        data = yaml.safe_load(text)
    except Exception as e:  # noqa: BLE001 - logged; falls back to "assume it logs"
        log.warning("Could not read the definition for %s to identify no-log tasks: %s", arn, e)
        return set()

    from schema import resolve_operator_fqn
    silent = set()
    for dag_cfg in (data or {}).values():
        tasks = (dag_cfg or {}).get("tasks")
        if not isinstance(tasks, dict):
            continue
        for tid, tcfg in tasks.items():
            if not isinstance(tcfg, dict):
                continue
            _, short, _ = resolve_operator_fqn(tcfg.get("operator", "") or "")
            if short in _NO_LOG_STREAM_OPERATORS:
                silent.add(tid)
    return silent


def verify_run_tasks(workflow_name: str, run_id: str = "", include_logs: bool = True,
                     max_error_lines: int = 8, wait_for_logs_seconds: int = 45) -> dict:
    """Determine each task's real outcome by reading its CloudWatch log stream.

    This exists because RunState is not a reliable indicator of task success:
    a run whose final task succeeded reports SUCCESS even when an earlier task
    raised (verified against the live service). Every task stream ends with a
    'Task finished' event carrying final_state, which is authoritative.

    CloudWatch lags the run state by a few seconds, so a task polled the instant a
    run turns SUCCESS may not have flushed its final marker yet. When that happens
    the check is retried for up to wait_for_logs_seconds rather than reporting a
    task as indeterminate when it simply has not been written yet. Set
    wait_for_logs_seconds=0 for a single non-blocking read.
    """
    client = _get_client()
    arn, name, _ = _resolve_workflow_arn(workflow_name)
    if arn is None:
        return {"error": name}

    if not run_id:
        try:
            runs, _ = _list_runs(client, arn)
            if not runs:
                return {"error": f"No runs found for '{name}'"}
            run_id = runs[0].get("RunId", "")
        except Exception as e:
            return {"error": f"Failed to list runs: {str(e)}"}

    reported_state = ""
    declared_tasks = []
    try:
        detail = client.get_workflow_run(WorkflowArn=arn, RunId=run_id).get("RunDetail", {})
        reported_state = detail.get("RunState", "")
        declared_tasks = [_parse_task_instance(t)["task_id"] for t in detail.get("TaskInstances", [])]
    except Exception as e:
        return {"error": f"get_workflow_run failed: {str(e)}"}

    run_is_terminal = reported_state in TERMINAL_RUN_STATES
    silent_tasks = _tasks_without_log_streams(client, arn)
    deadline = time.time() + max(0, int(wait_for_logs_seconds))
    attempts = 0

    while True:
        attempts += 1
        outcome = _read_task_outcomes(arn, run_id, declared_tasks, include_logs,
                                      max_error_lines, silent_tasks)
        if "reason" in outcome:
            # Log group absent. Only worth waiting if the run has finished.
            if run_is_terminal and time.time() < deadline:
                time.sleep(5)
                continue
            outcome.update({"workflow_name": name, "run_id": run_id,
                            "reported_run_state": reported_state, "verified": False,
                            "declared_tasks": declared_tasks, "log_read_attempts": attempts})
            return outcome

        incomplete = outcome["unknown"] or outcome["no_logs_yet"]
        # Only a finished run is expected to have every marker written; a RUNNING run
        # legitimately has tasks in flight.
        if not incomplete or not run_is_terminal or time.time() >= deadline:
            break
        time.sleep(5)

    result = {
        "workflow_name": name,
        "run_id": run_id,
        "reported_run_state": reported_state,
        "verified": True,
        "log_read_attempts": attempts,
        **outcome,
    }

    failed = result["failed"]
    if failed and reported_state == "SUCCESS":
        result["discrepancy"] = (
            f"The run reports SUCCESS but {len(failed)} task(s) FAILED: {', '.join(failed)}. "
            f"This happens when a downstream task with trigger_rule: all_done succeeds after "
            f"an upstream failure. Treat this run as a failure and fix the failing task(s)."
        )
    elif result["all_tasks_succeeded"]:
        note = "Every task completed successfully."
        if result.get("no_log_stream_expected"):
            note += (
                f" {len(result['no_log_stream_expected'])} task(s) "
                f"({', '.join(result['no_log_stream_expected'])}) write no log stream at all "
                f"because their operator runs no code on a worker — that is expected, not a "
                f"missing result."
            )
        result["conclusion"] = note
    elif result["unknown"] or result["no_logs_yet"]:
        result["conclusion"] = (
            f"No task failed, but {len(result['unknown']) + len(result['no_logs_yet'])} task(s) "
            f"have not written a final marker yet. CloudWatch can lag a completed run by a few "
            f"seconds — call again to confirm."
            if run_is_terminal else
            "The run is still in progress; tasks without a final marker are still executing."
        )
    return result


def _read_task_outcomes(arn, run_id, declared_tasks, include_logs, max_error_lines,
                        silent_tasks=frozenset()):
    """One pass over a run's task log streams. Returns the outcome dict, or
    {"reason": ...} when the log group does not exist yet."""
    logs_client = boto3.client("logs", config=_boto_config())
    log_group = _log_group_for(arn)
    prefix = f"workflow_id={arn.rsplit('/', 1)[-1]}/run_id={run_id}/"

    streams = []
    try:
        token = None
        while True:
            kwargs = {"logGroupName": log_group, "logStreamNamePrefix": prefix, "limit": 50}
            if token:
                kwargs["nextToken"] = token
            resp = logs_client.describe_log_streams(**kwargs)
            streams.extend(resp.get("logStreams", []))
            token = resp.get("nextToken")
            if not token or len(streams) >= 200:
                break
    except logs_client.exceptions.ResourceNotFoundException:
        return {"reason": f"Log group {log_group} does not exist yet. Logs can take a few minutes "
                          f"to appear after a run starts, and a run that never reached a worker "
                          f"produces none.",
                "log_group": log_group}
    except Exception as e:
        return {"reason": f"Could not list log streams: {e}", "log_group": log_group}

    task_results = {}
    for stream in streams:
        sname = stream.get("logStreamName", "")
        task_id, attempt = "", None
        for part in sname.split("/"):
            if part.startswith("task_id="):
                task_id = part.split("=", 1)[1]
            elif part.startswith("attempt="):
                attempt = part.split("=", 1)[1].replace(".log", "")
        if not task_id:
            continue

        outcome = {"task_id": task_id, "attempt": attempt, "final_state": "unknown",
                   "errors": [], "exception_type": None}
        try:
            events = logs_client.get_log_events(
                logGroupName=log_group, logStreamName=sname, limit=300, startFromHead=False
            ).get("events", [])
        except Exception as e:
            outcome["final_state"] = f"log read failed: {e}"
            task_results.setdefault(task_id, outcome)
            continue

        for ev in events:
            msg = ev.get("message", "")
            try:
                p = json.loads(msg)
            except Exception:
                low = msg.lower()
                if include_logs and any(k in low for k in ("error", "exception", "traceback", "failed")):
                    outcome["errors"].append(msg.strip()[:300])
                continue

            if "final_state" in p:
                outcome["final_state"] = p.get("final_state", "unknown")
                if p.get("duration") is not None:
                    outcome["duration_seconds"] = round(p["duration"], 2)
                if p.get("exit_code") is not None:
                    outcome["exit_code"] = p["exit_code"]

            if p.get("level") == "error":
                line = str(p.get("event", ""))[:300]
                for d in (p.get("error_detail") or []):
                    if isinstance(d, dict):
                        outcome["exception_type"] = outcome["exception_type"] or d.get("exc_type")
                        val = str(d.get("exc_value", ""))[:400]
                        if val:
                            line = f"{line} | {d.get('exc_type')}: {val}"
                if include_logs and line:
                    outcome["errors"].append(line)
            elif p.get("level") == "warning" and "DagBag" in str(p.get("logger", "")):
                if include_logs:
                    outcome["errors"].append(f"DAG import warning: {str(p.get('event'))[:200]}")

        outcome["errors"] = outcome["errors"][:max_error_lines]
        prev = task_results.get(task_id)
        # Keep the highest attempt, which is the final outcome after retries. Compared
        # as INTEGERS: a string compare made attempt "10" lose to "9", so past ten
        # attempts the reported outcome was the wrong one.
        if prev is None or _attempt_number(attempt) >= _attempt_number(prev.get("attempt")):
            task_results[task_id] = outcome

    failed = sorted(t["task_id"] for t in task_results.values() if t["final_state"] == "failed")
    succeeded = sorted(t["task_id"] for t in task_results.values() if t["final_state"] == "success")
    unknown = sorted(t["task_id"] for t in task_results.values()
                     if t["final_state"] not in ("failed", "success"))
    # Split the streamless tasks into "expected to have none" and "genuinely missing".
    absent = [t for t in declared_tasks if t not in task_results]
    no_stream_expected = sorted(t for t in absent if t in silent_tasks)
    missing = sorted(t for t in absent if t not in silent_tasks)

    return {
        "all_tasks_succeeded": bool(succeeded or no_stream_expected) and not failed
                               and not unknown and not missing,
        "no_log_stream_expected": no_stream_expected,
        "task_count": len(task_results),
        "succeeded": succeeded,
        "failed": failed,
        "unknown": unknown,
        "no_logs_yet": missing,
        "task_details": sorted(task_results.values(), key=lambda t: t["task_id"]),
        "log_group": log_group,
    }


def _get_task_error_logs(workflow_arn: str, run_id: str, max_lines: int = 5) -> list:
    """Pull error/warning log lines from CloudWatch for a failed run's tasks."""
    logs_client = boto3.client("logs", config=_boto_config())
    wf_id = workflow_arn.rsplit("/", 1)[-1] if "/" in workflow_arn else workflow_arn
    log_group = _log_group_for(workflow_arn)

    task_logs = []
    try:
        # Find log streams for this run
        streams = logs_client.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=f"workflow_id={wf_id}/run_id={run_id}/",
            limit=20,
        ).get("logStreams", [])

        for stream in streams:
            stream_name = stream.get("logStreamName", "")
            # Extract task_id from stream name
            task_id = ""
            for part in stream_name.split("/"):
                if part.startswith("task_id="):
                    task_id = part.split("=", 1)[1]

            try:
                events = logs_client.get_log_events(
                    logGroupName=log_group,
                    logStreamName=stream_name,
                    limit=50,
                ).get("events", [])

                # Extract error and warning lines
                error_lines = []
                for evt in events:
                    msg = evt.get("message", "")
                    try:
                        parsed = json.loads(msg)
                        level = parsed.get("level", "")
                        if level == "error":
                            entry = parsed.get("event", "")
                            # Include error_detail if present (has stack trace info)
                            detail = parsed.get("error_detail", "")
                            if detail and isinstance(detail, list):
                                exc_parts = []
                                for d in detail:
                                    if d.get("exc_value"):
                                        exc_parts.append(d["exc_value"])
                                if exc_parts:
                                    entry += " | " + "; ".join(exc_parts)
                            if entry:
                                error_lines.append(entry)
                    except (json.JSONDecodeError, TypeError):
                        lower = msg.lower()
                        if "error" in lower or "exception" in lower or "failed" in lower:
                            error_lines.append(msg.strip()[:200])

                if error_lines:
                    task_logs.append({
                        "task_id": task_id,
                        "errors": error_lines[:max_lines],
                    })
            except Exception as e:  # noqa: BLE001 - logged; this stream is skipped
                log.warning("Could not read log stream %s: %s", stream_name, e)
                continue
    except Exception as e:  # noqa: BLE001 - logged; caller sees no task logs
        log.warning("Could not collect task error logs for run %s: %s", run_id, e)

    return task_logs


def get_failed_runs_summary(name_contains: str = "", hours_back: int = 24, analyze: bool = True,
                            include_hidden_failures: bool = True) -> dict:
    """Scan workflows for recent failures, collect errors + CloudWatch logs, and optionally analyze with Bedrock.

    With include_hidden_failures (default), runs that report SUCCESS are also
    inspected for tasks that actually failed. A run whose last task succeeded
    reports SUCCESS even when an earlier task raised, so filtering on
    RunState == FAILED alone silently misses real breakage.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    # Get workflows
    all_wf = _list_all_workflows()
    if name_contains:
        name_lower = name_contains.lower()
        all_wf = [w for w in all_wf if name_lower in w.get("Name", "").lower()]

    def _check_workflow(wf):
        """Check a single workflow for failed runs and pull logs."""
        try:
            wf_client = _new_client()
            arn = wf["WorkflowArn"]
            # Bounded: each FAILED run here costs a get_workflow_run plus log reads, so
            # an unbounded walk of a busy workflow would not finish. The bound is
            # reported (see runs_truncated below) rather than left invisible — a silent
            # cap turns "no failures found" into a false negative.
            runs, runs_truncated = _list_runs(wf_client, arn, cap=_MAX_RUNS_SCANNED)
            failures = []
            hidden = []
            for r in runs:
                summary = r.get("RunDetailSummary", {})
                status = summary.get("Status")
                created = summary.get("CreatedOn", "")
                if created:
                    parsed = _parse_timestamp(created)
                    # A run whose timestamp cannot be parsed is INSPECTED rather than
                    # skipped: skipping it would hide a failure on a formatting quirk.
                    if parsed is not None and parsed < cutoff:
                        continue
                run_id = r.get("RunId", "")

                if status == "FAILED":
                    try:
                        detail_resp = wf_client.get_workflow_run(WorkflowArn=arn, RunId=run_id)
                        detail = detail_resp.get("RunDetail", {})
                        failure = {
                            "run_id": run_id,
                            "created": str(created),
                            "reported_state": status,
                            "error_message": detail.get("ErrorMessage", "No error message"),
                        }
                        task_logs = _get_task_error_logs(arn, run_id)
                        if task_logs:
                            failure["task_logs"] = task_logs
                        failures.append(failure)
                    except Exception:
                        failures.append({
                            "run_id": run_id,
                            "created": str(created),
                            "reported_state": status,
                            "error_message": "Could not retrieve run details",
                        })
                elif status == "SUCCESS" and include_hidden_failures and len(hidden) < 3:
                    # A green run can still contain failed tasks.
                    bad = _failed_tasks_in_run(arn, run_id)
                    if bad:
                        hidden.append({
                            "run_id": run_id,
                            "created": str(created),
                            "reported_state": "SUCCESS",
                            "failed_tasks": bad,
                            "why_it_looked_green": (
                                "A downstream task (typically trigger_rule: all_done) succeeded "
                                "after these failed, so the run reported SUCCESS."
                            ),
                        })

            out = {}
            if failures:
                out["failed_runs"] = failures
            if hidden:
                out["runs_reporting_success_with_failed_tasks"] = hidden
            if runs_truncated:
                out["history_truncated"] = (
                    f"Only the most recent {_MAX_RUNS_SCANNED} runs were scanned, so older "
                    f"failures may exist. This is not a clean bill of health for the whole "
                    f"history."
                )
            if out:
                return {"name": wf.get("Name", ""), "arn": arn, **out}, None
        except Exception as e:  # noqa: BLE001 - reported, never swallowed
            log.warning("Failure scan failed for %s: %s", wf.get("Name", ""), e)
            return None, f"{wf.get('Name', '')}: {e}"
        return None, None

    # Parallel scan
    failed_workflows = []
    scan_errors = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_check_workflow, wf): wf for wf in all_wf}
        for f in as_completed(futures):
            result, err = f.result()
            if result:
                failed_workflows.append(result)
            if err:
                scan_errors.append(err)

    # Bedrock analysis
    ai_analysis, ai_model, ai_error = None, None, None
    if analyze and failed_workflows:
        failure_text = json.dumps(failed_workflows, indent=2, default=str)
        if len(failure_text) > 12000:
            failure_text = failure_text[:12000] + "\n... (truncated)"

        prompt = (
            "You are an MWAA Serverless workflow debugging expert. Analyze these failed workflow runs. "
            "Each failure includes the error message AND CloudWatch task logs showing the actual errors "
            "from task execution. Use the task_logs to identify the real root cause — not just the "
            "generic error message. For each failure provide: (1) root cause from the logs, "
            "(2) specific suggested fix. Be concise — 2-3 sentences per workflow.\n\n"
            f"Failed runs with logs:\n{failure_text}"
        )
        ai_analysis, ai_model, ai_error = _analyse_with_bedrock(prompt)

    result = {
        "hours_back": hours_back,
        "workflows_scanned": len(all_wf),
        "workflows_with_failures": len(failed_workflows),
        "failures": failed_workflows,
    }
    if scan_errors:
        # A scan that could not read some workflows must not report "no failures".
        result["not_scanned"] = scan_errors
        result["incomplete_scan_warning"] = (
            f"{len(scan_errors)} of {len(all_wf)} workflow(s) could not be scanned, so a real "
            f"failure may be missing from this result. Fix the errors in not_scanned and re-run."
        )
    if ai_analysis:
        result["analysis"] = ai_analysis
        result["analysis_model"] = ai_model
        result["analysis_data_sent_to_bedrock"] = (
            "Failure details INCLUDING CloudWatch task log excerpts were sent to Amazon Bedrock "
            "for this analysis. Pass analyze=false to keep log content local."
        )
    elif ai_error:
        # Do not bury this in an "analysis" string that reads like a finding — the
        # caller needs to know the AI step did not run, and why.
        result["analysis"] = None
        result["analysis_unavailable"] = ai_error
        result["analysis_hint"] = (
            "The log-based failure details above are still complete and are the "
            "authoritative source. Set BEDROCK_MODEL_ID to a model this account can "
            "invoke, or call with analyze=false to skip this step."
        )

    return result


# Models tried in order for failure analysis. The first one that responds wins.
#
# Resolved from config.py so it can be changed without editing code: an environment
# variable (BEDROCK_MODEL_ID / BEDROCK_MODEL_CANDIDATES) or a mcp_config.json file.
#
# A chain rather than a single hardcoded id because Bedrock retires models: the id this
# server originally hardcoded (anthropic.claude-3-haiku-20240307-v1:0) has since reached
# end of life and returns ResourceNotFoundException, which silently disabled this feature.


def _analyse_with_bedrock(prompt: str, max_tokens: int = None):
    """Summarise failures with Bedrock. Returns (text, model_id, error).

    Uses the Converse API, which takes the same request shape for every provider.
    The previous implementation posted an Anthropic-specific body
    ({"anthropic_version": ..., "messages": [...]}) to invoke_model, so a
    non-Anthropic model needed a different payload and no fallback was possible.
    """
    import config

    cfg = config.load()
    candidates = cfg["_effective_model_candidates"]
    pinned = cfg["_model_selection"] == "pinned"
    tokens = max_tokens or cfg["bedrock_max_tokens"]
    region = cfg["bedrock_region"]

    if not candidates:
        return None, None, ("No Bedrock model configured. Set bedrock_model_id or "
                            "bedrock_model_candidates in mcp_config.json, or BEDROCK_MODEL_ID.")

    try:
        kwargs = {"region_name": region} if region else {}
        # Model inference is slow by nature, so this client gets a longer read timeout
        # than the 20s the AWS control-plane calls use. Retries stay bounded: a retry
        # storm against Bedrock costs money as well as time.
        from botocore.config import Config

        kwargs["config"] = Config(
            connect_timeout=5,
            read_timeout=60,
            retries={"max_attempts": 2, "mode": "standard"},
        )
        bedrock = boto3.client("bedrock-runtime", **kwargs)
    except Exception as e:
        return None, None, f"Could not create a bedrock-runtime client: {e}"

    errors = []
    for model_id in candidates:
        try:
            resp = bedrock.converse(
                modelId=model_id,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": tokens, "temperature": 0},
            )
            blocks = resp.get("output", {}).get("message", {}).get("content", [])
            text = "".join(b.get("text", "") for b in blocks).strip()
            if text:
                return text, model_id, None
            errors.append(f"{model_id}: empty response")
        except Exception as e:
            errors.append(f"{model_id}: {type(e).__name__}: {str(e)[:160]}")

    detail = " | ".join(errors)
    if pinned:
        return None, None, (
            f"The pinned model '{candidates[0]}' could not be invoked. Change "
            f"bedrock_model_id in mcp_config.json or BEDROCK_MODEL_ID. {detail}"
        )
    return None, None, (
        f"No Bedrock model could be invoked. Enable model access in this Region, or set "
        f"bedrock_model_id / BEDROCK_MODEL_ID to one you can call. Tried: {detail}"
    )


def delete_workflows(name_contains: str = "", not_run_in_days: int = 0, dry_run: bool = True,
                     confirm_delete_all: bool = False) -> dict:
    """Delete workflows matching criteria. Previews by default.

    REFUSES an unfiltered non-dry-run call. With no name_contains and no
    not_run_in_days, the target set is EVERY workflow in the account — including ones
    this server never created — and dry_run=True was the only thing standing between a
    mis-parameterised call and account-wide deletion. Deleting everything is still
    possible, but it now has to be asked for explicitly via confirm_delete_all.
    """
    client = _get_client()

    all_wf = _list_all_workflows(client)
    total_in_account = len(all_wf)
    unfiltered = not name_contains and not_run_in_days <= 0

    if unfiltered and not dry_run and not confirm_delete_all:
        return {
            "error": "Refusing to delete every workflow in the account.",
            "reason": (
                f"No filter was given, so this call targets all {total_in_account} workflow(s) "
                f"in this account and Region, including workflows this server did not create. "
                f"Deletion cannot be undone."
            ),
            "workflows_that_would_be_deleted": sorted(w.get("Name", "") for w in all_wf),
            "how_to_proceed": [
                "Pass name_contains='<substring>' to target a specific set (recommended), or",
                "pass not_run_in_days=N to target only stale workflows, or",
                "if you really do mean all of them, pass confirm_delete_all=true.",
            ],
        }

    # Filter by name
    if name_contains:
        name_lower = name_contains.lower()
        all_wf = [w for w in all_wf if name_lower in w.get("Name", "").lower()]

    # Filter by inactivity — check last run date
    if not_run_in_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=not_run_in_days)
        inactive = []
        check_errors = []

        def _check_inactive(wf):
            arn = wf["WorkflowArn"]
            try:
                wf_client = _new_client()
                # EVERY page. This decides whether a workflow gets deleted, and a single
                # page of history could easily omit the newest run — which would make a
                # workflow that ran yesterday look untouched for months.
                runs, truncated = _list_runs(wf_client, arn)
                if truncated:
                    return None, (
                        f"{wf.get('Name', arn)}: run history was truncated, so the last-run "
                        f"time cannot be established"
                    )
                if not runs:
                    # Never run — include it
                    return wf, None
                latest = max(
                    str(r.get("RunDetailSummary", {}).get("CreatedOn", ""))
                    for r in runs
                )
                if latest and _parse_timestamp(latest) and _parse_timestamp(latest) < cutoff:
                    return wf, None
            except Exception as e:  # noqa: BLE001 - reported, never swallowed
                log.warning("Inactivity check failed for %s: %s", wf.get("Name", arn), e)
                # A workflow whose run history could not be read must NOT be treated as
                # inactive: that would delete it on the strength of a failed API call.
                return None, f"{wf.get('Name', arn)}: {e}"
            return None, None

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_check_inactive, wf): wf for wf in all_wf}
            for f in as_completed(futures):
                wf, err = f.result()
                if wf:
                    inactive.append(wf)
                if err:
                    check_errors.append(err)
        all_wf = inactive
        if check_errors:
            return {
                "error": "Could not determine last-run time for every candidate, so nothing "
                         "was selected for deletion.",
                "failed_checks": check_errors,
                "hint": "Fix the permissions/errors above and retry, or delete by name instead.",
            }

    targets = [{"name": w.get("Name", ""), "arn": w["WorkflowArn"]} for w in all_wf]

    if dry_run:
        return {
            "dry_run": True,
            "count": len(targets),
            "total_workflows_in_account": total_in_account,
            "filter_applied": None if unfiltered else {
                "name_contains": name_contains or None,
                "not_run_in_days": not_run_in_days or None,
            },
            "unfiltered_warning": (
                f"NO FILTER was applied, so this targets all {total_in_account} workflow(s) in "
                f"the account. A non-dry-run call will be refused unless you pass "
                f"confirm_delete_all=true."
            ) if unfiltered else None,
            "workflows_to_delete": targets,
            "message": "Set dry_run=false to actually delete these workflows.",
        }

    # Actually delete
    results = []
    for t in targets:
        try:
            client.delete_workflow(WorkflowArn=t["arn"])
            results.append({"name": t["name"], "status": "deleted"})
        except Exception as e:  # noqa: BLE001 - per-workflow outcome is reported
            log.warning("delete_workflow failed for %s: %s", t["name"], e)
            results.append({"name": t["name"], "status": "error", "error": str(e)})

    return {
        "dry_run": False,
        "count": len(results),
        "deleted": sum(1 for r in results if r["status"] == "deleted"),
        "failed": sum(1 for r in results if r["status"] == "error"),
        "results": results,
    }


def list_runs(name_contains: str = "", status: str = "", hours_back: int = 0) -> dict:
    """List workflow runs with optional filters. Can filter by workflow name, run status, and time window."""
    cutoff = None
    if hours_back > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    all_wf = _list_all_workflows()
    if name_contains:
        name_lower = name_contains.lower()
        all_wf = [w for w in all_wf if name_lower in w.get("Name", "").lower()]

    status_upper = status.upper() if status else ""

    def _get_runs(wf):
        arn = wf.get("WorkflowArn", "")
        try:
            wf_client = _new_client()
            runs, runs_truncated = _list_runs(wf_client, arn, cap=_MAX_RUNS_SCANNED)
            matched = []
            for r in runs:
                summary = r.get("RunDetailSummary", {})
                run_status = summary.get("Status", "")
                created = summary.get("CreatedOn", "")

                if status_upper and run_status.upper() != status_upper:
                    continue
                if cutoff and created:
                    parsed = _parse_timestamp(created)
                    # An unparseable timestamp is KEPT rather than dropped: excluding it
                    # would hide a run because its date could not be read.
                    if parsed is not None and parsed < cutoff:
                        continue

                matched.append({
                    "run_id": r.get("RunId", ""),
                    "status": run_status,
                    "created": str(created),
                    "started": str(summary.get("StartedAt", "")),
                    "ended": str(summary.get("EndedAt", "")),
                })
            if matched:
                entry = {"name": wf.get("Name", ""), "runs": matched}
                if runs_truncated:
                    entry["history_truncated"] = (
                        f"Only the most recent {_MAX_RUNS_SCANNED} runs were examined."
                    )
                return entry, None
        except Exception as e:  # noqa: BLE001 - reported, never swallowed
            log.warning("Could not list runs for %s: %s", wf.get("Name", arn), e)
            return None, f"{wf.get('Name', arn)}: {e}"
        return None, None

    all_runs, errors = [], []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_get_runs, wf): wf for wf in all_wf}
        for f in as_completed(futures):
            result, err = f.result()
            if result:
                all_runs.append(result)
            if err:
                errors.append(err)

    # Summary counts
    total_runs = sum(len(w["runs"]) for w in all_runs)
    status_counts = {}
    for w in all_runs:
        for r in w["runs"]:
            s = r["status"]
            status_counts[s] = status_counts.get(s, 0) + 1

    out = {
        "workflows_scanned": len(all_wf),
        "workflows_with_runs": len(all_runs),
        "total_runs": total_runs,
        "status_summary": status_counts,
        "workflows": all_runs,
    }
    if errors:
        out["not_scanned"] = errors
        out["incomplete_scan_warning"] = (
            f"{len(errors)} of {len(all_wf)} workflow(s) could not be read; runs are missing "
            f"from this result."
        )
    return out


def get_workflow_summary(workflow_name: str) -> dict:
    """Compact workflow overview without the full YAML — name, status, last run, operator count."""
    client = _get_client()

    arn, name, _ = _resolve_workflow_arn(workflow_name)
    if arn is None:
        return {"error": name}

    detail = client.get_workflow(WorkflowArn=arn)

    info = {
        "name": detail.get("Name", ""),
        "status": detail.get("WorkflowStatus", ""),
        "trigger_mode": detail.get("TriggerMode", ""),
        "created": str(detail.get("CreatedAt", "")),
        "modified": str(detail.get("ModifiedAt", "")),
        "execution_role_arn": detail.get("RoleArn", ""),
    }

    # Operator counts come from the immutable snapshot when available; the S3 object
    # may have been overwritten since deployment.
    text = detail.get("WorkflowDefinition")
    s3_loc = detail.get("DefinitionS3Location", {})
    if s3_loc:
        info["definition_s3"] = f"s3://{s3_loc.get('Bucket','')}/{s3_loc.get('ObjectKey','')}"
    if not text and s3_loc:
        try:
            obj = boto3.client("s3", config=_boto_config()).get_object(
                Bucket=s3_loc["Bucket"], Key=s3_loc["ObjectKey"])
            text = obj["Body"].read().decode("utf-8")
        except Exception as e:  # noqa: BLE001 - reported, never swallowed
            log.warning("Could not read definition for %s: %s", name, e)
            info["definition_read_error"] = str(e)
    if text:
        try:
            dag = yaml.safe_load(text)
            operators = {}
            for t in _iter_tasks(dag):
                op = t.get("operator", "")
                if op:
                    short = op.rsplit(".", 1)[-1] if "." in op else op
                    operators[short] = operators.get(short, 0) + 1
            info["task_count"] = sum(operators.values())
            info["operators"] = operators
        except yaml.YAMLError as e:
            info["definition_parse_error"] = str(e)

    # Get latest run status
    try:
        runs, _ = _list_runs(client, arn)
        if runs:
            latest = runs[0]
            summary = latest.get("RunDetailSummary", {})
            info["latest_run"] = {
                "run_id": latest.get("RunId", ""),
                "status": summary.get("Status", ""),
                "created": str(summary.get("CreatedOn", "")),
            }
            # Count runs by status
            status_counts = {}
            for r in runs:
                s = r.get("RunDetailSummary", {}).get("Status", "")
                status_counts[s] = status_counts.get(s, 0) + 1
            info["run_history"] = status_counts
            info["total_runs"] = len(runs)
    except Exception as e:  # noqa: BLE001 - reported, never swallowed
        log.warning("Could not read run history for %s: %s", name, e)
        info["run_history_error"] = str(e)

    return info


def bulk_status(name_contains: str = "", names: list = None) -> dict:
    """Get status of multiple workflows with their latest run in one call."""
    all_wf = _list_all_workflows()

    if names:
        names_lower = [n.lower() for n in names]
        all_wf = [w for w in all_wf if any(n in w.get("Name", "").lower() for n in names_lower)]
    elif name_contains:
        name_lower = name_contains.lower()
        all_wf = [w for w in all_wf if name_lower in w.get("Name", "").lower()]

    def _get_status(wf):
        arn = wf.get("WorkflowArn", "")
        try:
            wf_client = _new_client()
            info = {"name": wf.get("Name", ""), "workflow_status": wf.get("WorkflowStatus", "")}
            runs, _ = _list_runs(wf_client, arn)
            if runs:
                s = runs[0].get("RunDetailSummary", {})
                info["latest_run"] = {
                    "status": s.get("Status", ""),
                    "created": str(s.get("CreatedOn", "")),
                }
            else:
                info["latest_run"] = None
            return info
        except Exception as e:  # noqa: BLE001 - the real reason is reported, not hidden
            log.warning("bulk_status failed for %s: %s", wf.get("Name", arn), e)
            return {"name": wf.get("Name", ""), "error": f"Failed to get status: {e}"}

    results = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_get_status, wf): wf for wf in all_wf}
        for f in as_completed(futures):
            results.append(f.result())

    # Sort by name
    results.sort(key=lambda r: r.get("name", ""))

    failed = [r["name"] for r in results if r.get("error")]
    out = {"count": len(results), "workflows": results}
    if failed:
        out["workflows_with_errors"] = failed
    return out


def redeploy_workflow(workflow_name: str, yaml_content: str, s3_bucket: str = "", s3_key: str = "",
                      expected_bucket_owner: str = "") -> dict:
    """Update an existing workflow's YAML and start a new run. Reuses existing S3 location and role if not specified."""
    client = _get_client()

    # Validate FIRST, before any AWS call: a broken definition is refused anyway, and
    # the local message is more actionable than the service's.
    try:
        import validator
        pre = validator.validate(yaml_content)
        if not pre["valid"]:
            return {
                "error": "Definition failed local validation; the workflow was NOT changed.",
                "validation_errors": pre["errors"],
                "hint": "Call repair_dag_yaml to fix the mechanical problems, then retry.",
            }
    except Exception as e:  # noqa: BLE001 - a validator failure must not block redeploy
        log.warning("Local validation could not run: %s", e)

    # Mutating: this OVERWRITES a deployed definition, so a substring must not be
    # allowed to pick the wrong workflow.
    arn, name, _ = _resolve_workflow_arn(workflow_name, require_unambiguous=True)
    if arn is None:
        return {"error": name}

    # Get existing workflow details for defaults
    detail = client.get_workflow(WorkflowArn=arn)
    existing_s3 = detail.get("DefinitionS3Location", {})
    role_arn = detail.get("RoleArn", "")

    if not s3_bucket:
        s3_bucket = existing_s3.get("Bucket", "")
    if not s3_key:
        s3_key = existing_s3.get("ObjectKey", "")

    if not s3_bucket or not s3_key:
        return {"error": "Could not determine S3 location. Provide s3_bucket and s3_key."}

    # Upload new YAML
    s3 = boto3.client("s3", config=_boto_config())
    try:
        _put_object(s3, s3_bucket, s3_key, yaml_content.encode("utf-8"), expected_bucket_owner)
    except Exception as e:  # noqa: BLE001 - surfaced to the caller
        return {"error": f"S3 upload failed: {e}"}

    # Update workflow
    try:
        client.update_workflow(
            WorkflowArn=arn,
            DefinitionS3Location={"Bucket": s3_bucket, "ObjectKey": s3_key},
            RoleArn=role_arn,
        )
    except Exception as e:  # noqa: BLE001 - surfaced to the caller
        return {"error": f"Update failed: {e}"}

    # Start run
    try:
        run_resp = client.start_workflow_run(WorkflowArn=arn)
        run_id = run_resp.get("RunId", "")
    except Exception as e:
        return {"workflow_name": name, "action": "updated", "error": f"Update succeeded but start failed: {str(e)}"}

    return {
        "workflow_name": name,
        "workflow_arn": arn,
        "action": "redeployed",
        "s3_location": {"bucket": s3_bucket, "key": s3_key},
        "run_id": run_id,
        "status": "STARTED",
    }


def compare_versions(workflow_name: str) -> dict:
    """Compare the latest two versions of a workflow to show what changed."""
    client = _get_client()

    arn, name, _ = _resolve_workflow_arn(workflow_name)
    if arn is None:
        return {"error": name}

    versions, _ = _list_all_pages(
        client, "list_workflow_versions", "WorkflowVersions", WorkflowArn=arn
    )
    if len(versions) < 2:
        return {"workflow_name": name, "message": "Only one version exists, nothing to compare."}

    # Sort by created date descending
    versions.sort(key=lambda v: str(v.get("CreatedAt", "")), reverse=True)
    latest = versions[0]
    previous = versions[1]

    s3 = boto3.client("s3", config=_boto_config())
    yamls = {}
    for label, ver in [("latest", latest), ("previous", previous)]:
        s3_loc = ver.get("DefinitionS3Location", {})
        try:
            obj = s3.get_object(Bucket=s3_loc["Bucket"], Key=s3_loc["ObjectKey"])
            yamls[label] = obj["Body"].read().decode("utf-8")
        except Exception:
            yamls[label] = None

    result = {
        "workflow_name": name,
        "latest_version": {
            "version": latest.get("WorkflowVersion", ""),
            "created": str(latest.get("CreatedAt", "")),
            "s3": f"s3://{latest.get('DefinitionS3Location',{}).get('Bucket','')}/{latest.get('DefinitionS3Location',{}).get('ObjectKey','')}",
        },
        "previous_version": {
            "version": previous.get("WorkflowVersion", ""),
            "created": str(previous.get("CreatedAt", "")),
            "s3": f"s3://{previous.get('DefinitionS3Location',{}).get('Bucket','')}/{previous.get('DefinitionS3Location',{}).get('ObjectKey','')}",
        },
        "total_versions": len(versions),
    }

    if yamls.get("latest") and yamls.get("previous"):
        if yamls["latest"] == yamls["previous"]:
            result["diff"] = "No changes in YAML content (S3 location may have changed)."
        else:
            # Compute a simple line diff
            import difflib
            diff = list(difflib.unified_diff(
                yamls["previous"].splitlines(keepends=True),
                yamls["latest"].splitlines(keepends=True),
                fromfile=f"v:{previous.get('WorkflowVersion','')}",
                tofile=f"v:{latest.get('WorkflowVersion','')}",
                n=2,
            ))
            result["diff"] = "".join(diff[:100])  # Cap at 100 lines
            result["diff_lines"] = len(diff)
    else:
        result["diff"] = "Could not retrieve one or both YAML definitions."

    return result
