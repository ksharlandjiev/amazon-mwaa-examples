"""Operational functions for MWAA Serverless workflow management."""

import json
import re
import time
import boto3
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

_client = None

# Terminal run states. Note SUCCESS here means "the run finished", NOT "every task
# succeeded" — verify_run_tasks() exists because those are different things.
TERMINAL_RUN_STATES = {"SUCCESS", "FAILED", "STOPPED", "TIMEOUT"}

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


def _get_client():
    """Cached boto3 client for MWAA Serverless.

    Requires a boto3 that registers the 'mwaa-serverless' service (>= 1.40), which is
    also the version that added the `Code` parameter needed for Python/Bash tasks.
    """
    global _client
    if _client is None:
        try:
            _client = boto3.client("mwaa-serverless")
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
        return boto3.client("mwaa-serverless")
    except Exception as e:
        raise RuntimeError(_client_error_message(e)) from e


def _failed_tasks_in_run(workflow_arn: str, run_id: str, limit: int = 20) -> list:
    """Task ids whose log stream ends in final_state=failed.

    Cheap variant of verify_run_tasks used when scanning many runs: it only looks
    for the authoritative 'Task finished' marker and the first exception value.
    """
    logs_client = boto3.client("logs")
    wf_id = workflow_arn.rsplit("/", 1)[-1] if "/" in workflow_arn else workflow_arn
    log_group = _log_group_for(workflow_arn)
    out = []
    try:
        streams = logs_client.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=f"workflow_id={wf_id}/run_id={run_id}/",
            limit=limit,
        ).get("logStreams", [])
    except Exception:
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
        except Exception:
            continue
        state, err = None, None
        for ev in events:
            try:
                p = json.loads(ev.get("message", ""))
            except Exception:
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
    client = _get_client()
    workflows = []
    paginator = client.get_paginator("list_workflows") if hasattr(client, "get_paginator") else None

    if paginator:
        try:
            for page in paginator.paginate():
                workflows.extend(page.get("Workflows", []))
        except Exception:
            resp = client.list_workflows()
            workflows = resp.get("Workflows", [])
    else:
        resp = client.list_workflows()
        workflows = resp.get("Workflows", [])

    # Filter
    if name_contains:
        name_lower = name_contains.lower()
        workflows = [w for w in workflows if name_lower in w.get("Name", "").lower()]
    if status:
        status_upper = status.upper()
        workflows = [w for w in workflows if w.get("WorkflowStatus", "").upper() == status_upper]

    # Compact output
    result = []
    for w in workflows:
        result.append({
            "name": w.get("Name", ""),
            "status": w.get("WorkflowStatus", ""),
            "trigger_mode": w.get("TriggerMode", ""),
            "arn": w.get("WorkflowArn", ""),
            "modified": w.get("ModifiedAt", ""),
        })

    return {"count": len(result), "workflows": result}


def get_workflow(workflow_name: str) -> dict:
    """Get detailed info about a workflow including its DAG YAML definition."""
    client = _get_client()

    # Find the workflow ARN by name
    all_wf = client.list_workflows().get("Workflows", [])
    match = [w for w in all_wf if w.get("Name", "") == workflow_name]
    if not match:
        # Try partial match
        match = [w for w in all_wf if workflow_name.lower() in w.get("Name", "").lower()]
    if not match:
        return {"error": f"No workflow found matching '{workflow_name}'"}

    wf = match[0]
    arn = wf["WorkflowArn"]
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
            s3 = boto3.client("s3")
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
        except Exception:
            pass
        try:
            import validator
            v = validator.validate(yaml_content)
            if not v["valid"]:
                info["definition_validation_errors"] = v["errors"]
            if v["warnings"]:
                info["definition_validation_warnings"] = v["warnings"]
        except Exception:
            pass

    return info


def start_workflow_run(workflow_name: str) -> dict:
    """Start a workflow run by name."""
    client = _get_client()

    all_wf = client.list_workflows().get("Workflows", [])
    match = [w for w in all_wf if w.get("Name", "") == workflow_name]
    if not match:
        match = [w for w in all_wf if workflow_name.lower() in w.get("Name", "").lower()]
    if not match:
        return {"error": f"No workflow found matching '{workflow_name}'"}

    arn = match[0]["WorkflowArn"]
    resp = client.start_workflow_run(WorkflowArn=arn)

    return {
        "workflow_name": match[0].get("Name", ""),
        "workflow_arn": arn,
        "run_id": resp.get("RunId", resp.get("WorkflowRunId", "")),
        "status": "STARTED",
    }


def get_workflow_run_status(workflow_name: str, run_id: str = "") -> dict:
    """Get the status of a workflow run. If no run_id, gets the latest run."""
    client = _get_client()

    all_wf = client.list_workflows().get("Workflows", [])
    match = [w for w in all_wf if w.get("Name", "") == workflow_name]
    if not match:
        match = [w for w in all_wf if workflow_name.lower() in w.get("Name", "").lower()]
    if not match:
        return {"error": f"No workflow found matching '{workflow_name}'"}

    arn = match[0]["WorkflowArn"]

    if not run_id:
        # Get latest run
        try:
            runs = client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
            if not runs:
                return {"workflow_name": match[0].get("Name", ""), "message": "No runs found"}
            # Sort by created time descending
            runs.sort(
                key=lambda r: str(r.get("RunDetailSummary", {}).get("CreatedOn", r.get("StartedAt", ""))),
                reverse=True,
            )
            run_id = runs[0].get("RunId", "")
        except Exception as e:
            return {"error": f"Failed to list runs: {str(e)}"}

    try:
        run = client.get_workflow_run(WorkflowArn=arn, RunId=run_id)
    except Exception as e:
        return {"error": f"Failed to get run: {str(e)}"}

    detail = run.get("RunDetail", {})
    result = {
        "workflow_name": match[0].get("Name", ""),
        "run_id": run_id,
        "status": detail.get("RunState", run.get("Status", "")),
        "started_at": str(detail.get("CreatedAt", run.get("StartedAt", ""))),
        "ended_at": str(detail.get("ModifiedAt", run.get("EndedAt", ""))),
    }

    # Include error message if present
    if detail.get("ErrorMessage"):
        result["error_message"] = detail["ErrorMessage"]

    # Include task details if available
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

    # Include failure reason if failed
    if run.get("FailureReason"):
        result["failure_reason"] = run["FailureReason"]

    return result


def stop_workflow_run(workflow_name: str, run_id: str = "") -> dict:
    """Stop a running workflow. If no run_id, stops the latest active run."""
    client = _get_client()

    all_wf = client.list_workflows().get("Workflows", [])
    match = [w for w in all_wf if w.get("Name", "") == workflow_name]
    if not match:
        match = [w for w in all_wf if workflow_name.lower() in w.get("Name", "").lower()]
    if not match:
        return {"error": f"No workflow found matching '{workflow_name}'"}

    arn = match[0]["WorkflowArn"]

    if not run_id:
        try:
            runs = client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
            active = [
                r for r in runs
                if r.get("RunDetailSummary", {}).get("Status", "") in ("RUNNING", "STARTING", "QUEUED")
            ]
            if not active:
                return {"message": "No active runs to stop"}
            active.sort(
                key=lambda r: str(r.get("RunDetailSummary", {}).get("CreatedOn", "")),
                reverse=True,
            )
            run_id = active[0].get("RunId", "")
        except Exception as e:
            return {"error": f"Failed to list runs: {str(e)}"}

    client.stop_workflow_run(WorkflowArn=arn, RunId=run_id)
    return {"workflow_name": match[0].get("Name", ""), "run_id": run_id, "status": "STOPPING"}


def find_workflows_using_service(service_keyword: str) -> dict:
    """Find workflows that use operators for a specific AWS service by inspecting DAG YAML definitions."""
    client = _get_client()

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
    all_wf = client.list_workflows().get("Workflows", [])

    def _inspect(wf):
        try:
            arn = wf["WorkflowArn"]
            detail = _new_client().get_workflow(WorkflowArn=arn)
            s3_loc = detail.get("DefinitionS3Location", {})
            if not s3_loc:
                return None
            obj = boto3.client("s3").get_object(Bucket=s3_loc["Bucket"], Key=s3_loc["ObjectKey"])
            dag = yaml.safe_load(obj["Body"].read().decode("utf-8"))
            found = [
                {"task_id": t.get("task_id", ""), "operator": t.get("operator", "")}
                for t in _iter_tasks(dag)
                if any(kw.lower() in t.get("operator", "").lower() for kw in keywords)
            ]
            if found:
                return {"name": wf.get("Name", ""), "arn": arn, "matching_tasks": found}
        except Exception:
            pass
        return None

    matches = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_inspect, wf): wf for wf in all_wf}
        for f in as_completed(futures):
            result = f.result()
            if result:
                matches.append(result)

    return {"service": service_keyword, "count": len(matches), "workflows": matches}


def _resolve_workflow_arn(workflow_name: str) -> tuple:
    """Resolve a workflow name to (arn, name, all_workflows). Returns (None, error_msg, None) on failure."""
    client = _get_client()
    all_wf = client.list_workflows().get("Workflows", [])
    match = [w for w in all_wf if w.get("Name", "") == workflow_name]
    if not match:
        match = [w for w in all_wf if workflow_name.lower() in w.get("Name", "").lower()]
    if not match:
        return None, f"No workflow found matching '{workflow_name}'", None
    return match[0]["WorkflowArn"], match[0].get("Name", ""), all_wf


def deploy_and_run(workflow_name: str, yaml_content: str, s3_bucket: str, execution_role_arn: str,
                   s3_key: str = "", code_zip_base64: str = "", code_s3_key: str = "",
                   trigger_mode: str = "", start_run: bool = True) -> dict:
    """Upload the definition (and optional code bundle) to S3, create or update the
    workflow, and start a run.

    Pass code_zip_base64 when the DAG has PythonOperator or BashOperator tasks —
    their code is a separate S3 object referenced by the CreateWorkflow `Code`
    parameter, not part of the YAML.
    """
    client = _get_client()
    s3 = boto3.client("s3")

    if not s3_key:
        s3_key = f"workflows/{workflow_name}.yaml"

    # Validate before touching AWS — a bad definition is rejected anyway, and the
    # local error message is far more actionable than the service's.
    try:
        import validator
        pre = validator.validate(yaml_content)
        if not pre["valid"]:
            return {
                "error": "Definition failed local validation; nothing was deployed.",
                "validation_errors": pre["errors"],
                "hint": "Call repair_dag_yaml to fix the mechanical problems, then redeploy.",
            }
    except Exception:
        pre = None

    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=yaml_content.encode("utf-8"))
    except Exception as e:
        return {"error": f"S3 upload of the definition failed: {str(e)}"}

    s3_loc = {"Bucket": s3_bucket, "ObjectKey": s3_key}

    # Optional code bundle for Python/Bash tasks
    code_arg = {}
    if code_zip_base64:
        if not _supports_code_param():
            return {"error": "This runtime's botocore does not support the CreateWorkflow `Code` "
                             "parameter. Upgrade boto3 to >= 1.40 to deploy Python/Bash tasks."}
        import base64
        try:
            blob = base64.b64decode(code_zip_base64)
        except Exception as e:
            return {"error": f"code_zip_base64 is not valid base64: {e}"}
        ckey = code_s3_key or f"workflows/code/{workflow_name}.zip"
        try:
            s3.put_object(Bucket=s3_bucket, Key=ckey, Body=blob)
        except Exception as e:
            return {"error": f"S3 upload of the code bundle failed: {str(e)}"}
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
        all_wf = client.list_workflows().get("Workflows", [])
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
                         code_zip_base64: str = "") -> dict:
    """Have MWAA Serverless itself validate a definition, then clean up.

    Creates a throwaway workflow to exercise the service's own validator, reports
    the verdict (including the Warnings list), and deletes it again. This catches
    anything the local validator cannot know about — such as an operator argument
    the installed provider version does not accept.
    """
    import uuid
    import validator

    local = validator.validate(yaml_content)
    out = {"local_validation": {"valid": local["valid"], "errors": local["errors"],
                                "warnings": local["warnings"], "hints": local["hints"]}}
    if not local["valid"]:
        out["service_validation"] = "skipped — fix the local errors first"
        out["valid"] = False
        return out

    client = _get_client()
    s3 = boto3.client("s3")
    probe = f"preflight-{uuid.uuid4().hex[:8]}"
    key = f"preflight/{probe}.yaml"
    arn = None
    try:
        s3.put_object(Bucket=s3_bucket, Key=key, Body=yaml_content.encode("utf-8"))
    except Exception as e:
        out["service_validation"] = f"skipped — S3 upload failed: {e}"
        out["valid"] = local["valid"]
        return out

    extra = {}
    if code_zip_base64 and _supports_code_param():
        import base64
        ckey = f"preflight/{probe}.zip"
        try:
            s3.put_object(Bucket=s3_bucket, Key=ckey, Body=base64.b64decode(code_zip_base64))
            extra["Code"] = {"S3Location": {"Bucket": s3_bucket, "ObjectKey": ckey}}
        except Exception:
            pass

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
        out["valid"] = True
    except Exception as e:
        msg = str(e)
        out["service_validation"] = "rejected"
        out["service_error"] = msg.split("Workflow validation failed:")[-1].strip() or msg
        out["valid"] = False
    finally:
        if arn:
            try:
                client.delete_workflow(WorkflowArn=arn)
                out["cleanup"] = "throwaway workflow deleted"
            except Exception as e:
                out["cleanup"] = f"could not delete throwaway workflow {arn}: {e}"
        try:
            s3.delete_object(Bucket=s3_bucket, Key=key)
        except Exception:
            pass

    if out.get("service_warnings"):
        out["service_warnings_note"] = (
            "The service will silently drop these attributes. Remove them from the definition."
        )
    return out


def poll_workflow_run(workflow_name: str, run_id: str = "", max_seconds: int = 0) -> dict:
    """Poll a workflow run until terminal state or timeout. Returns final status with error details."""
    client = _get_client()

    # Leave headroom under the Lambda timeout so the function returns the current
    # status rather than being killed mid-poll. The Function URL transport allows a
    # far longer request than API Gateway's old 29s cap, so a single call can now
    # wait out most task transitions instead of forcing the client to re-poll.
    _default_poll, _max_poll = _poll_limits()
    budget = max(5, min(int(max_seconds or _default_poll), _max_poll))

    arn, name, _ = _resolve_workflow_arn(workflow_name)
    if arn is None:
        return {"error": name}

    # Resolve run_id if not provided
    if not run_id:
        try:
            runs = client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
            if not runs:
                return {"workflow_name": name, "message": "No runs found"}
            runs.sort(
                key=lambda r: str(r.get("RunDetailSummary", {}).get("CreatedOn", "")),
                reverse=True,
            )
            run_id = runs[0].get("RunId", "")
        except Exception as e:
            return {"error": f"Failed to list runs: {str(e)}"}

    terminal_states = {"SUCCESS", "FAILED", "STOPPED", "TIMEOUT"}
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
            runs = client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
            if not runs:
                return {"error": f"No runs found for '{name}'"}
            runs.sort(key=lambda r: str(r.get("RunDetailSummary", {}).get("CreatedOn", "")), reverse=True)
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
    deadline = time.time() + max(0, int(wait_for_logs_seconds))
    attempts = 0

    while True:
        attempts += 1
        outcome = _read_task_outcomes(arn, run_id, declared_tasks, include_logs, max_error_lines)
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
        result["conclusion"] = "Every task completed successfully."
    elif result["unknown"] or result["no_logs_yet"]:
        result["conclusion"] = (
            f"No task failed, but {len(result['unknown']) + len(result['no_logs_yet'])} task(s) "
            f"have not written a final marker yet. CloudWatch can lag a completed run by a few "
            f"seconds — call again to confirm."
            if run_is_terminal else
            "The run is still in progress; tasks without a final marker are still executing."
        )
    return result


def _read_task_outcomes(arn, run_id, declared_tasks, include_logs, max_error_lines):
    """One pass over a run's task log streams. Returns the outcome dict, or
    {"reason": ...} when the log group does not exist yet."""
    logs_client = boto3.client("logs")
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
        # Keep the highest attempt, which is the final outcome after retries.
        if prev is None or (attempt or "0") >= (prev.get("attempt") or "0"):
            task_results[task_id] = outcome

    failed = sorted(t["task_id"] for t in task_results.values() if t["final_state"] == "failed")
    succeeded = sorted(t["task_id"] for t in task_results.values() if t["final_state"] == "success")
    unknown = sorted(t["task_id"] for t in task_results.values()
                     if t["final_state"] not in ("failed", "success"))
    missing = sorted(t for t in declared_tasks if t not in task_results)

    return {
        "all_tasks_succeeded": bool(succeeded) and not failed and not unknown and not missing,
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
    logs_client = boto3.client("logs")
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
            except Exception:
                continue
    except Exception:
        pass

    return task_logs


def get_failed_runs_summary(name_contains: str = "", hours_back: int = 24, analyze: bool = True,
                            include_hidden_failures: bool = True) -> dict:
    """Scan workflows for recent failures, collect errors + CloudWatch logs, and optionally analyze with Bedrock.

    With include_hidden_failures (default), runs that report SUCCESS are also
    inspected for tasks that actually failed. A run whose last task succeeded
    reports SUCCESS even when an earlier task raised, so filtering on
    RunState == FAILED alone silently misses real breakage.
    """
    client = _get_client()
    from datetime import datetime, timezone, timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    # Get workflows
    all_wf = client.list_workflows().get("Workflows", [])
    if name_contains:
        name_lower = name_contains.lower()
        all_wf = [w for w in all_wf if name_lower in w.get("Name", "").lower()]

    def _check_workflow(wf):
        """Check a single workflow for failed runs and pull logs."""
        try:
            wf_client = _new_client()
            arn = wf["WorkflowArn"]
            runs = wf_client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
            failures = []
            hidden = []
            for r in runs:
                summary = r.get("RunDetailSummary", {})
                status = summary.get("Status")
                created = summary.get("CreatedOn", "")
                if created:
                    try:
                        if datetime.fromisoformat(str(created)) < cutoff:
                            continue
                    except ValueError:
                        pass
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
            if out:
                return {"name": wf.get("Name", ""), "arn": arn, **out}
        except Exception:
            pass
        return None

    # Parallel scan
    failed_workflows = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_check_workflow, wf): wf for wf in all_wf}
        for f in as_completed(futures):
            result = f.result()
            if result:
                failed_workflows.append(result)

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
    if ai_analysis:
        result["analysis"] = ai_analysis
        result["analysis_model"] = ai_model
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


def delete_workflows(name_contains: str = "", not_run_in_days: int = 0, dry_run: bool = True) -> dict:
    """Delete workflows matching criteria. Supports filtering by name and by inactivity period.
    Always does a dry run first unless dry_run=False."""
    client = _get_client()
    from datetime import datetime, timezone, timedelta

    all_wf = client.list_workflows().get("Workflows", [])

    # Filter by name
    if name_contains:
        name_lower = name_contains.lower()
        all_wf = [w for w in all_wf if name_lower in w.get("Name", "").lower()]

    # Filter by inactivity — check last run date
    if not_run_in_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=not_run_in_days)
        inactive = []

        def _check_inactive(wf):
            try:
                wf_client = _new_client()
                arn = wf["WorkflowArn"]
                runs = wf_client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
                if not runs:
                    # Never run — include it
                    return wf
                latest = max(
                    str(r.get("RunDetailSummary", {}).get("CreatedOn", ""))
                    for r in runs
                )
                if latest and datetime.fromisoformat(latest) < cutoff:
                    return wf
            except Exception:
                pass
            return None

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_check_inactive, wf): wf for wf in all_wf}
            for f in as_completed(futures):
                result = f.result()
                if result:
                    inactive.append(result)
        all_wf = inactive

    targets = [{"name": w.get("Name", ""), "arn": w["WorkflowArn"]} for w in all_wf]

    if dry_run:
        return {
            "dry_run": True,
            "count": len(targets),
            "workflows_to_delete": targets,
            "message": "Set dry_run=false to actually delete these workflows.",
        }

    # Actually delete
    results = []
    for t in targets:
        try:
            client.delete_workflow(WorkflowArn=t["arn"])
            results.append({"name": t["name"], "status": "deleted"})
        except Exception as e:
            results.append({"name": t["name"], "status": "error", "error": str(e)})

    return {"dry_run": False, "count": len(results), "results": results}


def list_runs(name_contains: str = "", status: str = "", hours_back: int = 0) -> dict:
    """List workflow runs with optional filters. Can filter by workflow name, run status, and time window."""
    client = _get_client()
    from datetime import datetime, timezone, timedelta

    cutoff = None
    if hours_back > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    all_wf = client.list_workflows().get("Workflows", [])
    if name_contains:
        name_lower = name_contains.lower()
        all_wf = [w for w in all_wf if name_lower in w.get("Name", "").lower()]

    status_upper = status.upper() if status else ""

    def _get_runs(wf):
        try:
            wf_client = _new_client()
            arn = wf["WorkflowArn"]
            runs = wf_client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
            matched = []
            for r in runs:
                summary = r.get("RunDetailSummary", {})
                run_status = summary.get("Status", "")
                created = summary.get("CreatedOn", "")

                if status_upper and run_status.upper() != status_upper:
                    continue
                if cutoff and created:
                    try:
                        if datetime.fromisoformat(str(created)) < cutoff:
                            continue
                    except Exception:
                        pass

                matched.append({
                    "run_id": r.get("RunId", ""),
                    "status": run_status,
                    "created": str(created),
                    "started": str(summary.get("StartedAt", "")),
                    "ended": str(summary.get("EndedAt", "")),
                })
            if matched:
                return {"name": wf.get("Name", ""), "runs": matched}
        except Exception:
            pass
        return None

    all_runs = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_get_runs, wf): wf for wf in all_wf}
        for f in as_completed(futures):
            result = f.result()
            if result:
                all_runs.append(result)

    # Summary counts
    total_runs = sum(len(w["runs"]) for w in all_runs)
    status_counts = {}
    for w in all_runs:
        for r in w["runs"]:
            s = r["status"]
            status_counts[s] = status_counts.get(s, 0) + 1

    return {
        "workflows_with_runs": len(all_runs),
        "total_runs": total_runs,
        "status_summary": status_counts,
        "workflows": all_runs,
    }


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

    # Get operator count from S3
    s3_loc = detail.get("DefinitionS3Location", {})
    if s3_loc:
        info["definition_s3"] = f"s3://{s3_loc.get('Bucket','')}/{s3_loc.get('ObjectKey','')}"
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=s3_loc["Bucket"], Key=s3_loc["ObjectKey"])
            dag = yaml.safe_load(obj["Body"].read().decode("utf-8"))
            operators = {}
            for t in _iter_tasks(dag):
                op = t.get("operator", "")
                if op:
                    short = op.rsplit(".", 1)[-1] if "." in op else op
                    operators[short] = operators.get(short, 0) + 1
            info["task_count"] = sum(operators.values())
            info["operators"] = operators
        except Exception:
            pass

    # Get latest run status
    try:
        runs = client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
        if runs:
            runs.sort(
                key=lambda r: str(r.get("RunDetailSummary", {}).get("CreatedOn", "")),
                reverse=True,
            )
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
    except Exception:
        pass

    return info


def bulk_status(name_contains: str = "", names: list = None) -> dict:
    """Get status of multiple workflows with their latest run in one call."""
    client = _get_client()

    all_wf = client.list_workflows().get("Workflows", [])

    if names:
        names_lower = [n.lower() for n in names]
        all_wf = [w for w in all_wf if any(n in w.get("Name", "").lower() for n in names_lower)]
    elif name_contains:
        name_lower = name_contains.lower()
        all_wf = [w for w in all_wf if name_lower in w.get("Name", "").lower()]

    def _get_status(wf):
        try:
            wf_client = _new_client()
            arn = wf["WorkflowArn"]
            info = {"name": wf.get("Name", ""), "workflow_status": wf.get("WorkflowStatus", "")}
            runs = wf_client.list_workflow_runs(WorkflowArn=arn).get("WorkflowRuns", [])
            if runs:
                runs.sort(
                    key=lambda r: str(r.get("RunDetailSummary", {}).get("CreatedOn", "")),
                    reverse=True,
                )
                s = runs[0].get("RunDetailSummary", {})
                info["latest_run"] = {
                    "status": s.get("Status", ""),
                    "created": str(s.get("CreatedOn", "")),
                }
            else:
                info["latest_run"] = None
            return info
        except Exception:
            return {"name": wf.get("Name", ""), "error": "Failed to get status"}

    results = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_get_status, wf): wf for wf in all_wf}
        for f in as_completed(futures):
            results.append(f.result())

    # Sort by name
    results.sort(key=lambda r: r.get("name", ""))

    return {"count": len(results), "workflows": results}


def redeploy_workflow(workflow_name: str, yaml_content: str, s3_bucket: str = "", s3_key: str = "") -> dict:
    """Update an existing workflow's YAML and start a new run. Reuses existing S3 location and role if not specified."""
    client = _get_client()

    arn, name, _ = _resolve_workflow_arn(workflow_name)
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
    s3 = boto3.client("s3")
    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=yaml_content.encode("utf-8"))
    except Exception as e:
        return {"error": f"S3 upload failed: {str(e)}"}

    # Update workflow
    try:
        client.update_workflow(
            WorkflowArn=arn,
            DefinitionS3Location={"Bucket": s3_bucket, "ObjectKey": s3_key},
            RoleArn=role_arn,
        )
    except Exception as e:
        return {"error": f"Update failed: {str(e)}"}

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

    versions = client.list_workflow_versions(WorkflowArn=arn).get("WorkflowVersions", [])
    if len(versions) < 2:
        return {"workflow_name": name, "message": "Only one version exists, nothing to compare."}

    # Sort by created date descending
    versions.sort(key=lambda v: str(v.get("CreatedAt", "")), reverse=True)
    latest = versions[0]
    previous = versions[1]

    s3 = boto3.client("s3")
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
