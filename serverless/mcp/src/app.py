"""MCP Server Lambda handler for the MWAA Serverless workflow assistant."""

import json
import logging
import os
from typing import Optional

from awslabs.mcp_lambda_handler import MCPLambdaHandler

import builder as _builder
import codebundle as _codebundle
import config as _config
from tools import (
    generate_yaml, validate_yaml, repair_yaml, list_operators, list_unsupported,
    get_constraints, get_overview, get_dag_yaml_spec as _get_dag_yaml_spec,
    describe_operator as _describe_operator,
    generate_execution_role_policy,
    get_service_tasks as _get_service_tasks, compose_dag_yaml as _compose_dag_yaml,
)
from python_analyzer import analyze_python_dag
from python_converter import convert_python_to_yaml
from operations import (
    list_workflows as _list_workflows,
    get_workflow as _get_workflow,
    start_workflow_run as _start_workflow_run,
    get_workflow_run_status as _get_workflow_run_status,
    stop_workflow_run as _stop_workflow_run,
    find_workflows_using_service as _find_workflows_using_service,
    deploy_and_run as _deploy_and_run,
    poll_workflow_run as _poll_workflow_run,
    verify_run_tasks as _verify_run_tasks,
    preflight_definition as _preflight_definition,
    get_failed_runs_summary as _get_failed_runs_summary,
    delete_workflows as _delete_workflows,
    list_runs as _list_runs,
    get_workflow_summary as _get_workflow_summary,
    bulk_status as _bulk_status,
    redeploy_workflow as _redeploy_workflow,
    compare_versions as _compare_versions,
)

logger = logging.getLogger()
logger.setLevel(_config.get("log_level") or os.environ.get("LOG_LEVEL", "INFO"))

mcp_server = MCPLambdaHandler(name="mwaa-serverless-mcp", version="3.1.0")


def _j(obj):
    return json.dumps(obj, indent=2, default=str)


# ══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
def get_server_config() -> str:
    """Show this server's effective configuration and where each value came from.

    Use this when an AI-assisted result is missing or a poll returns sooner than
    expected — it distinguishes a misconfiguration from a bug, rather than leaving
    you to infer settings from behaviour.

    Reports the resolved values, their provenance (default, config file, or
    environment variable), which Bedrock models will be tried in order, the config
    file in use and the paths searched for it, and instructions for changing the
    model locally versus in a deployed Lambda.

    Precedence is environment variable > config file > built-in default.
    """
    return _j(_config.describe())


# ══════════════════════════════════════════════════════════════════════════
#  REFERENCE
# ══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
def get_serverless_overview() -> str:
    """Read this FIRST before authoring or deploying any MWAA Serverless workflow.

    Returns how the service works plus the two things that determine whether a DAG
    actually runs: the exact YAML schema, and the authoring policy.

    THE SCHEMA IS NOT THE OBVIOUS ONE. MWAA Serverless runs dag-factory, and it
    rejects the shape most people write by default:
      - `tasks` is a MAPPING keyed by task_id, never a list
      - `operator` must be the FULLY QUALIFIED class path, never a short name
      - operator arguments sit FLAT on the task; there is no `parameters:` wrapper
      - dependencies use `dependencies:`, never `upstream_tasks:`
      - retry_delay is an integer of seconds; execution_timeout is a
        `{__type__: datetime.timedelta, minutes: N}` mapping

    THE AUTHORING POLICY: build exactly what the user asked for. Do not add
    monitoring, alerting, CloudFormation provisioning or cleanup tasks that were
    not requested. When a best practice is missing, mention it and ask — do not
    silently include it.

    COST: the service bills for the time a task occupies a worker, so waiting is not
    free — and neither Airflow mechanism for releasing it applies here. `deferrable: true`
    is ignored (no triggerer), and `mode: reschedule` is accepted at create time but not
    supported end to end. Leave sensors in poke mode, always set a timeout, and wait less
    rather than differently. See authoring_policy.cost_efficiency.

    PythonOperator and BashOperator ARE now supported (this changed recently);
    their code is uploaded separately as a code bundle.
    """
    return _j(get_overview())


@mcp_server.tool()
def get_dag_yaml_spec() -> str:
    """The authoritative MWAA Serverless YAML schema, with a worked example and the
    exact service error each mistake produces.

    Call this before writing DAG YAML by hand. Every rule was verified against the
    live CreateWorkflow API, so the failure messages quoted here are the ones the
    service will actually return. Also covers how to pass values between tasks via
    XCom, and the default_args allowlist (only 10 keys are accepted).
    """
    return _j(_get_dag_yaml_spec())


@mcp_server.tool()
def get_serverless_constraints() -> str:
    """Full constraint reference: YAML schema, authoring policy, XCom parameter
    passing, supported and unsupported Jinja variables, validated and ignored
    DAG/task parameters, the default_args allowlist, Python/Bash code bundle rules,
    service quotas, and how to read task logs.
    """
    return _j(get_constraints())


@mcp_server.tool()
def list_supported_operators(service_filter: str = "") -> str:
    """List every operator MWAA Serverless allows, as short name plus the fully
    qualified path. ALWAYS emit the `fqn` value in YAML — short names are rejected.

    Args:
        service_filter: Optional substring filter (e.g. "glue", "s3", "sensor")
    """
    return _j(list_operators(service_filter))


@mcp_server.tool()
def describe_operator(operator: str) -> str:
    """Look up one operator: its fully qualified path, the arguments it requires,
    and what it pushes to XCom for downstream tasks to consume.

    Use this before writing a task so the required arguments are present and any
    chaining uses the right XCom shape. Missing arguments are the top cause of a
    DAG that deploys cleanly and then fails every run.

    Args:
        operator: Short name (e.g. "GlueJobOperator") or a fully qualified path
    """
    return _j(_describe_operator(operator))


@mcp_server.tool()
def suggest_operator(intent: str) -> str:
    """Find the operator for a described task when you do not know its name.

    Args:
        intent: Plain description, e.g. "run a SQL query on Redshift"
    """
    return _j(_builder.suggest_operator(intent))


# ══════════════════════════════════════════════════════════════════════════
#  AUTHORING
# ══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
def list_pipeline_steps(search: str = "") -> str:
    """Catalog of pipeline operations you can compose a DAG from, each with its
    operator, required arguments and XCom chaining behaviour. Use the returned keys
    with plan_pipeline.

    Args:
        search: Optional filter, e.g. "glue", "query", "notify"
    """
    return _j(_builder.list_pipeline_steps(search))


@mcp_server.tool()
def plan_pipeline(steps: list, has_schedule: bool = False, creates_resources: bool = False) -> str:
    """START HERE when the user asks for a real pipeline. Turns the operations they
    named into a build plan BEFORE any YAML is written.

    Returns five things you must act on:
      1. planned_tasks — the operator and arguments for each step
      2. questions_to_ask_the_user — values the DAG cannot work without (job names,
         database names, bucket names, output locations). ASK THESE. Never invent an
         ARN, bucket name or account ID.
      3. deliberately_not_added — best practices intentionally left out. Mention
         them and ask whether the user wants them. Do NOT add them unprompted.
      4. cost_plan — how the plan avoids paying for idle waiting, and the one
         question only the user can answer (how long each job takes).
      5. iam_actions_needed — feeds into generate_execution_role.

    One task per operation the user actually named. If they asked for two Glue jobs
    and an Athena query, that is three or four tasks, not thirty. Then call
    build_dag_yaml.

    Args:
        steps: Step keys from list_pipeline_steps, e.g. ["glue_job", "glue_crawler", "athena_query"]
        has_schedule: True if the user already specified a schedule
        creates_resources: True only if the user asked the DAG to CREATE infrastructure
    """
    return _j(_builder.plan_pipeline(steps, has_schedule, creates_resources))


@mcp_server.tool()
def build_dag_yaml(dag_id: str, tasks: list, schedule: str = "", description: str = "",
                   params: Optional[dict] = None, default_args: Optional[dict] = None,
                   max_active_runs: Optional[int] = None, start_date: str = "") -> str:
    """PREFERRED way to produce DAG YAML. Emits the schema MWAA Serverless accepts
    and validates the result, so the six mistakes that break hand-written YAML
    (list tasks, short operator names, a `parameters:` wrapper, `upstream_tasks`,
    string durations, integer execution_timeout) cannot happen.

    Each entry in `tasks`:
        task_id                    (required) unique id
        operator                   (required) short name or FQN; always emitted as FQN
        params                     (optional) operator arguments as a mapping
        dependencies               (optional) list of upstream task_ids
        retries                    (optional) 0-3
        retry_delay_seconds        (optional) 0-300
        execution_timeout_minutes  (optional) 1-60
        trigger_rule               (optional) e.g. "all_done" for a cleanup task,
                                   "one_failed" for a failure notification
        sensor_timeout_seconds     (optional) sensors only; bounds the wait (default 3600)

    COST — this matters and is easy to get wrong. MWAA Serverless bills for the time a
    task occupies a worker, so a task sitting in a poll loop costs the same as one doing
    real work. Sensors are therefore emitted with a bounded `timeout` (Airflow's default
    is 7 days); `cost_optimizations_applied` lists what was set.

    Do NOT reach for the usual fixes: `deferrable: true` is ignored (there is no
    triggerer), and `mode: reschedule` is accepted at create time but is not supported end
    to end, so the wait never completes. Since the wait is billed either way, prefer ONE
    operator with wait_for_completion: true over an operator plus a sensor; the split adds
    a task start and saves nothing.

    To pass a value between tasks, reference the upstream task's XCom in a
    downstream argument: "{{ ti.xcom_pull(task_ids='upstream_id') }}", and list that
    task in `dependencies`. describe_operator tells you what each operator returns.

    After this returns valid YAML: preflight_dag_yaml, then generate_execution_role,
    then mwaa_deploy_and_run.

    Args:
        dag_id: Workflow/DAG identifier
        tasks: List of task specifications (see above)
        schedule: Cron expression or "@daily". Leave empty for on-demand only.
        description: Optional description
        params: DAG-level params referenced as {{ params.x }}
        default_args: Only these keys are accepted: owner, email, retries, retry_delay,
            priority_weight, end_date, wait_for_downstream, execution_timeout,
            trigger_rule, start_date
        max_active_runs: Concurrent run limit
        start_date: YYYY-MM-DD
    """
    return _j(_builder.build_dag_yaml(dag_id, tasks, schedule or None, description,
                                     params, default_args, max_active_runs, start_date))


@mcp_server.tool()
def validate_dag_yaml(yaml_content: str) -> str:
    """Validate DAG YAML against the real MWAA Serverless schema. Run this on ANY
    YAML before deploying, including YAML you wrote yourself.

    Every rule was checked against the live CreateWorkflow API. Catches: list-shaped
    tasks, short operator names, a `parameters:` wrapper, `upstream_tasks`, string
    durations, non-timedelta execution_timeout, disallowed default_args keys,
    task_groups, multiple DAGs in one file, unknown or abstract operators, missing
    required operator arguments, dependency cycles, dependencies on tasks that do
    not exist, unsupported Jinja variables, xcom_pull from a task that is not
    upstream or that pushes nothing, and definitions over the 50 KB limit.

    `errors` will break the workflow. `warnings` are attributes the service silently
    ignores. `hints` are best-practice observations, including COST hints where a wait
    is unbounded or a task split adds cost without saving any.

    If there are mechanical errors, call repair_dag_yaml to fix them automatically.

    Args:
        yaml_content: The DAG YAML to validate
    """
    return _j(validate_yaml(yaml_content))


@mcp_server.tool()
def repair_dag_yaml(yaml_content: str) -> str:
    """Rewrite DAG YAML into the form MWAA Serverless accepts, and report every
    change made.

    Fixes automatically: list tasks to a mapping, short operator names to fully
    qualified paths, `parameters:` wrapper flattened, `upstream_tasks`/`depends_on`/
    `downstream_tasks` converted to `dependencies`, duration strings to integer
    seconds, execution_timeout to a `__type__: datetime.timedelta` mapping, over-limit
    values capped, and ignored attributes (aws_conn_id, region_name, catchup, tags)
    removed.

    Cost-related repairs: `mode: reschedule` is downgraded to `poke` (accepted at create
    time but not supported end to end), `deferrable` is dropped as the service ignores it,
    `mode` in default_args is pushed down onto the sensor tasks that can accept it, and an
    invalid `mode` value is removed.

    Anything needing a human decision — an unknown operator, a missing required
    argument — comes back in `unfixable`.

    Args:
        yaml_content: The DAG YAML to repair
    """
    return _j(repair_yaml(yaml_content))


@mcp_server.tool()
def preflight_dag_yaml(yaml_content: str, s3_bucket: str, execution_role_arn: str,
                       code_zip_base64: str = "") -> str:
    """Have MWAA Serverless itself validate a definition, without leaving a workflow
    behind. The definitive correctness check before deployment.

    Runs local validation first, then creates a throwaway workflow so the service's
    own validator runs, reports the verdict and any `Warnings` (attributes it will
    silently drop), and deletes the throwaway. Catches things a local check cannot,
    such as an argument the installed provider version does not accept.

    Args:
        yaml_content: The DAG YAML to check
        s3_bucket: Bucket to stage the definition in
        execution_role_arn: Execution role ARN (only validated, never assumed)
        code_zip_base64: Optional code bundle, for DAGs with Python/Bash tasks
    """
    return _j(_preflight_definition(yaml_content, s3_bucket, execution_role_arn, code_zip_base64))


@mcp_server.tool()
def generate_execution_role(yaml_content: str) -> str:
    """Produce an IAM execution role for a DAG: trust policy, permissions policy
    scoped to the API calls its operators actually make, and the CLI commands to
    create it.

    Actions are least-privilege per operator rather than service wildcards.
    iam:PassRole is added only when a task genuinely hands a role to another service,
    and is constrained with iam:PassedToService. Resources are still "*" — the
    response lists how to narrow them.

    Args:
        yaml_content: The DAG YAML to analyse
    """
    return _j(generate_execution_role_policy(yaml_content))


# ══════════════════════════════════════════════════════════════════════════
#  PYTHON / BASH CODE BUNDLES
# ══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
def get_code_bundle_guidance() -> str:
    """How to write and package code for PythonOperator and BashOperator tasks.

    Covers the required `python_callable` format (module.function), the flat-archive
    requirement, the worker environment (1 vCPU / 3 GiB, Python 3.12, code extracted
    to /usr/local/airflow/dags), pre-installed packages you must not bundle, how to
    build a dependency zip for Linux x86_64, and the fact that tasks have NO internet
    access unless a VPC is attached.

    Prefer a native AWS operator whenever one exists. Use PythonOperator for genuine
    glue logic only.
    """
    return _j(_codebundle.code_bundle_guidance())


@mcp_server.tool()
def build_code_bundle(files: dict, bundle_name: str = "code.zip") -> str:
    """Package Python modules and shell scripts into a deployable code bundle.

    Returns base64 to pass straight to mwaa_deploy_and_run as code_zip_base64, plus
    the list of callables the bundle exposes for use in `python_callable`. Checks
    each module parses, that filenames are flat (nested directories are not
    importable on the worker), that callables accept the Airflow context, and that
    no AWS credentials are hard-coded. Flags imports that cannot work without
    internet access.

    Args:
        files: Mapping of filename to file contents, e.g.
            {"transform.py": "def clean(**context): ...", "run.sh": "#!/bin/bash\\n..."}
        bundle_name: Name for the archive
    """
    return _j(_codebundle.build_code_bundle(files, bundle_name))


@mcp_server.tool()
def check_dag_code_consistency(yaml_content: str, files: Optional[dict] = None) -> str:
    """Cross-check a DAG's Python/Bash tasks against its code bundle.

    Confirms every `python_callable: module.function` resolves to a real function in
    a real module, and that any script named in a `bash_command` is present. This is
    the failure that dominates first Python/Bash deployments — the workflow creates
    successfully and the task then dies on import.

    Args:
        yaml_content: The DAG YAML
        files: The same {filename: contents} mapping passed to build_code_bundle
    """
    return _j(_codebundle.check_dag_code_consistency(yaml_content, files))


# ══════════════════════════════════════════════════════════════════════════
#  DEMO TEMPLATES AND PYTHON MIGRATION
# ══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
def generate_dag_yaml(dag_id: str, service: str, description: str = "",
                      schedule: str = "None", params: Optional[dict] = None) -> str:
    """Generate a self-contained DEMO workflow for one AWS service, to try that
    service out end to end.

    These templates provision their own prerequisites with CloudFormation and tear
    them down, so they run in an empty account. That also makes them a poor starting
    point for a real pipeline — for that use plan_pipeline then build_dag_yaml, which
    produces one task per operation the user actually asked for.

    Check `params_you_must_set` in the response: where a template previously read
    CloudFormation stack outputs from XCom (which returns None and fails at run
    time), the reference is now a param you must fill in.

    Args:
        dag_id: Workflow/DAG identifier
        service: s3, glue, athena, bedrock, lambda, emr_serverless, emr, batch,
            step_functions, redshift, sns, sqs, ecs, eks, cloudformation, sagemaker,
            rds, ec2, eventbridge, comprehend, dms, kinesis_analytics, neptune,
            glacier, datasync, appflow, quicksight, dynamodb, opensearch_serverless
        description: Optional description
        schedule: Cron expression, "@daily", or "None" for on-demand only
        params: Override the template's default params
    """
    return _j(generate_yaml(dag_id, service, description, schedule, params))


@mcp_server.tool()
def get_service_tasks_tool(service: str) -> str:
    """Inspect one service's demo task block as a reusable building block.

    Args:
        service: Service name (e.g. s3, glue, athena)
    """
    return _j(_get_service_tasks(service))


@mcp_server.tool()
def compose_dag_yaml_tool(dag_id: str, services_config: list, description: str = "",
                          schedule: str = "None", params: Optional[dict] = None) -> str:
    """Chain several self-contained service DEMO blocks into one workflow.

    Each block keeps its own provisioning and cleanup, so the result is large. This
    demonstrates several services together; it is NOT how to build a production
    pipeline. Use plan_pipeline then build_dag_yaml for that.

    Args:
        dag_id: Workflow/DAG identifier
        services_config: e.g. [{"service": "s3"}, {"service": "glue", "depends_on": ["s3"]}]
        description: Optional description
        schedule: Cron expression, "@daily", or "None"
        params: DAG-level param overrides (keys are prefixed, e.g. s3_bucket_name)
    """
    return _j(_compose_dag_yaml(dag_id, services_config, description, schedule, params))


@mcp_server.tool()
def analyze_python_dag_tool(python_source: str) -> str:
    """Analyse a Python Airflow DAG for MWAA Serverless compatibility before
    converting it.

    Reports blockers (operators outside the allowlist, dynamic task mapping,
    task groups, decorated tasks, module-level Python logic that cannot be expressed
    in YAML) and warnings (parameters the service ignores).

    Note that PythonOperator and BashOperator are now SUPPORTED — they convert to
    YAML tasks plus a code bundle rather than being blockers.

    Args:
        python_source: The Python DAG source
    """
    return _j(analyze_python_dag(python_source))


@mcp_server.tool()
def convert_python_to_yaml_tool(python_source: str) -> str:
    """Convert a Python Airflow DAG to MWAA Serverless YAML.

    Extracts tasks, operators, arguments and >> dependency chains via AST and emits
    the mapping-shaped, fully-qualified YAML the service accepts. Always validate the
    result with validate_dag_yaml and review anything listed as replaced or dropped.

    Args:
        python_source: The Python DAG source
    """
    return _j(convert_python_to_yaml(python_source))


# ══════════════════════════════════════════════════════════════════════════
#  WORKFLOW OPERATIONS
# ══════════════════════════════════════════════════════════════════════════

@mcp_server.tool()
def mwaa_deploy_and_run(workflow_name: str, yaml_content: str, s3_bucket: str,
                        execution_role_arn: str, s3_key: str = "",
                        code_zip_base64: str = "", code_s3_key: str = "",
                        trigger_mode: str = "", start_run: bool = True) -> str:
    """Upload the definition (and code bundle, if any), create or update the
    workflow, and start a run.

    Validates locally first and refuses to deploy a broken definition. Surfaces the
    service's `Warnings` list, which is how MWAA Serverless reports attributes it
    silently dropped — act on those rather than ignoring them.

    After the run starts: poll with mwaa_poll_run, then ALWAYS confirm with
    mwaa_verify_run_tasks. A run can report SUCCESS while individual tasks failed.

    Args:
        workflow_name: Name for the workflow
        yaml_content: The DAG YAML
        s3_bucket: Bucket for the definition (and code bundle)
        execution_role_arn: Execution role ARN
        s3_key: Optional key. Defaults to workflows/{workflow_name}.yaml
        code_zip_base64: Code bundle from build_code_bundle. REQUIRED if the DAG has
            PythonOperator or BashOperator tasks.
        code_s3_key: Optional key for the code bundle, or point at an existing object
        trigger_mode: SCHEDULED | MANUAL | DISABLED
        start_run: Set false to deploy without starting a run
    """
    return _j(_deploy_and_run(workflow_name, yaml_content, s3_bucket, execution_role_arn,
                              s3_key, code_zip_base64, code_s3_key, trigger_mode, start_run))


@mcp_server.tool()
def mwaa_verify_run_tasks(workflow_name: str, run_id: str = "", include_logs: bool = True,
                          wait_for_logs_seconds: int = 45) -> str:
    """Determine each task's REAL outcome by reading its CloudWatch log stream.

    Call this after every run. RunState is not a reliable indicator: a run whose
    final task succeeded reports SUCCESS even when an earlier task raised — verified
    against the live service. Each task's log stream ends with a `final_state` marker,
    which is authoritative.

    Returns per-task final_state, the exception type and message for failures, and a
    `discrepancy` field when the run claims SUCCESS but tasks failed.

    Args:
        workflow_name: Workflow name (exact or partial)
        run_id: Optional run ID. Defaults to the latest run.
        include_logs: Include error lines from each task's log
        wait_for_logs_seconds: CloudWatch lags a completed run by a few seconds, so a
            task checked the instant a run turns SUCCESS may not have flushed its final
            marker. When that happens the read is retried for up to this long rather
            than reporting the task as indeterminate. Set 0 for a single fast read.
    """
    return _j(_verify_run_tasks(workflow_name, run_id, include_logs,
                                wait_for_logs_seconds=wait_for_logs_seconds))


@mcp_server.tool()
def mwaa_poll_run(workflow_name: str, run_id: str = "", max_seconds: int = 0) -> str:
    """Poll a run until it reaches a terminal state or the timeout expires. If it
    returns completed=false, call again to keep polling.

    A terminal SUCCESS is not proof every task succeeded — follow up with
    mwaa_verify_run_tasks.

    Args:
        workflow_name: Workflow name (exact or partial)
        run_id: Optional run ID. Defaults to the latest run.
        max_seconds: Seconds to poll before returning. 0 uses the configured default
            (90s), capped at the configured maximum (110s) to stay under the Lambda
            timeout. See get_server_config.
    """
    return _j(_poll_workflow_run(workflow_name, run_id, max_seconds))


@mcp_server.tool()
def mwaa_get_failed_runs(name_contains: str = "", hours_back: int = 24,
                         analyze: bool = True, include_hidden_failures: bool = True) -> str:
    """Scan workflows for recent failures, pull the CloudWatch task logs, and
    optionally get an AI root-cause analysis.

    With include_hidden_failures (default), runs reported as SUCCESS are also
    inspected for tasks that actually failed — filtering on FAILED alone misses real
    breakage, because a trailing all_done task turns a failed run green.

    Args:
        name_contains: Only scan workflows matching this substring
        hours_back: How far back to look (default 24)
        analyze: Include Bedrock root-cause analysis. The model is configurable —
            see get_server_config. The response reports analysis_model, or
            analysis_unavailable with the reason if no model could be invoked.
        include_hidden_failures: Also inspect SUCCESS runs for failed tasks
    """
    return _j(_get_failed_runs_summary(name_contains, hours_back, analyze, include_hidden_failures))


@mcp_server.tool()
def mwaa_get_run_status(workflow_name: str, run_id: str = "") -> str:
    """Run status with per-task detail and error messages. Defaults to the latest run.
    For an authoritative per-task verdict use mwaa_verify_run_tasks.

    Args:
        workflow_name: Workflow name (exact or partial)
        run_id: Optional run ID
    """
    return _j(_get_workflow_run_status(workflow_name, run_id))


@mcp_server.tool()
def mwaa_list_workflows(name_contains: str = "", status: str = "") -> str:
    """List workflows: name, status, trigger mode, ARN, last modified.

    Args:
        name_contains: Optional name substring filter
        status: Optional status filter (READY, DELETING)
    """
    return _j(_list_workflows(name_contains, status))


@mcp_server.tool()
def mwaa_get_workflow(workflow_name: str) -> str:
    """Full workflow detail including the deployed DAG YAML, operators used,
    execution role, S3 locations, code bundle, log group, and a validation check of
    the deployed definition.

    The YAML comes from the immutable snapshot the workflow actually runs, not from
    the S3 object, so it stays accurate even if the bucket has since changed.

    Args:
        workflow_name: Workflow name (exact or partial)
    """
    return _j(_get_workflow(workflow_name))


@mcp_server.tool()
def mwaa_get_workflow_summary(workflow_name: str) -> str:
    """Compact overview: status, task and operator counts, latest run, run history
    stats. Much lighter than mwaa_get_workflow.

    Args:
        workflow_name: Workflow name (exact or partial)
    """
    return _j(_get_workflow_summary(workflow_name))


@mcp_server.tool()
def mwaa_bulk_status(name_contains: str = "", names: str = "") -> str:
    """Status of several workflows with their latest run result in one call.

    Args:
        name_contains: Filter by name substring
        names: Comma-separated name substrings
    """
    names_list = [n.strip() for n in names.split(",") if n.strip()] if names else None
    return _j(_bulk_status(name_contains, names_list))


@mcp_server.tool()
def mwaa_start_run(workflow_name: str) -> str:
    """Start a workflow run.

    Args:
        workflow_name: Workflow name (exact or partial)
    """
    return _j(_start_workflow_run(workflow_name))


@mcp_server.tool()
def mwaa_stop_run(workflow_name: str, run_id: str = "") -> str:
    """Stop a running workflow. Defaults to the latest active run.

    Args:
        workflow_name: Workflow name (exact or partial)
        run_id: Optional run ID
    """
    return _j(_stop_workflow_run(workflow_name, run_id))


@mcp_server.tool()
def mwaa_list_runs(name_contains: str = "", status: str = "", hours_back: int = 0) -> str:
    """List runs across workflows with status and time filters.

    Args:
        name_contains: Filter by workflow name substring
        status: SUCCESS, FAILED, RUNNING, QUEUED, STOPPED, TIMEOUT
        hours_back: Only runs from the last N hours (0 = all)
    """
    return _j(_list_runs(name_contains, status, hours_back))


@mcp_server.tool()
def mwaa_find_workflows_by_service(service: str) -> str:
    """Find workflows that use a given AWS service by inspecting their DAG YAML.

    Args:
        service: e.g. step_functions, s3, glue, lambda, bedrock
    """
    return _j(_find_workflows_using_service(service))


@mcp_server.tool()
def mwaa_redeploy(workflow_name: str, yaml_content: str, s3_bucket: str = "", s3_key: str = "") -> str:
    """Update an existing workflow's YAML and start a new run, reusing its existing
    S3 location and execution role.

    Args:
        workflow_name: Existing workflow name (exact or partial)
        yaml_content: New DAG YAML
        s3_bucket: Optional bucket override
        s3_key: Optional key override
    """
    return _j(_redeploy_workflow(workflow_name, yaml_content, s3_bucket, s3_key))


@mcp_server.tool()
def mwaa_compare_versions(workflow_name: str) -> str:
    """Diff the latest two versions of a workflow's YAML.

    Args:
        workflow_name: Workflow name (exact or partial)
    """
    return _j(_compare_versions(workflow_name))


@mcp_server.tool()
def mwaa_delete_workflows(name_contains: str = "", not_run_in_days: int = 0,
                          dry_run: bool = True) -> str:
    """Delete workflows by name pattern or inactivity. Previews by default.

    Deletion is irreversible and removes all versions. Show the dry-run result and
    get explicit confirmation before calling again with dry_run=false.

    Args:
        name_contains: Delete workflows whose name contains this substring
        not_run_in_days: Delete workflows not run in this many days (0 = ignore)
        dry_run: True (default) previews. False actually deletes.
    """
    return _j(_delete_workflows(name_contains, not_run_in_days, dry_run))


def handler(event, context):
    return mcp_server.handle_request(event, context)


# ── Keep the full docstring as the tool description ──
# MCPLambdaHandler derives `description` from the docstring's summary line only,
# so everything after the first blank line is dropped. Most of the guidance in
# these docstrings — the schema rules, the ask-before-adding policy, the required
# call order — lives below that line, and an agent that never sees it goes back to
# guessing. Rewrite each description with the full body (the Args block still
# becomes inputSchema, so it is excluded here).
def _restore_full_tool_descriptions(server) -> int:
    import inspect
    import re

    fixed = 0
    for name, spec in server.tools.items():
        fn = server.tool_implementations.get(name)
        doc = inspect.getdoc(fn) if fn else None
        if not doc:
            continue
        body = re.split(r"\n\s*Args:\s*\n", doc, maxsplit=1)[0].strip()
        if body and body != spec.get("description"):
            spec["description"] = body
            fixed += 1
    return fixed


_TOOL_DESCRIPTIONS_RESTORED = _restore_full_tool_descriptions(mcp_server)
logger.info("Restored full descriptions for %d tools", _TOOL_DESCRIPTIONS_RESTORED)
