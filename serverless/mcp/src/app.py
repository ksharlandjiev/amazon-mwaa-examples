# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""MCP Server Lambda handler for the MWAA Serverless workflow assistant."""

import json
import logging
import os
from typing import Optional

from awslabs.mcp_lambda_handler import MCPLambdaHandler

import builder as _builder
import codebundle as _codebundle
import config as _config
import operations as _operations
from tools import (
    generate_yaml, validate_yaml, repair_yaml, list_operators,
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

    COST: the service bills for the time a task occupies a worker, so waiting is not free.
    Sensors should set `mode: reschedule`, which releases the worker between checks;
    `deferrable: true` looks like the answer but is ignored, because there is no
    triggerer. Always bound a wait with a timeout. See authoring_policy.cost_efficiency.

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
    string durations, integer execution_timeout) cannot reach the output.

    Nothing is changed silently. `values_adjusted` lists every value coerced, capped
    or renamed on your behalf (a duration string parsed to seconds, a retry_delay
    above the 300s maximum capped, `upstream_tasks` renamed to `dependencies`), and
    `build_problems` holds anything that could not be interpreted — those are never
    replaced with a plausible default.

    Each entry in `tasks`:
        task_id                    (required) unique id
        operator                   (required) short name or FQN; always emitted as FQN
        params                     (optional) operator arguments as a mapping
        dependencies               (optional) list of upstream task_ids
        retries                    (optional) 0-3
        retry_delay_seconds        (optional) 0-300, or a duration like "90s"/"5m"
        execution_timeout_minutes  (optional) 1-60
        trigger_rule               (optional) e.g. "all_done" for a cleanup task,
                                   "one_failed" for a failure notification
        sensor_timeout_seconds     (optional) sensors only; bounds the wait (default 3600)

    COST — this matters and is easy to get wrong. MWAA Serverless bills for the time a
    task occupies a worker, so a task sitting in a poll loop costs the same as one doing
    real work. Sensors are therefore emitted with `mode: reschedule`, a `poke_interval`
    and a bounded `timeout`, which releases the worker between checks. Pass
    params {"mode": "poke"} only for a wait that resolves in a minute or two.
    `cost_optimizations_applied` lists what was set.

    Do NOT use `deferrable: true` — MWAA Serverless has no triggerer and ignores it. For a
    job running more than a few minutes, prefer wait_for_completion: false on the operator
    plus a reschedule-mode sensor over a blocking wait.

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
    ignores. `hints` are best-practice observations, including COST hints where a sensor
    holds a worker longer than it needs to or a wait is unbounded.

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

    Cost-related repairs: `deferrable` on a sensor becomes `mode: reschedule` (the
    mechanism that actually releases the worker), `mode` in default_args is pushed down
    onto the sensor tasks that can accept it, and an invalid `mode` value is removed.

    Anything needing a human decision — an unknown operator, a missing required
    argument — comes back in `unfixable`.

    Args:
        yaml_content: The DAG YAML to repair
    """
    return _j(repair_yaml(yaml_content))


@mcp_server.tool()
def preflight_dag_yaml(yaml_content: str, s3_bucket: str, execution_role_arn: str,
                       code_zip_base64: str = "", expected_bucket_owner: str = "") -> str:
    """Have MWAA Serverless itself validate a definition, without leaving a workflow
    behind. The definitive correctness check before deployment.

    Runs local validation first, then creates a throwaway workflow so the service's
    own validator runs, reports the verdict and any `Warnings` (attributes it will
    silently drop), and deletes the throwaway. Catches things a local check cannot,
    such as an argument the installed provider version does not accept.

    Branch on `verdict`, which is three-state: "valid" means the service accepted the
    exact artifact you passed; "invalid" means it was rejected; "indeterminate" means
    the check could not be completed and NOTHING is known — a staging failure, or a
    code bundle that never reached S3 so the Python/Bash tasks went unchecked. Treat
    "indeterminate" as "not validated", never as a pass. `valid` is True only for
    "valid".

    Side effects: writes and deletes two objects in s3_bucket, and briefly consumes
    one of the 100 workflows-per-account quota slots. If cleanup fails, the response
    names the leftover workflow and objects so you can remove them. The service also
    creates a CloudWatch log group for the throwaway workflow that outlives it; it is
    left behind empty and named in `log_group_residue`.

    Args:
        yaml_content: The DAG YAML to check
        s3_bucket: Bucket to stage the definition in
        execution_role_arn: Execution role ARN (only validated, never assumed)
        code_zip_base64: Optional code bundle, for DAGs with Python/Bash tasks
        expected_bucket_owner: Your account id, to assert bucket ownership on write
    """
    return _j(_preflight_definition(yaml_content, s3_bucket, execution_role_arn, code_zip_base64,
                                    expected_bucket_owner))


@mcp_server.tool()
def generate_execution_role(yaml_content: str, account_id: str = "", region: str = "",
                            passable_role_arns: Optional[list] = None,
                            include_destructive_actions: bool = False) -> str:
    """Produce an IAM execution role for a DAG: trust policy, permissions policy
    scoped to the API calls its operators actually make, and the CLI commands to
    create it.

    Every statement is scoped to one service, in one region, in one account, and the
    response tells you how to narrow it further to individual job/table/queue ARNs.
    The exception is the *Unscopable statement(s): IAM defines no resource type for a
    few actions (ec2:DescribeInstances and friends), so an ARN would DENY them at run
    time. Those are isolated into their own statement with Resource "*" and listed in
    `scope_down`. Do not merge them back in, and do not "fix" them with an ARN — that
    is exactly the bug this sample shipped in its own Lambda policy, which deployed
    cleanly and then denied every call.

    Pass account_id and region to get real ARNs; omit them and the policy comes back
    with ${ACCOUNT_ID}/${REGION} placeholders and a file-based apply sequence.

    region also selects the ARN partition — a policy written with arn:aws matches
    nothing in GovCloud or China, which shows up as tasks mysteriously losing
    permissions rather than as an error.

    Delete*/Terminate* actions are WITHHELD BY DEFAULT and reported in
    `destructive_actions_withheld`. A DAG that tears down what it created needs
    include_destructive_actions=True; everything else should stay unable to destroy.

    iam:PassRole is added only when a task genuinely hands a role to another service,
    and is constrained both by iam:PassedToService and by role ARN. PassRole to
    CloudFormation is withheld entirely unless you name the exact roles in
    passable_role_arns: CloudFormation acts with whatever role it is given, so an
    unscoped grant lets this role do anything any passable role in the account can do.

    Args:
        yaml_content: The DAG YAML to analyse
        account_id: Your AWS account id. Omit for a placeholder policy.
        region: Region the workflow runs in. Also selects the ARN partition.
        passable_role_arns: Exact role ARNs the DAG's tasks pass to other services
        include_destructive_actions: Grant Delete*/Terminate*. Defaults to false.
    """
    return _j(generate_execution_role_policy(yaml_content, account_id, region,
                                             passable_role_arns, include_destructive_actions))


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
    """Generate a DEMO workflow for one AWS service, to try that service out end to end.

    These templates provision their own prerequisites with CloudFormation and tear
    them down, which also makes them a poor starting point for a real pipeline — for
    that use plan_pipeline then build_dag_yaml, which produces one task per operation
    the user actually asked for.

    CHECK `params_you_must_set` BEFORE DEPLOYING. Only about half the templates are
    fully self-contained. The rest need an identifier the stack itself GENERATES (a
    !Ref'd bucket name, a !GetAtt role ARN), which cannot be known before the stack
    exists — CloudFormationCreateStackOperator returns None via XCom, so reading stack
    Outputs from it fails at run time. Those references come back as params with
    REPLACE_ME defaults. If `params_you_must_set` is non-empty and you deploy anyway,
    the DAG provisions its stack, fails the work task on the placeholder, and tears the
    stack back down.

    Self-contained today (no params required): s3, lambda, bedrock, redshift, rds, dms,
    neptune, glacier, appflow, quicksight, dynamodb, opensearch_serverless,
    emr_serverless, eventbridge, cloudformation.

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

    Extracts tasks, operators, arguments and dependency chains (>>, <<, set_upstream,
    set_downstream) via AST and emits the mapping-shaped, fully-qualified YAML the
    service accepts. The source is only ever parsed, never executed.

    CHECK `faithful` AND `dropped` BEFORE YOU DEPLOY. `valid` only means the YAML
    matches the schema; `faithful: false` means the emitted DAG is NOT equivalent to
    the Python you supplied, and every difference is itemised in `dropped` with the
    source expression, why it could not be carried over, and what to do about it.

    Things that land in `dropped` rather than disappearing: arguments whose value is a
    variable, function call, f-string or comprehension (not knowable without running
    the code); lists and dicts containing any of those; default_args keys the service
    does not honour (depends_on_past, sla, email...); dependency edges whose endpoint
    is a task built in a helper or a loop; duplicate task_ids; and a second DAG in the
    same file — one file is one workflow, so only the first is converted.

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
                        trigger_mode: str = "", start_run: bool = True,
                        expected_bucket_owner: str = "") -> str:
    """Upload the definition (and code bundle, if any), create or update the
    workflow, and start a run.

    MUTATING: if a workflow of this name already exists, its definition is UPDATED in
    place, and unless start_run=false a billable run starts immediately. Check
    mwaa_list_workflows first if you are unsure whether the name is taken.

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
        expected_bucket_owner: Your account id. Passed as ExpectedBucketOwner so the
            upload fails instead of writing to a bucket you do not own.
    """
    return _j(_deploy_and_run(workflow_name, yaml_content, s3_bucket, execution_role_arn,
                              s3_key, code_zip_base64, code_s3_key, trigger_mode, start_run,
                              expected_bucket_owner))


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

    DATA FLOW — `analyze` DEFAULTS TO TRUE. When it is on, the collected failure
    details INCLUDING CLOUDWATCH TASK LOG EXCERPTS (up to 12,000 characters) are sent
    to Amazon Bedrock. Task logs routinely contain bucket names, ARNs, account ids,
    table names and sometimes payload fragments. The default model is a cross-Region
    inference profile (us.*), so that content may be processed in a different AWS
    Region than your workflows. Pass analyze=false to keep everything local, or set
    BEDROCK_REGION / pin a non-us.* BEDROCK_MODEL_ID to keep inference in-Region. The
    log-based findings are complete and authoritative without the AI step.

    With include_hidden_failures (default), runs reported as SUCCESS are also
    inspected for tasks that actually failed — filtering on FAILED alone misses real
    breakage, because a trailing all_done task turns a failed run green.

    If some workflows could not be read, the response carries `not_scanned` and
    `incomplete_scan_warning` — an empty `failures` list with those present does NOT
    mean everything is healthy.

    Args:
        name_contains: Only scan workflows matching this substring
        hours_back: How far back to look (default 24)
        analyze: Send failure details and log excerpts to Bedrock for root-cause
            analysis. DEFAULT TRUE. The model is configurable — see get_server_config.
            The response reports analysis_model, or analysis_unavailable with the reason.
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
def mwaa_redeploy(workflow_name: str, yaml_content: str, s3_bucket: str = "", s3_key: str = "",
                  expected_bucket_owner: str = "") -> str:
    """Update an existing workflow's YAML and start a new run, reusing its existing
    S3 location and execution role.

    DESTRUCTIVE: this OVERWRITES the deployed definition and the S3 object behind it,
    then starts a billable run. workflow_name must be the EXACT name — a partial match
    is refused, because overwriting the wrong workflow's definition is unrecoverable.
    The new YAML is validated locally first and the workflow is left untouched if it
    fails.

    Args:
        workflow_name: Existing workflow name (EXACT)
        yaml_content: New DAG YAML
        s3_bucket: Optional bucket override
        s3_key: Optional key override
        expected_bucket_owner: Your account id, to assert bucket ownership on write
    """
    return _j(_redeploy_workflow(workflow_name, yaml_content, s3_bucket, s3_key,
                                 expected_bucket_owner))


@mcp_server.tool()
def mwaa_compare_versions(workflow_name: str) -> str:
    """Diff the latest two versions of a workflow's YAML.

    Args:
        workflow_name: Workflow name (exact or partial)
    """
    return _j(_compare_versions(workflow_name))


@mcp_server.tool()
def mwaa_delete_workflows(name_contains: str = "", not_run_in_days: int = 0,
                          dry_run: bool = True, confirm_delete_all: bool = False) -> str:
    """Delete workflows by name pattern or inactivity. Previews by default.

    READ THIS BEFORE CALLING. With NO name_contains and NO not_run_in_days, this
    targets EVERY workflow in the account and Region — including workflows this
    server never created. Always pass a filter.

    Deletion is irreversible and removes all versions. An unfiltered call with
    dry_run=false is REFUSED and returns the list it would have deleted; deleting
    everything requires confirm_delete_all=true, which you should only pass after the
    user has seen that list and asked for it explicitly.

    Show the dry-run result and get explicit confirmation before calling again with
    dry_run=false.

    Args:
        name_contains: Delete workflows whose name contains this substring
        not_run_in_days: Delete workflows not run in this many days (0 = ignore)
        dry_run: True (default) previews. False actually deletes.
        confirm_delete_all: Required to delete with no filter at all
    """
    return _j(_delete_workflows(name_contains, not_run_in_days, dry_run, confirm_delete_all))


def handler(event, context):
    # Hand the Lambda context to operations so the poll loops can derive their deadline
    # from the time actually remaining, rather than from a constant chosen to sit under
    # whatever Timeout happens to be in template.yaml.
    _operations.set_lambda_context(context)
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
