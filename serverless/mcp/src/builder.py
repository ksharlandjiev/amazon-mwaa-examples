# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Correct-by-construction DAG assembly and pipeline planning.

`build_dag_yaml` takes a structured task list and emits YAML in the exact shape
MWAA Serverless accepts. Because the caller never writes YAML by hand, the six
schema mistakes that dominate real failures cannot reach the output: list-shaped
tasks, short operator names, a `parameters:` wrapper, `upstream_tasks` (and the
other dependency spellings, which are normalised to `dependencies`), duration
strings, and a bare-int `execution_timeout`.

Nothing is changed silently. Every value the builder coerces, caps or renames on
the caller's behalf is listed in `values_adjusted`, and anything it cannot make
sense of goes in `build_problems` rather than being replaced with a default.

`plan_pipeline` goes one step earlier: given the operations a user asked for, it
returns the operator to use, the arguments that must be supplied, the questions
to ask, and the best practices that were deliberately NOT added. That last part
is the point — the pipeline stays minimal and the user decides what else to add.
"""

import copy

import yaml

from constraints import AUTHORING_POLICY, QUOTAS
from schema import (
    LONG_WAIT_OPERATOR_PAIRS,
    OPERATOR_REQUIRED_PARAMS,
    OPERATOR_XCOM_RETURNS,
    RESCHEDULE_MODE_SUPPORTED,
    RESCHEDULE_MODE_UNSUPPORTED,
    SENSOR_SAFETY_DEFAULTS,
    SUPPORTED_OPERATORS,
    is_sensor,
    resolve_operator_fqn,
)
# Duration parsing and nested-string traversal are shared with the validator rather
# than reimplemented here. The previous local handling ("any string retry_delay
# becomes 300") silently discarded the value the caller asked for.
from validator import _duration_to_seconds, _iter_strings, validate

# Dependency spellings that must never reach the operator constructor. These are
# normalised to `dependencies`; letting one through is what broke this module's own
# correct-by-construction guarantee, since `upstream_tasks` was not in _RESERVED_SPEC_KEYS
# and the spec-passthrough loop copied it verbatim into the emitted task.
_DEP_ALIASES = ("upstream_tasks", "depends_on", "upstream", "depends", "needs")
_REVERSE_DEP_ALIASES = ("downstream_tasks", "downstream")

# Spec keys this builder consumes itself instead of forwarding to the operator.
_RESERVED_SPEC_KEYS = {
    "task_id", "operator", "params", "parameters", "arguments", "dependencies",
    "retries", "retry_delay_seconds", "retry_delay",
    "execution_timeout_minutes", "execution_timeout", "trigger_rule",
    "sensor_timeout_seconds",
    *_DEP_ALIASES,
    *_REVERSE_DEP_ALIASES,
}


# ══════════════════════════════════════════════════════════════════════════
#  PIPELINE STEP CATALOG
# ══════════════════════════════════════════════════════════════════════════
# One entry per operation people actually ask for. `ask` lists the values that
# have no safe default and MUST come from the user. `chains_via` describes how
# the step hands a value to the next one.

STEP_CATALOG = {
    "glue_job": {
        "what": "Run an existing AWS Glue ETL job.",
        "operator": "GlueJobOperator",
        "required": {"job_name": "Name of the existing Glue job"},
        "recommended": {
            "wait_for_completion": "true to block until the job finishes (simplest); "
                                   "false if you want a separate GlueJobSensor",
            "script_args": "dict of --key value job arguments, e.g. {'--input_path': '...'}",
        },
        "ask": ["The Glue job name", "Does the job already exist, or should the DAG create it?"],
        "chains_via": "Returns the Glue job RUN ID via XCom. Only needed if wait_for_completion is false.",
        "pairs_with": "glue_job_sensor",
        "iam": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJob", "glue:BatchStopJobRun"],
    },
    "glue_job_sensor": {
        "what": "Wait for a Glue job run started by an earlier task.",
        "operator": "GlueJobSensor",
        "required": {
            "job_name": "Same job name as the GlueJobOperator task",
            "run_id": "{{ ti.xcom_pull(task_ids='<glue_job_task_id>') }}",
        },
        "ask": [],
        "chains_via": "Consumes the run ID from the GlueJobOperator task's XCom.",
        "note": "Only add this if the GlueJobOperator used wait_for_completion: false. "
                "Otherwise it duplicates work.",
        "iam": ["glue:GetJobRun"],
    },
    "glue_crawler": {
        "what": "Run an existing Glue crawler so the Data Catalog picks up new data.",
        "operator": "GlueCrawlerOperator",
        "required": {"config": "{'Name': '<crawler-name>'} — the crawler config mapping"},
        "recommended": {"wait_for_completion": "true to block until the crawl finishes"},
        "ask": ["The crawler name", "Does the crawler already exist?"],
        "chains_via": "Returns the crawler name via XCom.",
        "pairs_with": "glue_crawler_sensor",
        "iam": ["glue:StartCrawler", "glue:GetCrawler", "glue:GetCrawlerMetrics"],
    },
    "glue_crawler_sensor": {
        "what": "Wait for a Glue crawler to reach READY.",
        "operator": "GlueCrawlerSensor",
        "required": {"crawler_name": "The crawler name"},
        "ask": [],
        "note": "Only needed if GlueCrawlerOperator used wait_for_completion: false.",
        "iam": ["glue:GetCrawler"],
    },
    "athena_query": {
        "what": "Run a SQL query on Amazon Athena.",
        "operator": "AthenaOperator",
        "required": {
            "query": "The SQL statement",
            "database": "Glue/Athena database name",
            "output_location": "s3://bucket/prefix/ for query results",
        },
        "recommended": {
            "workgroup": "Athena workgroup (defaults to 'primary')",
            "sleep_time": "Poll interval in seconds while waiting",
        },
        "ask": ["The database name", "The Athena results S3 location", "The SQL to run"],
        "chains_via": "Returns the query execution ID via XCom. AthenaOperator already waits for "
                      "completion, so an AthenaSensor is usually unnecessary.",
        "iam": ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
                "athena:StopQueryExecution", "glue:GetTable", "glue:GetDatabase", "glue:GetPartitions",
                "s3:GetObject", "s3:PutObject", "s3:ListBucket"],
    },
    "athena_sensor": {
        "what": "Wait for an Athena query execution to finish.",
        "operator": "AthenaSensor",
        "required": {"query_execution_id": "{{ ti.xcom_pull(task_ids='<athena_task_id>') }}"},
        "ask": [],
        "note": "AthenaOperator already blocks until the query completes. Add this only if you "
                "deliberately want the wait as a separate task.",
        "iam": ["athena:GetQueryExecution"],
    },
    "s3_wait_for_file": {
        "what": "Block until an object appears in S3.",
        "operator": "S3KeySensor",
        "required": {"bucket_key": "Object key, or a full s3://bucket/key URI",
                     "bucket_name": "Bucket name (omit if bucket_key is a full s3:// URI)"},
        "recommended": {"wildcard_match": "true to treat bucket_key as a glob",
                        "poke_interval": "Seconds between checks"},
        "ask": ["The bucket and key (or prefix) to wait for"],
        "iam": ["s3:ListBucket", "s3:GetObject"],
    },
    "s3_list": {
        "what": "List object keys under a prefix.",
        "operator": "S3ListOperator",
        "required": {"bucket": "Bucket name"},
        "recommended": {"prefix": "Key prefix to filter on"},
        "ask": ["The bucket and prefix"],
        "chains_via": "Returns list[str] of keys via XCom.",
        "iam": ["s3:ListBucket"],
    },
    "s3_copy": {
        "what": "Copy one object within or between buckets.",
        "operator": "S3CopyObjectOperator",
        "required": {"source_bucket_key": "Source key", "dest_bucket_key": "Destination key"},
        "recommended": {"source_bucket_name": "Source bucket", "dest_bucket_name": "Destination bucket"},
        "ask": ["Source and destination bucket/key"],
        "iam": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
    },
    "s3_delete_objects": {
        "what": "Delete objects from a bucket.",
        "operator": "S3DeleteObjectsOperator",
        "required": {"bucket": "Bucket name"},
        "recommended": {"keys": "list of keys", "prefix": "or a prefix to delete under"},
        "ask": ["The bucket, and the keys or prefix to delete"],
        "iam": ["s3:DeleteObject", "s3:ListBucket"],
    },
    "lambda_invoke": {
        "what": "Invoke a Lambda function.",
        "operator": "LambdaInvokeFunctionOperator",
        "required": {"function_name": "Function name or ARN"},
        "recommended": {"payload": "JSON STRING of the event payload (not a dict)"},
        "ask": ["The function name", "The payload, if any"],
        "chains_via": "Returns the response payload as a string via XCom.",
        "iam": ["lambda:InvokeFunction", "lambda:GetFunction"],
    },
    "step_functions_start": {
        "what": "Start a Step Functions state machine execution.",
        "operator": "StepFunctionStartExecutionOperator",
        "required": {"state_machine_arn": "State machine ARN"},
        "recommended": {"state_machine_input": "dict of execution input",
                        "waiter_delay": "set with wait_for_completion to block"},
        "ask": ["The state machine ARN"],
        "chains_via": "Returns the execution ARN via XCom; feed it to StepFunctionExecutionSensor.",
        "pairs_with": "step_functions_sensor",
        "iam": ["states:StartExecution", "states:DescribeExecution", "states:DescribeStateMachine"],
    },
    "step_functions_sensor": {
        "what": "Wait for a Step Functions execution to finish.",
        "operator": "StepFunctionExecutionSensor",
        "required": {"execution_arn": "{{ ti.xcom_pull(task_ids='<start_task_id>') }}"},
        "ask": [],
        "iam": ["states:DescribeExecution"],
    },
    "redshift_sql": {
        "what": "Run SQL against Redshift (provisioned or Serverless) via the Data API.",
        "operator": "RedshiftDataOperator",
        "required": {"sql": "The SQL statement or a list of statements",
                     "database": "Database name"},
        "recommended": {"cluster_identifier": "for a provisioned cluster",
                        "workgroup_name": "for Redshift Serverless",
                        "wait_for_completion": "true to block",
                        "db_user": "database user for provisioned clusters"},
        "ask": ["Redshift cluster identifier or Serverless workgroup name", "The database name",
                "The SQL to run"],
        "chains_via": "Returns the statement ID, or rows when return_sql_result is true.",
        "iam": ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement",
                "redshift-data:DescribeStatement", "redshift-data:GetStatementResult",
                "redshift:GetClusterCredentials", "redshift-serverless:GetCredentials"],
    },
    "emr_serverless_job": {
        "what": "Submit a Spark or Hive job to an EMR Serverless application.",
        "operator": "EmrServerlessStartJobOperator",
        "required": {
            "application_id": "EMR Serverless application ID",
            "execution_role_arn": "Role EMR Serverless assumes for the job (NOT the workflow role)",
            "job_driver": "{'sparkSubmit': {'entryPoint': 's3://.../job.py'}}",
        },
        "recommended": {"configuration_overrides": "logging and Spark configuration"},
        "ask": ["The EMR Serverless application ID (or should the DAG create one?)",
                "The job execution role ARN", "The S3 location of the job script"],
        "chains_via": "Returns the job run ID via XCom.",
        "iam": ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun",
                "emr-serverless:CancelJobRun", "iam:PassRole"],
    },
    "batch_job": {
        "what": "Submit an AWS Batch job.",
        "operator": "BatchOperator",
        "required": {"job_name": "Name for this job submission",
                     "job_definition": "Batch job definition name or ARN",
                     "job_queue": "Batch job queue name or ARN"},
        "recommended": {"overrides": "container overrides, e.g. {'command': [...]}"},
        "ask": ["The job definition and job queue"],
        "chains_via": "Returns the Batch job ID via XCom.",
        "iam": ["batch:SubmitJob", "batch:DescribeJobs", "batch:TerminateJob"],
    },
    "ecs_run_task": {
        "what": "Run a container task on ECS/Fargate.",
        "operator": "EcsRunTaskOperator",
        "required": {"task_definition": "Task definition family or ARN", "cluster": "ECS cluster name"},
        "recommended": {"launch_type": "FARGATE", "overrides": "container overrides",
                        "network_configuration": "required for FARGATE — subnets and security groups"},
        "ask": ["The ECS cluster and task definition", "Subnets and security groups for Fargate"],
        "iam": ["ecs:RunTask", "ecs:DescribeTasks", "iam:PassRole"],
    },
    "sns_notify": {
        "what": "Publish a message to an SNS topic.",
        "operator": "SnsPublishOperator",
        "required": {"target_arn": "SNS topic ARN", "message": "Message body"},
        "recommended": {"subject": "Message subject",
                        "trigger_rule": "one_failed for a failure alert; all_done to always send"},
        "ask": ["The SNS topic ARN"],
        "iam": ["sns:Publish"],
    },
    "sqs_send": {
        "what": "Send a message to an SQS queue.",
        "operator": "SqsPublishOperator",
        "required": {"sqs_queue": "Queue URL", "message_content": "Message body"},
        "ask": ["The queue URL"],
        "iam": ["sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
    },
    "sqs_receive": {
        "what": "Receive and delete messages from an SQS queue.",
        "operator": "SqsSensor",
        "required": {"sqs_queue": "Queue URL"},
        "ask": ["The queue URL"],
        "chains_via": "Returns the received messages via XCom.",
        "iam": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
    },
    "bedrock_invoke": {
        "what": "Invoke a Bedrock foundation model.",
        "operator": "BedrockInvokeModelOperator",
        "required": {"model_id": "e.g. amazon.nova-lite-v1:0",
                     "input_data": "Model-specific request body mapping"},
        "ask": ["The model ID", "The prompt or input payload"],
        "chains_via": "Returns the model response body via XCom.",
        "iam": ["bedrock:InvokeModel"],
    },
    "data_quality_check": {
        "what": "Evaluate a Glue Data Quality ruleset against a catalog table.",
        "operator": "GlueDataQualityOperator",
        "required": {"name": "Ruleset name", "ruleset": "DQDL ruleset string"},
        "ask": ["The table to check and the rules to apply"],
        "iam": ["glue:CreateDataQualityRuleset", "glue:GetDataQualityRuleset",
                "glue:StartDataQualityRulesetEvaluationRun", "glue:GetDataQualityRulesetEvaluationRun"],
    },
    "glue_catalog_create_database": {
        "what": "Create a database in the Glue Data Catalog.",
        "operator": "GlueCatalogCreateDatabaseOperator",
        "required": {"database_input": "{'Name': '<db-name>'}"},
        "ask": ["The database name"],
        "iam": ["glue:CreateDatabase", "glue:GetDatabase"],
    },
    "python_code": {
        "what": "Run your own Python function (glue logic, reshaping an XCom value, branching).",
        "operator": "PythonOperator",
        "required": {"python_callable": "module_name.function_name — module is a .py at the "
                                        "root of the uploaded code bundle"},
        "recommended": {"op_kwargs": "mapping of extra keyword arguments"},
        "ask": ["What the function should do", "Whether the code bundle already exists in S3"],
        "chains_via": "Returns whatever the callable returns (JSON-serialisable, under 100 KB).",
        "note": "Requires a code bundle uploaded via the CreateWorkflow `Code` parameter. "
                "Tasks have NO internet access unless you attach a VPC. Prefer a native AWS "
                "operator when one exists.",
        "iam": [],
    },
    "bash_command": {
        "what": "Run a shell command or a script from the code bundle.",
        "operator": "BashOperator",
        "required": {"bash_command": "The command, or './script.sh' for a bundled script"},
        "ask": ["The command to run"],
        "chains_via": "Returns the last line of stdout via XCom.",
        "note": "Working directory is /usr/local/airflow/dags. No internet access by default.",
        "iam": [],
    },
    "join": {
        "what": "A no-op join or fan-in point.",
        "operator": "EmptyOperator",
        "required": {},
        "ask": [],
        "note": "Costs a task slot. Only add one if you genuinely need a single fan-in node.",
        "iam": [],
    },
}


# ── Cost-control guidance, applied uniformly to every sensor in the catalog ──
# MWAA Serverless bills for worker occupancy, so how a sensor waits is a pricing
# decision, not a style one. Attaching this here (rather than repeating it in each
# entry) keeps it consistent as the catalog grows.
_SENSOR_COST_RECOMMENDED = {
    "mode": "reschedule — releases the worker slot between checks, so the wait is not billed as "
            "occupied worker time. Strongly preferred for any wait beyond a couple of minutes.",
    "poke_interval": "Seconds between checks. Each reschedule cycle is a task start, so 30-120 "
                     "is the useful range.",
    "timeout": "Seconds before giving up. Airflow defaults to 7 days and the wait is billed, so "
               "always bound this.",
}
_SENSOR_COST_NOTE = (
    "COST: set mode: reschedule so the worker is released between checks, and always bound the "
    "wait with a timeout. Do NOT use deferrable: true — MWAA Serverless has no triggerer and "
    "ignores it (CreateWorkflow returns 'ignored attributes: deferrable')."
)

def _annotate_catalog_with_cost_guidance(catalog):
    """Attach the cost note each step needs. Called once at import."""
    for step in catalog.values():
        operator = step.get("operator")
        if is_sensor(operator):
            rec = dict(_SENSOR_COST_RECOMMENDED)
            rec.update(step.get("recommended") or {})
            step["recommended"] = rec
            step["cost"] = _SENSOR_COST_NOTE
        elif operator in LONG_WAIT_OPERATOR_PAIRS:
            step["cost"] = (
                f"COST: wait_for_completion: true holds a worker slot for the whole job. For a job "
                f"that runs more than a few minutes, prefer wait_for_completion: false plus a "
                f"{LONG_WAIT_OPERATOR_PAIRS[operator]} with mode: reschedule, which releases "
                f"the worker between checks. For a short job, blocking is simpler."
            )


_annotate_catalog_with_cost_guidance(STEP_CATALOG)


def list_pipeline_steps(search: str = "") -> dict:
    """Catalog of pipeline steps, optionally filtered."""
    s = (search or "").lower()
    out = {}
    for key, spec in STEP_CATALOG.items():
        if s and s not in key and s not in spec["what"].lower() and s not in spec["operator"].lower():
            continue
        fqn, _, _ = resolve_operator_fqn(spec["operator"])
        # deepcopy, not {**spec}: a shallow copy shares the nested `required` and
        # `recommended` dicts with STEP_CATALOG, so a caller mutating the response
        # corrupted the catalog for the lifetime of the process.
        out[key] = {**copy.deepcopy(spec), "operator_fqn": fqn}
    return {"steps": out, "count": len(out)}


def plan_pipeline(steps, has_schedule: bool = False, creates_resources: bool = False) -> dict:
    """Turn a list of requested operations into a build plan.

    Returns the operator and arguments for each step, the questions that must be
    answered before the DAG can work, and the best practices that were NOT added
    so they can be offered to the user rather than silently included.
    """
    if isinstance(steps, str):
        steps = [s.strip() for s in steps.split(",") if s.strip()]
    if not isinstance(steps, list):
        return {"error": "steps must be a list of step keys or a comma-separated string. "
                         "Call list_pipeline_steps to see the available keys."}

    planned, unknown, questions = [], [], []
    iam_actions, chaining_notes = set(), []
    needs_code_bundle = False
    sensor_steps, blocking_candidates = [], []

    for raw in steps:
        key = str(raw).strip().lower().replace("-", "_").replace(" ", "_")
        spec = STEP_CATALOG.get(key)
        if not spec:
            matches = [k for k in STEP_CATALOG if key in k or k in key]
            unknown.append({"requested": raw, "did_you_mean": matches[:4]})
            continue
        fqn, short, _ = resolve_operator_fqn(spec["operator"])
        if short in ("PythonOperator", "BashOperator"):
            needs_code_bundle = True
        planned.append({
            "step": key,
            "what": spec["what"],
            "operator": short,
            "operator_fqn": fqn,
            "required_arguments": copy.deepcopy(spec.get("required", {})),
            "worth_setting": copy.deepcopy(spec.get("recommended", {})),
            "xcom_output": spec.get("chains_via") or OPERATOR_XCOM_RETURNS.get(short, "No XCom output."),
            "note": spec.get("note"),
            "cost": spec.get("cost"),
            "optional_companion_step": spec.get("pairs_with"),
        })
        for q in spec.get("ask", []):
            if q not in questions:
                questions.append(q)
        iam_actions.update(spec.get("iam", []))
        if spec.get("chains_via") and "xcom" in spec["chains_via"].lower():
            chaining_notes.append(f"{short}: {spec['chains_via']}")
        if is_sensor(spec["operator"]):
            sensor_steps.append(key)
        if spec["operator"] in LONG_WAIT_OPERATOR_PAIRS:
            blocking_candidates.append((key, short, LONG_WAIT_OPERATOR_PAIRS[spec["operator"]]))

    if not has_schedule:
        questions.append(
            "Should this run on a schedule? Without one the workflow is on-demand only."
        )

    not_added = [
        c for c in AUTHORING_POLICY["gentle_prompting"]["candidates_to_offer"]
        if not (c.startswith("A schedule") and has_schedule)
    ]
    if not creates_resources:
        not_added.append(
            "Resource provisioning and teardown — this plan assumes the Glue jobs, crawlers, "
            "databases and buckets already exist and are referenced by name."
        )

    cost_plan = {
        "billing_model": AUTHORING_POLICY["cost_efficiency"]["principle"],
        "will_be_applied_automatically": [],
        "decisions_that_need_the_user": [],
        "do_not_use": [AUTHORING_POLICY["cost_efficiency"]["deferrable_does_not_work"]]
                      + ([] if RESCHEDULE_MODE_SUPPORTED else [RESCHEDULE_MODE_UNSUPPORTED]),
    }
    if sensor_steps:
        cost_plan["will_be_applied_automatically"].append(
            f"mode: reschedule, a poke_interval and a bounded timeout on {len(sensor_steps)} "
            f"sensor step(s) ({', '.join(sensor_steps)}), so the wait releases its worker between "
            f"checks instead of holding one throughout. build_dag_yaml does this for you."
        )
    for key, short, sensor in blocking_candidates:
        cost_plan["decisions_that_need_the_user"].append(
            f"'{key}' ({short}): how long does this normally take? Under ~5 minutes, keep "
            f"wait_for_completion: true — one task, simplest. Longer than that, use "
            f"wait_for_completion: false plus a {sensor} in reschedule mode, so the worker is not "
            f"held for the whole job."
        )
    if blocking_candidates:
        names = ", ".join(k for k, _, _ in blocking_candidates)
        questions.append(
            f"Roughly how long do these steps run for: {names}? Anything beyond ~5 minutes is "
            f"cheaper to wait for in a reschedule-mode sensor than to block on in-task."
        )

    return {
        "planned_tasks": planned,
        "unknown_steps": unknown,
        "questions_to_ask_the_user": questions,
        "deliberately_not_added": not_added,
        "how_to_offer": AUTHORING_POLICY["gentle_prompting"]["how_to_phrase"],
        "chaining": chaining_notes,
        "cost_plan": cost_plan,
        "needs_code_bundle": needs_code_bundle,
        "iam_actions_needed": sorted(iam_actions),
        "next_step": (
            "Ask the user the questions above. Then call build_dag_yaml with one entry per "
            "planned task — it emits YAML in the exact shape the service accepts, applies the "
            "sensor cost defaults, and validates the result."
        ),
        "scope_reminder": AUTHORING_POLICY["scope_discipline"]["principle"],
    }


# ══════════════════════════════════════════════════════════════════════════
#  BUILD
# ══════════════════════════════════════════════════════════════════════════

def build_dag_yaml(
    dag_id: str,
    tasks,
    schedule=None,
    description: str = "",
    params=None,
    default_args=None,
    max_active_runs=None,
    start_date: str = "",
) -> dict:
    """Assemble validated MWAA Serverless DAG YAML from a structured task list.

    Each entry in `tasks` is a mapping:
        task_id                    (required) unique id
        operator                   (required) short name or FQN; emitted as FQN
        params                     (optional) operator arguments, emitted FLAT
        dependencies               (optional) list of upstream task_ids
        retries                    (optional) int 0-3
        retry_delay_seconds        (optional) int 0-300
        execution_timeout_minutes  (optional) int 1-60, emitted as a timedelta mapping
        trigger_rule               (optional) e.g. all_done for cleanup tasks
        sensor_timeout_seconds     (optional) sensors only; bounds the wait (default 3600)

    Sensors are emitted with `mode: reschedule`, a `poke_interval` and a bounded
    `timeout` unless the caller sets them, because a poke-mode sensor occupies a worker
    for its whole wait and Airflow's default timeout is 7 days. Pass
    `params: {"mode": "poke"}` to opt out for a short wait.
    """
    if isinstance(tasks, dict):
        tasks = [{"task_id": k, **(v or {})} for k, v in tasks.items()]
    if not isinstance(tasks, list) or not tasks:
        return {"error": "tasks must be a non-empty list of task specifications."}
    if not dag_id or not isinstance(dag_id, str):
        return {"error": "dag_id is required."}

    body = {}
    if description:
        body["description"] = description
    body["schedule"] = schedule if schedule not in ("", "None", "none") else None
    if start_date:
        body["start_date"] = start_date
    if max_active_runs is not None:
        body["max_active_runs"] = max_active_runs

    built, problems = {}, []
    cost_applied = []
    adjustments = []

    if default_args:
        da = copy.deepcopy(default_args)
        # default_args durations get the same treatment as task-level ones: parsed and
        # reported, never silently replaced. The previous code turned ANY string
        # retry_delay into 300, so a caller asking for "60s" got the 5-minute maximum
        # with nothing in the response saying so.
        for key in ("retry_delay", "execution_timeout"):
            raw = da.get(key)
            if raw is None or isinstance(raw, dict):
                continue
            secs = _duration_to_seconds(raw)
            if secs is None:
                problems.append(
                    f"default_args.{key} must be a number of seconds or a duration like "
                    f"'5m' (got {raw!r})."
                )
                continue
            cap = (QUOTAS["max_retry_delay_seconds"] if key == "retry_delay"
                   else QUOTAS["max_task_execution_timeout_minutes"] * 60)
            capped = max(0, min(secs, cap))
            da[key] = capped
            if capped != secs or not isinstance(raw, int):
                adjustments.append(
                    f"default_args.{key} {raw!r} was set to {capped} seconds"
                    + (f" (capped at the {cap}s maximum)." if capped != secs else ".")
                )
        body["default_args"] = da

    if params:
        body["params"] = copy.deepcopy(params)

    for i, spec in enumerate(tasks):
        if not isinstance(spec, dict):
            problems.append(f"tasks[{i}] is not a mapping.")
            continue
        tid = spec.get("task_id")
        if not tid:
            problems.append(f"tasks[{i}] has no task_id.")
            continue
        if tid in built:
            problems.append(f"Duplicate task_id '{tid}'.")
            continue

        op = spec.get("operator")
        fqn, short, _ = resolve_operator_fqn(op) if op else (None, None, False)
        if not fqn:
            problems.append(
                f"Task '{tid}': operator '{op}' is not in the MWAA Serverless allowlist. "
                f"Call list_supported_operators to find the right one."
            )
            continue

        tcfg = {"operator": fqn}

        # Operator arguments, flat. Accept several spellings of the wrapper key so
        # a caller that reaches for `parameters` still gets valid output.
        op_params = spec.get("params") or spec.get("parameters") or spec.get("arguments") or {}
        if not isinstance(op_params, dict):
            problems.append(f"Task '{tid}': params must be a mapping.")
            op_params = {}
        for k, v in op_params.items():
            if k in ("operator", "dependencies", "task_id"):
                continue
            tcfg[k] = v

        # Also accept operator arguments given directly on the spec.
        for k, v in spec.items():
            if k not in _RESERVED_SPEC_KEYS and k not in tcfg:
                tcfg[k] = v

        if spec.get("retries") is not None:
            tcfg["retries"] = spec["retries"]

        rd = spec.get("retry_delay_seconds", spec.get("retry_delay"))
        if rd is not None:
            secs = _duration_to_seconds(rd)
            if secs is None:
                problems.append(
                    f"Task '{tid}': retry_delay_seconds must be an integer number of seconds "
                    f"or a duration like '90s'/'5m' (got {rd!r})."
                )
            elif secs < 0:
                problems.append(f"Task '{tid}': retry_delay_seconds cannot be negative (got {secs}).")
            else:
                capped = min(secs, QUOTAS["max_retry_delay_seconds"])
                tcfg["retry_delay"] = capped
                if capped != secs:
                    # Capping used to happen silently, so a caller asking for 9999
                    # got 300 with nothing in the report.
                    adjustments.append(
                        f"Task '{tid}': retry_delay {secs}s exceeds the "
                        f"{QUOTAS['max_retry_delay_seconds']}s maximum and was capped to {capped}s."
                    )
                elif not isinstance(rd, int):
                    adjustments.append(
                        f"Task '{tid}': retry_delay {rd!r} was parsed as {capped} seconds."
                    )

        etm = spec.get("execution_timeout_minutes")
        if etm is not None:
            try:
                requested = int(etm)
            except (TypeError, ValueError):
                problems.append(f"Task '{tid}': execution_timeout_minutes must be an integer.")
            else:
                if requested < 1:
                    problems.append(
                        f"Task '{tid}': execution_timeout_minutes must be at least 1 (got {requested})."
                    )
                else:
                    minutes = min(requested, QUOTAS["max_task_execution_timeout_minutes"])
                    tcfg["execution_timeout"] = {"__type__": "datetime.timedelta",
                                                 "minutes": minutes}
                    if minutes != requested:
                        adjustments.append(
                            f"Task '{tid}': execution_timeout {requested}m exceeds the "
                            f"{QUOTAS['max_task_execution_timeout_minutes']}m maximum and was "
                            f"capped to {minutes}m."
                        )

        if spec.get("trigger_rule"):
            tcfg["trigger_rule"] = spec["trigger_rule"]

        # Dependencies, accepting the aliases people reach for. Normalising here is
        # what keeps `upstream_tasks` out of the emitted YAML.
        deps, dep_source = None, None
        for key in ("dependencies", *_DEP_ALIASES):
            if spec.get(key) is not None:
                deps, dep_source = spec[key], key
                break
        if deps is not None:
            if isinstance(deps, str):
                deps = [deps]
            if not isinstance(deps, list):
                # A dict used to be silently coerced to its keys.
                problems.append(
                    f"Task '{tid}': dependencies must be a list of task_ids or a single "
                    f"task_id string, not {type(deps).__name__}."
                )
            elif not all(isinstance(d, str) for d in deps):
                problems.append(f"Task '{tid}': every entry in dependencies must be a task_id string.")
            else:
                tcfg["dependencies"] = list(deps)
                if dep_source != "dependencies":
                    adjustments.append(
                        f"Task '{tid}': '{dep_source}' was renamed to 'dependencies' — "
                        f"MWAA Serverless forwards any other spelling to the operator, "
                        f"which fails."
                    )
        for key in _REVERSE_DEP_ALIASES:
            if spec.get(key):
                problems.append(
                    f"Task '{tid}': '{key}' points the wrong way and has no YAML equivalent. "
                    f"Declare the edge on the DOWNSTREAM task with 'dependencies: [{tid}]'."
                )

        # Sensors get reschedule mode, a poke interval and a bounded wait, so a wait
        # never silently holds a worker for its full duration. An explicit value always
        # wins, so `params: {mode: poke}` opts out for a short wait.
        if is_sensor(fqn):
            sensor_timeout = None
            raw_timeout = spec.get("sensor_timeout_seconds")
            if raw_timeout is not None:
                sensor_timeout = _duration_to_seconds(raw_timeout)
                if sensor_timeout is None or sensor_timeout <= 0:
                    problems.append(
                        f"Task '{tid}': sensor_timeout_seconds must be a positive number of "
                        f"seconds or a duration like '1h' (got {raw_timeout!r})."
                    )
            for k, v in SENSOR_SAFETY_DEFAULTS.items():
                if k not in tcfg:
                    tcfg[k] = sensor_timeout if (k == "timeout" and sensor_timeout) else v
                    cost_applied.append(
                        f"Task '{tid}': set {k}: {tcfg[k]} — {_SENSOR_DEFAULT_REASONS.get(k, '')}"
                    )

        built[str(tid)] = tcfg

    body["tasks"] = built
    dag_yaml = yaml.dump({dag_id: body}, default_flow_style=False, sort_keys=False,
                         width=4096, allow_unicode=True)

    result = validate(dag_yaml)
    missing = _collect_placeholder_warnings(built)

    out = {
        "dag_yaml": dag_yaml,
        "valid": result["valid"] and not problems,
        "build_problems": problems,
        "errors": result["errors"],
        "warnings": result["warnings"],
        "hints": result["hints"],
        "summary": result["summary"],
        "placeholders_to_replace": missing,
        "cost_optimizations_applied": cost_applied,
        "next_step": (
            "Fix any errors, then call preflight_dag_yaml to have the service itself validate it, "
            "then generate_execution_role and mwaa_deploy_and_run."
            if result["valid"] and not problems else
            "Fix the errors above and rebuild."
        ),
    }
    if adjustments:
        # Every value the builder changed on the caller's behalf. Capping and
        # renaming used to happen silently, so a caller could ask for a 9999s retry
        # delay, receive 300, and have nothing in the response say so.
        out["values_adjusted"] = adjustments
    return out


_PLACEHOLDER_HINTS = ("REPLACE", "CHANGEME", "your-", "my-bucket", "example", "amzn-s3-demo",
                      "111122223333", "<", "TODO", "xxx")

# Why each sensor default is applied, so the report explains itself rather than
# repeating one message that only fits `timeout`.
_SENSOR_DEFAULT_REASONS = {
    "mode": "releases the worker slot between checks instead of holding one for the whole "
            "wait, which is what MWAA Serverless bills for. Pass mode: poke to opt out.",
    "poke_interval": "seconds between checks; each reschedule cycle is a task start, so "
                     "30-120s is the useful range.",
    "timeout": "bounds the wait, so a stuck upstream job cannot run up an unbounded bill "
               "(Airflow's default is 7 days).",
}


def _collect_placeholder_warnings(tasks) -> list:
    """Flag obviously-placeholder values so they are never mistaken for real ones.

    Walks NESTED values via the validator's traversal helper. Scanning only top-level
    strings missed placeholders exactly where they live in practice — inside
    script_args, job_driver, overrides and configuration_overrides.
    """
    found = []
    for tid, tcfg in tasks.items():
        for path, value in _iter_strings(tcfg):
            if any(h.lower() in value.lower() for h in _PLACEHOLDER_HINTS):
                found.append(
                    f"Task '{tid}'.{path} = {value!r} looks like a placeholder — "
                    f"confirm the real value."
                )
    return found


def suggest_operator(intent: str) -> dict:
    """Map a free-text description onto candidate operators."""
    q = (intent or "").lower()
    if not q:
        return {"error": "Provide a description of what the task should do."}

    step_hits = [
        {"step": k, "what": v["what"], "operator": v["operator"]}
        for k, v in STEP_CATALOG.items()
        if any(tok in k or tok in v["what"].lower() for tok in q.split() if len(tok) > 2)
    ]

    op_hits = []
    for short, fqn in SUPPORTED_OPERATORS.items():
        if any(tok in short.lower() for tok in q.split() if len(tok) > 3):
            op_hits.append({
                "operator": short,
                "operator_fqn": fqn,
                "required": OPERATOR_REQUIRED_PARAMS.get(short, []),
                "xcom": OPERATOR_XCOM_RETURNS.get(short),
            })

    return {
        "intent": intent,
        "catalog_matches": step_hits[:8],
        "operator_matches": op_hits[:12],
        "note": "If nothing fits, there may be no AWS operator for this. Consider PythonOperator "
                "with a code bundle — but check list_supported_operators first.",
    }
