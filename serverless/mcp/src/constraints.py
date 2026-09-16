"""
Amazon MWAA Serverless ground truth: YAML schema, Jinja support, DAG/task
parameters, quotas, and DAG authoring policy.

Sources:
  - https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/
  - Behaviour marked "verified" was confirmed empirically against the live
    mwaa-serverless API (CreateWorkflow validation + real workflow runs).

Where the documentation and the service disagree, the service wins and the
discrepancy is called out explicitly.
"""

# ══════════════════════════════════════════════════════════════════════════
#  THE YAML SCHEMA  (verified against the live service)
# ══════════════════════════════════════════════════════════════════════════
# This is the single most important section. Getting any of these wrong
# produces a workflow that either fails CreateWorkflow validation or deploys
# and then fails every task at run time.

YAML_SCHEMA = {
    "summary": (
        "MWAA Serverless runs dag-factory 1.0.0. A definition file contains "
        "EXACTLY ONE DAG. The root key is the dag_id. Tasks are a MAPPING keyed "
        "by task_id. Operators must be FULLY QUALIFIED class paths. Operator "
        "arguments are FLAT keys on the task — there is no 'parameters' wrapper. "
        "Dependencies use the 'dependencies' key."
    ),
    "canonical_example": """my_pipeline:
  description: "Crawl raw data, then query it"
  schedule: "0 2 * * *"          # cron, "@daily", or null for manual-only
  start_date: "2024-01-01"
  max_active_runs: 1
  default_args:
    retries: 2
    retry_delay: 60              # INTEGER SECONDS (0-300)
  tasks:
    crawl_raw:                   # task_id is the MAPPING KEY
      operator: airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator
      config:                    # operator kwargs are FLAT on the task
        Name: my-crawler
      wait_for_completion: true
      execution_timeout:         # must be a timedelta MAPPING
        __type__: datetime.timedelta
        minutes: 30
    run_query:
      operator: airflow.providers.amazon.aws.operators.athena.AthenaOperator
      query: "SELECT count(*) FROM my_db.my_table"
      database: my_db
      output_location: "s3://my-results-bucket/athena/"
      dependencies: [crawl_raw]  # upstream task_ids
    check_query:
      operator: airflow.providers.amazon.aws.sensors.athena.AthenaSensor
      # pass a value produced by an upstream task via XCom:
      query_execution_id: "{{ ti.xcom_pull(task_ids='run_query') }}"
      mode: reschedule           # COST: releases the worker slot between checks
      poke_interval: 60          # seconds between checks
      timeout: 3600              # ALWAYS bound a wait; Airflow defaults to 7 days
      dependencies: [run_query]
""",
    "rules": [
        {
            "rule": "Exactly one DAG per definition file.",
            "verified": "Two root keys -> ValidationException: 'DAG definition should contain a single DAG'.",
        },
        {
            "rule": "`tasks` MUST be a mapping keyed by task_id. A LIST of task objects is rejected.",
            "verified": "A list -> ValidationException: 'Invalid tasks configuration.'",
            "wrong": "tasks:\n  - task_id: a\n    operator: ...",
            "right": "tasks:\n  a:\n    operator: ...",
        },
        {
            "rule": "`operator` MUST be the fully qualified class path. Short names are rejected.",
            "verified": "operator: S3ListOperator -> ValidationException: \"operator 'S3ListOperator' is not supported\".",
            "wrong": "operator: S3ListOperator",
            "right": "operator: airflow.providers.amazon.aws.operators.s3.S3ListOperator",
        },
        {
            "rule": "Operator arguments are FLAT keys on the task. There is no `parameters:` wrapper.",
            "verified": "Nesting under `parameters` -> ValidationException: \"missing keyword arguments 'data', 's3_key'\".",
            "wrong": "task_a:\n  operator: <fqn>\n  parameters:\n    bucket: b",
            "right": "task_a:\n  operator: <fqn>\n  bucket: b",
        },
        {
            "rule": "Dependencies use `dependencies: [upstream_task_ids]`. `upstream_tasks` and `downstream_tasks` are NOT recognised.",
            "verified": "upstream_tasks -> ValidationException: \"Invalid arguments were passed ... {'upstream_tasks': ['a']}\" "
                        "(dag-factory forwards unknown keys straight to the operator constructor).",
            "wrong": "upstream_tasks: [extract]",
            "right": "dependencies: [extract]",
        },
        {
            "rule": "`retry_delay` is an INTEGER number of seconds (0-300). Duration strings are rejected.",
            "verified": "retry_delay: 30s -> ValidationException: 'unsupported type for timedelta seconds component: str'.",
            "wrong": 'retry_delay: "5m"',
            "right": "retry_delay: 300",
        },
        {
            "rule": "`execution_timeout` MUST be a mapping with `__type__: datetime.timedelta` plus at least one "
                    "timedelta field (weeks/days/hours/minutes/seconds/milliseconds/microseconds). Max 60 minutes.",
            "verified": "Int -> 'execution_timeout must be timedelta object but passed as type: int'. "
                        "String -> same with str. A bare mapping without __type__ -> same with dict. "
                        "Over 60 min -> 'execution_timeout (66 minutes) must be less than or equal to 60 minutes'.",
            "wrong": 'execution_timeout: "30m"',
            "right": "execution_timeout:\n  __type__: datetime.timedelta\n  minutes: 30",
        },
        {
            "rule": "`task_groups` is NOT supported.",
            "verified": "ValidationException: 'my_dag.task_groups: Unexpected element'.",
        },
        {
            "rule": "`default_args` accepts ONLY: owner, email, retries, retry_delay, priority_weight, "
                    "end_date, execution_timeout, trigger_rule, start_date, wait_for_downstream. "
                    "Any other key fails validation.",
            "verified": "retry_delay_sec in default_args -> \"Key error - 'retry_delay_sec' not in (...)\".",
        },
        {
            "rule": "`aws_conn_id`, `region_name`, `verify` and `botocore_config` are accepted but silently dropped. "
                    "CreateWorkflow returns Warnings: ['ignored attributes: aws_conn_id']. Omit them.",
            "verified": "Confirmed via the Warnings field on CreateWorkflow.",
        },
        {
            "rule": "`trigger_rule` IS honoured at run time, despite being listed as unsupported in the "
                    "'Task level parameters that are not supported' documentation table.",
            "verified": "A task with trigger_rule: all_done downstream of a FAILED task did execute. "
                        "It is also in the accepted default_args allowlist. Safe to use for cleanup tasks.",
        },
        {
            "rule": "A past `start_date` is accepted, despite the docs saying it 'must be in the future'.",
            "verified": "start_date: '2024-01-01' created successfully.",
        },
        {
            "rule": "COST: `mode: reschedule` releases the worker between checks and is the "
                    "primary cost lever for any wait beyond a couple of minutes. `deferrable: true` "
                    "is NOT an alternative — it is accepted and then ignored, as there is no "
                    "triggerer.",
            "verified": "Create time: mode: reschedule -> ACCEPTED; mode: poke -> ACCEPTED; "
                        "mode: nonsense -> \"The mode must be one of ['poke', 'reschedule']\". "
                        "End to end: a reschedule-mode sensor pokes repeatedly within one attempt, "
                        "reports UP_FOR_RESCHEDULE between checks, and honours its timeout. Each "
                        "cycle adds roughly 45s of scheduling overhead on top of poke_interval.",
        },
        {
            "rule": "COST: `mode` cannot go in `default_args` — it is not in the allowlist, so it must be "
                    "set on each sensor task individually.",
            "verified": "default_args: {mode: reschedule} -> \"Key error - 'mode' not in (...)\".",
        },
        {
            "rule": "COST: `poke_interval`, `timeout`, `exponential_backoff`, `max_wait` and `soft_fail` "
                    "are all accepted on sensors. Always set `timeout` — Airflow's default is 7 days "
                    "and the worker is billed for the whole wait.",
            "verified": "Each created successfully with no Warnings.",
        },
    ],
}

# Keys that dag-factory reserves and must not be used as operator arguments.
DAG_FACTORY_RESERVED_KEYS = {"__type__", "__args__", "__join__", "__and__", "__or__"}

# Task-level keys that are structural (consumed by dag-factory) rather than
# forwarded to the operator constructor.
TASK_STRUCTURAL_KEYS = {"operator", "dependencies", "task_id"}

# ── Supported Jinja template variables (verified against docs) ──
SUPPORTED_JINJA_VARIABLES = {
    "macros", "task_instance", "ti", "params",
    "ds", "ds_nodash", "ts", "ts_nodash",
}

# ── Supported macros ──
SUPPORTED_MACROS = {
    "macros.datetime", "macros.timedelta", "macros.dateutil",
    "macros.time", "macros.uuid", "macros.random",
    "datetime_diff_for_humans", "ds_add", "ds_format", "random",
}

# Commonly attempted Jinja variables that do NOT exist in MWAA Serverless,
# with the supported replacement.
UNSUPPORTED_JINJA_REPLACEMENTS = {
    "dag_run": "Not available. Use {{ params.* }} for configuration.",
    "logical_date": "Not available directly. Use {{ ds }}, {{ ds_nodash }}, {{ ts }} or {{ ts_nodash }}.",
    "execution_date": "Deprecated and unavailable. Use {{ ds }} / {{ ts }}.",
    "data_interval_start": "Not available. Use {{ ds }} / {{ ts }}.",
    "data_interval_end": "Not available. Use {{ ds }} / {{ ts }}.",
    "next_ds": "Not available. Use {{ macros.ds_add(ds, 1) }}.",
    "prev_ds": "Not available. Use {{ macros.ds_add(ds, -1) }}.",
    "run_id": "Not available.",
    "dag": "Not available.",
    "conf": "Not available.",
    "var": "Airflow Variables are not available. Use {{ params.* }}.",
    "conn": "Airflow Connections are not available; credentials come from the execution role.",
    "task": "Not available.",
    "outlets": "Not available.",
    "inlets": "Not available.",
}

# ── DAG-level parameters MWAA Serverless validates ──
VALIDATED_DAG_PARAMS = {
    "schedule": "Cron expression, an @preset (@daily, @hourly, ...), or null for manual-only runs",
    "start_date": "YYYY-MM-DD string. A past date is accepted in practice.",
    "end_date": "Must be after or equal to start_date",
    "max_active_runs": "Integer, must be below the account limit (default 16)",
}

# DAG-level keys accepted by the service (structural or validated).
ACCEPTED_DAG_KEYS = {
    "tasks", "params", "default_args", "schedule",
    "start_date", "end_date", "max_active_runs", "max_active_tasks",
}

# Keys the service accepts without complaint but reports in the CreateWorkflow
# `Warnings` list as "ignored attributes". Harmless, but they misrepresent what
# actually runs, so the validator flags them. Verified via the Warnings field.
SILENTLY_IGNORED_DAG_KEYS = {
    "description": "The service reports 'ignored attributes: description'. It has no effect on the run.",
    "max_active_runs": "The service reports 'ignored attributes: max_active_runs'. Per-workflow "
                       "concurrency is governed by the account/workflow quota, not this field.",
    "max_active_tasks": "Not applied — each task gets its own isolated worker.",
}

# ── DAG-level parameters ignored by MWAA Serverless ──
IGNORED_DAG_PARAMS = {
    "dag_id", "template_searchpath", "template_undefined", "user_defined_macros",
    "user_defined_filters", "catchup", "access_control",
    "jinja_environment_kwargs", "render_template_as_native_obj", "tags",
    "owner_links", "auto_register", "fail_fast", "dag_display_name",
    "depends_on_past", "email_on_failure", "email_on_retry",
    "max_consecutive_failed_dag_runs", "dagrun_timeout", "sla_miss_callback",
    "on_failure_callback", "on_success_callback", "is_paused_upon_creation",
    "schedule_interval",
}

# ── Task-level parameters MWAA Serverless validates ──
VALIDATED_TASK_PARAMS = {
    "task_id": "The mapping key. Must match ^[a-zA-Z0-9_.-]+$",
    "retries": "Integer 0-3 (default 1)",
    "retry_delay": "INTEGER SECONDS, 0-300 (default 300). Not a duration string.",
    "execution_timeout": "Mapping: {__type__: datetime.timedelta, minutes: N}. Max 60 minutes.",
}

# ── `default_args` allowlist (verified — anything else fails validation) ──
DEFAULT_ARGS_ALLOWLIST = {
    "owner", "email", "retries", "retry_delay", "priority_weight",
    "end_date", "wait_for_downstream", "execution_timeout",
    "trigger_rule", "start_date",
}

# ── Task-level parameters ignored by MWAA Serverless ──
# NOTE: `trigger_rule` and `deferrable` appear in the docs' unsupported table but
# behave differently in practice — see YAML_SCHEMA["rules"]. They are handled
# specially by the validator and deliberately excluded from this set.
IGNORED_TASK_PARAMS = {
    "email_on_retry", "email_on_failure", "retry_exponential_backoff",
    "depends_on_past", "ignore_first_depends_on_past", "wait_for_downstream",
    "priority_weight", "sla", "max_active_tis_per_dag",
    "max_active_tis_per_dagrun", "task_concurrency", "resources",
    "run_as_user", "executor_config", "doc", "doc_md", "doc_rst",
    "doc_json", "doc_yaml", "task_display_name", "logger_name",
    "allow_nested_operators", "inlets", "outlets", "map_index_template",
    "email", "owner", "max_retry_delay", "on_execute_callback",
    "on_failure_callback", "on_success_callback", "on_retry_callback",
    "on_skipped_callback", "wait_for_past_depends_before_skipping",
    "do_xcom_push", "multiple_outputs", "start_date", "end_date",
    "weight_rule", "queue", "pool", "pool_slots", "pre_execute",
    "post_execute", "executor", "task_group", "task_group_name",
}

# ── AWS base operator attributes ──
AWS_BASE_OPERATOR_ATTRS = {
    "aws_conn_id": "Controlled by the service. Accepted but dropped (CreateWorkflow returns an 'ignored attributes' warning).",
    "verify": "Not supported. Accepted but dropped.",
    "botocore_config": "Not supported. Accepted but dropped.",
    "region_name": "Taken from the workflow's Region. Accepted but dropped.",
}

# ── Features explicitly NOT supported ──
UNSUPPORTED_FEATURES = [
    "Dynamic task mapping (.expand() / .expand_kwargs() / .map())",
    "Task groups (task_groups is rejected: 'Unexpected element')",
    "Decorated tasks and DAGs (@task, @task.python, @task.bash, @dag) — the TaskFlow API",
    "Operators outside the allowlist, including any non-Amazon, non-standard provider operator",
    "Airflow Variables, Connections and Pools",
    "Multiple DAGs in one definition file",
    "Direct Airflow UI access (monitor via CloudWatch Logs)",
    "verify / botocore_config / region_name / aws_conn_id on operators",
]

# Previously unsupported, now available — call this out so guidance stays current.
RECENTLY_ADDED_FEATURES = {
    "python_and_bash_operators": (
        "PythonOperator and BashOperator ARE now supported. Earlier guidance that said "
        "'no Python/Bash operators' is out of date. They run custom code that you upload "
        "separately via the CreateWorkflow/UpdateWorkflow `Code` parameter. See CODE_SUPPORT."
    ),
}

# ══════════════════════════════════════════════════════════════════════════
#  PYTHON / BASH OPERATOR CODE SUPPORT
# ══════════════════════════════════════════════════════════════════════════
CODE_SUPPORT = {
    "operators": {
        "PythonOperator": {
            "fqn": "airflow.providers.standard.operators.python.PythonOperator",
            "legacy_fqn": "airflow.operators.python.PythonOperator",
            "required_param": "python_callable",
            "value_format": "module_name.function_name (e.g. 'transform.clean_rows'). "
                            "NOT a lambda, not an inline def, and not a bare function name.",
        },
        "BashOperator": {
            "fqn": "airflow.providers.standard.operators.bash.BashOperator",
            "legacy_fqn": "airflow.operators.bash.BashOperator",
            "required_param": "bash_command",
            "value_format": "A shell command string, or './script.sh' for a script in the code bundle.",
        },
    },
    "how_code_is_delivered": (
        "The YAML definition and the code are two SEPARATE S3 objects. The YAML goes in "
        "DefinitionS3Location; the code goes in Code.S3Location. Both are snapshotted "
        "into an immutable workflow version on create/update."
    ),
    "accepted_code_files": [
        "a single .py file",
        "a single .sh script",
        "a .zip archive with all modules and dependencies at the ARCHIVE ROOT (no nested folders)",
    ],
    "worker_environment": {
        "os_arch": "Linux / x86_64",
        "cpu_memory": "1 vCPU / 3 GiB per task",
        "python": "3.12",
        "code_extracted_to": "/usr/local/airflow/dags (also the BashOperator working directory)",
        "credentials": "The workflow execution role is resolved automatically by boto3. Do not embed credentials.",
    },
    "preinstalled_packages": {
        "apache-airflow": "3.0.6",
        "apache-airflow-providers-amazon": "9.32.0",
        "boto3": "1.43.1",
        "botocore": "1.43.1",
        "dag-factory": "1.0.0",
        "pyyaml": "unpinned",
        "lz4": "4.4.4",
    },
    "packaging_dependencies": (
        "pip install -r requirements.txt --target ./pkg --platform manylinux2014_x86_64 "
        "--python-version 3.12 --only-binary=:all:  then  (cd pkg && zip -r ../code.zip . "
        "-x '*__pycache__*' '*.pyc'). Pre-installed packages take precedence over bundled "
        "copies, so do not bundle boto3 or airflow."
    ),
    "limits": {
        "max_code_size": "250 MB (compressed file and uncompressed archive)",
        "code_storage_per_account": "75 GB",
    },
    "no_internet_by_default": (
        "Python and Bash tasks have NO internet access unless you attach a VPC with egress "
        "via NetworkConfiguration. They can reach S3, ECR and CloudWatch. Calls to third-party "
        "APIs will hang or fail — stage that data in S3 first."
    ),
    "when_to_use": (
        "Use a native AWS operator whenever one exists — it is simpler, needs no code bundle, "
        "and is easier to debug. Reach for PythonOperator only for genuine glue logic: "
        "reshaping an upstream XCom value, branching on a computed condition, or calling an "
        "AWS API that has no operator."
    ),
}

# ══════════════════════════════════════════════════════════════════════════
#  PASSING VALUES BETWEEN TASKS
# ══════════════════════════════════════════════════════════════════════════
PARAMETER_PASSING = {
    "mechanism": (
        "Tasks exchange values through XCom. An operator's return value is pushed "
        "automatically; a downstream task reads it with a Jinja reference: "
        "{{ ti.xcom_pull(task_ids='<upstream_task_id>') }}"
    ),
    "rules": [
        "The task you pull from MUST also be listed in `dependencies`, otherwise it may not "
        "have run yet and the pull returns None.",
        "XCom values are capped at 100 KB. Pass an S3 URI, not the data itself.",
        "Jinja renders to a STRING. Indexing works ({{ ti.xcom_pull(task_ids='x')['id'] }}) but "
        "arithmetic and type coercion do not — do that in a PythonOperator.",
        "Only templated operator fields render Jinja. Check the operator's template_fields "
        "before relying on a Jinja reference in an unusual field.",
        "CloudFormationCreateStackOperator does NOT return stack outputs via XCom. Do not "
        "attempt to read Outputs[n].OutputValue from it — pass concrete values as params instead.",
    ],
    "worked_example": """# Verified working: Glue job -> sensor -> Athena
tasks:
  transform:
    operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
    job_name: "{{ params.glue_job_name }}"
    wait_for_completion: false          # return immediately, sensor waits
  wait_transform:
    operator: airflow.providers.amazon.aws.sensors.glue.GlueJobSensor
    job_name: "{{ params.glue_job_name }}"
    run_id: "{{ ti.xcom_pull(task_ids='transform') }}"   # <- run ID from XCom
    dependencies: [transform]
""",
    "python_example": """# In your code bundle (transform.py):
def summarise(**context):
    keys = context["ti"].xcom_pull(task_ids="list_input")   # read upstream
    return {"count": len(keys), "first": keys[0] if keys else None}

# In the YAML:
tasks:
  summarise:
    operator: airflow.providers.standard.operators.python.PythonOperator
    python_callable: transform.summarise
    dependencies: [list_input]
  report:
    operator: airflow.providers.amazon.aws.operators.sns.SnsPublishOperator
    target_arn: "{{ params.topic_arn }}"
    message: "processed {{ ti.xcom_pull(task_ids='summarise')['count'] }} files"
    dependencies: [summarise]
""",
}

# ══════════════════════════════════════════════════════════════════════════
#  QUOTAS
# ══════════════════════════════════════════════════════════════════════════
QUOTAS = {
    "max_workflows_per_account": 100,
    "max_versions_per_workflow": 50,
    "max_concurrent_runs_per_account": 100,
    "max_concurrent_runs_per_workflow": 20,
    "max_xcom_kb": 100,
    "max_dag_definition_kb": 50,
    "max_code_storage_gb": 75,
    "max_task_execution_timeout_minutes": 60,
    "max_retries_per_task": 3,
    "max_retry_delay_seconds": 300,
}

# ══════════════════════════════════════════════════════════════════════════
#  DAG AUTHORING POLICY
# ══════════════════════════════════════════════════════════════════════════
# The failure mode this policy exists to prevent: an agent asked for "a DAG that
# runs two Glue jobs and an Athena query" produces forty tasks that provision
# CloudFormation stacks, publish SNS alerts and emit CloudWatch metrics, none of
# which was requested and most of which does not run.

AUTHORING_POLICY = {
    "scope_discipline": {
        "principle": "Build exactly what was asked for. Nothing else.",
        "rules": [
            "One task per operation the user actually named. Do not invent extra steps.",
            "Do NOT add monitoring, alerting, notification, metric-publishing, logging or "
            "audit tasks unless the user asked for them.",
            "Do NOT add CloudFormation provisioning unless the user asked you to create the "
            "underlying infrastructure. Assume named resources (Glue jobs, crawlers, "
            "databases, buckets) already exist and reference them via params.",
            "Do NOT add cleanup/teardown tasks unless the DAG itself created the resource.",
            "Do NOT add sensors that duplicate an operator's own wait_for_completion behaviour.",
        ],
    },
    "gentle_prompting": {
        "principle": (
            "When a best practice is missing from the request, do not silently add it and do "
            "not silently omit it. Build the minimal DAG, then tell the user what you left out "
            "and ask whether they want it."
        ),
        "how_to_phrase": (
            "State the DAG you built, then: 'I kept this to what you asked for. A few things "
            "you may want to add: (1) ... (2) ... Want me to include any of these?'"
        ),
        "candidates_to_offer": [
            "Failure notification (SnsPublishOperator with trigger_rule: one_failed)",
            "Retries and per-task execution_timeout, if the defaults are not appropriate",
            "A data-quality gate (GlueDataQualityOperator) between transform and consume steps",
            "A crawler run after a write, so the Glue Catalog reflects new partitions",
            "An S3KeySensor at the start, if the pipeline depends on an upstream file landing",
            "A schedule, if the user did not specify one (the DAG is manual-only without it)",
            "max_active_runs: 1, if the pipeline is not safe to run concurrently",
        ],
        "must_ask_when_unknown": [
            "Resource identifiers the DAG cannot work without: Glue job names, crawler names, "
            "database and table names, bucket names, Athena output location, role ARNs.",
            "Whether named resources already exist, or the DAG should create them.",
            "The schedule.",
            "Whether a step should block on completion, or fire and continue.",
        ],
        "never_do": [
            "Never invent a plausible-looking ARN, bucket name or account ID. Use a "
            "{{ params.x }} reference with an obviously-placeholder default, and tell the user "
            "it needs replacing.",
        ],
    },
    "cost_efficiency": {
        "principle": (
            "MWAA Serverless bills for the time a task occupies a worker. A task that spends "
            "20 minutes sleeping in a poll loop costs the same as 20 minutes of real work. "
            "Waiting should therefore be done in a way that releases the worker."
        ),
        "the_one_lever_that_works": (
            "mode: reschedule on the sensor. In poke mode (the default) the sensor holds its worker "
            "slot for the entire wait. In reschedule mode the task exits after each check and is "
            "re-queued, so nothing is held in between. Gated on "
            "schema.RESCHEDULE_MODE_SUPPORTED, which is verified against the live service."
        ),
        "deferrable_does_not_work": (
            "deferrable: true is the usual Airflow answer and it does not apply here. MWAA "
            "Serverless has no triggerer: CreateWorkflow accepts the argument and returns "
            "Warnings: ['ignored attributes: deferrable'], then runs the task in blocking mode "
            "anyway. Use mode: reschedule instead."
        ),
        "reschedule_cycle_overhead": (
            "A reschedule cycle is a task start, and measured end to end it adds roughly 45s on "
            "top of poke_interval. So a 30s interval behaves like a ~75s one. Keep poke_interval "
            "in the 30-120s range: below that you pay scheduling churn without checking sooner."
        ),
        "rules": [
            "Any sensor expected to wait more than ~2 minutes SHOULD set mode: reschedule.",
            "ALWAYS set a timeout on a sensor. Airflow's default is 7 days, and the wait is "
            "billed, so an unbounded wait is an unbounded bill.",
            "For a LONG job, prefer wait_for_completion: false plus a reschedule-mode sensor over "
            "wait_for_completion: true — the operator returns in seconds and only the cheap "
            "sensor waits. For a SHORT job, blocking in one task is simpler and cheaper.",
            "Do not add a sensor that duplicates an operator's own wait.",
            "Keep poke_interval between 30 and 120 seconds; each reschedule cycle is a task "
            "start plus about 45s of scheduling overhead.",
            "Push long waits into the service being orchestrated where you can — a Glue job that "
            "polls its own dependency costs Glue time, not Airflow worker time.",
            "exponential_backoff: true with max_wait is accepted and reduces API chatter on an "
            "unpredictable wait.",
            "mode cannot be set in default_args (not in the allowlist) — which is moot, since it "
            "should not be set at all.",
        ],
        "if_reschedule_regresses": (
            "Set schema.RESCHEDULE_MODE_SUPPORTED = False. The builder stops emitting mode, the "
            "validator reports it as an error, and repair downgrades it to poke — the guidance "
            "follows from that one flag."
        ),
    },
    "correctness_checklist": [
        "Every task's operator is a fully qualified path from the allowlist.",
        "`tasks` is a mapping, not a list.",
        "Operator arguments are flat on the task; there is no `parameters:` block.",
        "Dependencies use `dependencies:`; the graph is acyclic and every referenced task exists.",
        "Every semantically required operator argument is present.",
        "Every `ti.xcom_pull(task_ids='X')` names a task that is also in that task's `dependencies`.",
        "retry_delay is an integer; execution_timeout is a __type__ timedelta mapping under 60 minutes.",
        "No aws_conn_id / region_name / verify / botocore_config.",
        "Cleanup tasks (only for resources this DAG created) set trigger_rule: all_done.",
        "Every sensor that may wait more than ~2 minutes sets mode: reschedule and a timeout.",
        "The definition is under 50 KB.",
    ],
    "best_practices": [
        "Parameterise every environment-specific value with {{ params.x }} and give it a default.",
        "For a SHORT job (under ~5 minutes) prefer one operator that waits "
        "(wait_for_completion: true) — simpler, and the blocked time is cheap.",
        "For a LONG job, use wait_for_completion: false plus a sensor with mode: reschedule, so "
        "the worker is released between checks. See the cost_efficiency policy.",
        "Set max_active_runs: 1 for pipelines that write to a shared destination.",
        "Keep retries low (0-3) and retry_delay short; MWAA Serverless caps both.",
        "Put the long tail of work in the service being orchestrated (Glue, EMR, Athena), not "
        "in PythonOperator tasks — each task gets 1 vCPU / 3 GiB and a 60 minute ceiling.",
        "Name tasks after what they do (crawl_raw_events, not task_1).",
    ],
}

# ══════════════════════════════════════════════════════════════════════════
#  API / CLI
# ══════════════════════════════════════════════════════════════════════════
MWAA_API_ACTIONS = {
    "service_name": "mwaa-serverless",
    "boto3_client": "mwaa-serverless (requires boto3 >= 1.40 for the `Code` parameter; "
                    "older botocore registers the service as 'airflow-serverless' without Code support)",
    "iam_action_prefix": "airflow-serverless:",
    "cli_prefix": "aws mwaa-serverless",
    "create_workflow_params": {
        "Name": "required",
        "DefinitionS3Location": "required — {Bucket, ObjectKey, VersionId?}",
        "RoleArn": "required — NOT 'ExecutionRoleArn'",
        "Code": "optional — {S3Location: {Bucket, ObjectKey, VersionId?}}. Required for Python/Bash tasks.",
        "TriggerMode": "optional — SCHEDULED | MANUAL | DISABLED",
        "LoggingConfiguration": "optional — {LogGroupName}",
        "NetworkConfiguration": "optional — {SubnetIds, SecurityGroupIds} for VPC access",
        "EncryptionConfiguration": "optional — {Type, KmsKeyId}",
        "Description": "optional",
        "Tags": "optional",
    },
    "actions": [
        "CreateWorkflow", "UpdateWorkflow", "DeleteWorkflow", "GetWorkflow",
        "ListWorkflows", "ListWorkflowVersions", "StartWorkflowRun",
        "StopWorkflowRun", "GetWorkflowRun", "ListWorkflowRuns",
        "ListTagsForResource", "TagResource", "UntagResource",
    ],
    "response_notes": [
        "CreateWorkflow/UpdateWorkflow return a `Warnings` list — ALWAYS surface it. "
        "It is how the service reports silently-dropped attributes.",
        "GetWorkflow returns `WorkflowDefinition` with the YAML inline; there is no need to "
        "read the S3 object, and it reflects the immutable snapshot rather than whatever "
        "is in the bucket now.",
        "The workflow name in the ARN has a random 10-character suffix appended "
        "(my-wf -> my-wf-a1b2c3d4e5). ListWorkflows returns the BARE name. "
        "CloudWatch log groups use the SUFFIXED name from the ARN.",
    ],
}

# ══════════════════════════════════════════════════════════════════════════
#  OBSERVABILITY / DEBUGGING
# ══════════════════════════════════════════════════════════════════════════
OBSERVABILITY = {
    "log_group": "/aws/mwaa-serverless/{workflow_name_with_arn_suffix}/",
    "log_stream": "workflow_id={wf}/run_id={run_id}/task_id={task_id}/attempt={n}.log",
    "log_format": "One JSON object per event: {timestamp, level, event, logger, ...}. "
                  "Task exceptions carry error_detail[].exc_type / .exc_value / .frames.",
    "task_outcome_marker": 'The final event of each task stream is {"event": "Task finished", '
                           '"final_state": "success"|"failed", "exit_code": N}.',
    "critical_caveat": (
        "A workflow run can report RunState=SUCCESS while individual tasks FAILED. Verified: a "
        "task that raised NoSuchBucket was followed by a trigger_rule: all_done task that "
        "succeeded, and the run reported SUCCESS. NEVER treat RunState=SUCCESS as proof that "
        "every task succeeded — read each task's final_state from its log stream."
    ),
    "task_instances_format": (
        "GetWorkflowRun returns RunDetail.TaskInstances as a list of STRINGS shaped "
        "'ex_<uuid>_<task_id>_<attempt>', not objects. Parse the task_id out of the middle."
    ),
}

# ══════════════════════════════════════════════════════════════════════════
#  SERVICE OVERVIEW
# ══════════════════════════════════════════════════════════════════════════
SERVICE_OVERVIEW = {
    "what_is_mwaa_serverless": (
        "Amazon MWAA Serverless runs Apache Airflow workflows without an Airflow environment "
        "to manage. Workflows are declared in YAML (dag-factory format), scale automatically, "
        "bill per task-second, and each workflow gets its own IAM execution role. "
        "Runtime: Apache Airflow 3.0.6 on Python 3.12."
    ),
    "how_it_differs_from_classic_mwaa": {
        "service_name": "mwaa-serverless (classic MWAA is 'mwaa')",
        "no_environments": "There is no environment to create. The unit of deployment is a workflow.",
        "no_airflow_ui": "No Airflow UI. Observability is CloudWatch Logs plus the console.",
        "yaml_only": "Workflows are YAML, not Python DAG files. Custom code is uploaded separately.",
        "per_workflow_isolation": "One execution role and isolated compute per workflow; each task "
                                  "provisions its own worker, so expect per-task startup latency.",
        "pay_per_use": "You pay only for task run time.",
    },
    "yaml_schema": YAML_SCHEMA,
    "authoring_policy": AUTHORING_POLICY,
    "parameter_passing": PARAMETER_PASSING,
    "code_support": CODE_SUPPORT,
    "quotas": QUOTAS,
    "observability": OBSERVABILITY,
    "recently_added": RECENTLY_ADDED_FEATURES,
    "deployment_workflow": {
        "step_1_s3_bucket": "An S3 bucket in the same Region, public access blocked, versioning on.",
        "step_2_execution_role": (
            "An IAM role trusting airflow-serverless.amazonaws.com, with logs:CreateLogStream and "
            "logs:PutLogEvents plus the permissions the tasks need. Add iam:PassRole only when a "
            "task hands a role to another service (Glue, EMR, SageMaker)."
        ),
        "step_3_upload": "Upload the YAML definition, and the code bundle if using Python/Bash tasks.",
        "step_4_create": (
            "aws mwaa-serverless create-workflow --name <name> "
            "--definition-s3-location '{\"Bucket\":\"<b>\",\"ObjectKey\":\"<k>\"}' "
            "[--code '{\"S3Location\":{\"Bucket\":\"<b>\",\"ObjectKey\":\"code.zip\"}}'] "
            "--role-arn <role-arn>"
        ),
        "step_5_run": "aws mwaa-serverless start-workflow-run --workflow-arn <arn>",
        "step_6_monitor": (
            "aws mwaa-serverless get-workflow-run --workflow-arn <arn> --run-id <id>. "
            "Run states: STARTING, QUEUED, RUNNING, SUCCESS, FAILED, TIMEOUT, STOPPING, STOPPED. "
            "Then verify per-task final_state in CloudWatch — see OBSERVABILITY.critical_caveat."
        ),
    },
    "workflow_types": {
        "SCHEDULED": "Runs on the YAML schedule; can also be started on demand.",
        "MANUAL": "Ignores the schedule; on-demand only.",
        "DISABLED": "Cannot run at all.",
    },
    "execution_role_trust_policy": {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "airflow-serverless.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    },
    "prerequisites": [
        "AWS account with airflow-serverless:* permissions",
        "S3 bucket in the same Region (versioning recommended)",
        "IAM execution role trusting airflow-serverless.amazonaws.com",
        "AWS CLI v2 recent enough to expose --code (verify: aws mwaa-serverless create-workflow help)",
    ],
    "available_regions": [
        "us-east-1", "us-east-2", "us-west-1", "us-west-2",
        "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-central-2",
        "eu-north-1", "eu-south-1", "eu-south-2",
        "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
        "ap-south-1", "ap-south-2",
        "ap-southeast-1", "ap-southeast-2", "ap-southeast-3", "ap-southeast-4",
        "ap-southeast-5", "ap-southeast-6", "ap-southeast-7",
        "ap-east-1", "ap-east-2",
        "ca-central-1", "ca-west-1", "sa-east-1", "af-south-1",
        "il-central-1", "mx-central-1", "us-gov-east-1", "us-gov-west-1",
    ],
}
