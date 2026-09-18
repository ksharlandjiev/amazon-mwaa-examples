# MWAA Serverless DAG reference

Background on what MWAA Serverless actually accepts, and the rules this server
enforces on your behalf.

**You should not need this to use the server.** It is here for three cases: you are
debugging a DAG the server did not author, you are reviewing YAML by hand, or you want
to see the evidence behind a rule before trusting it. The server serves the same
material to an agent through the `get_serverless_overview`, `get_dag_yaml_spec` and
`get_serverless_constraints` tools, so an agent reads it without you pasting it.

Every rule below was checked against the live `mwaa-serverless` API rather than inferred
from documentation. Where the two disagree, the verified behaviour is what the server
reports — including two cases where the documentation is wrong.

See [the README](../README.md) for installation and usage.

---

## The YAML schema is not the obvious one

MWAA Serverless runs [dag-factory](https://astronomer.github.io/dag-factory/) 1.0.0. It rejects the shape most people — and most LLMs — write by default. These are the six mistakes that account for nearly every "it deployed but nothing worked":

| What people write | What the service requires | Error you get |
|---|---|---|
| `tasks:` as a **list** of task objects | `tasks:` as a **mapping** keyed by task_id | `Invalid tasks configuration.` |
| `operator: S3ListOperator` | the **fully qualified** path | `operator 'S3ListOperator' is not supported` |
| operator args nested under `parameters:` | args **flat** on the task | `missing keyword arguments 'data', 's3_key'` |
| `upstream_tasks: [extract]` | `dependencies: [extract]` | `Invalid arguments were passed ... {'upstream_tasks': ['a']}` |
| `retry_delay: "5m"` | `retry_delay: 300` (integer seconds) | `unsupported type for timedelta seconds component: str` |
| `execution_timeout: "30m"` | a `__type__: datetime.timedelta` mapping | `execution_timeout must be timedelta object but passed as type: str` |

Also verified:

- **One DAG per definition file.** Two root keys → `DAG definition should contain a single DAG`.
- **`task_groups` is not supported** → `Unexpected element`.
- **`default_args` accepts only ten keys**: `owner`, `email`, `retries`, `retry_delay`, `priority_weight`, `end_date`, `wait_for_downstream`, `execution_timeout`, `trigger_rule`, `start_date`. Anything else fails validation.
- **`aws_conn_id`, `region_name`, `verify` and `max_active_runs` are silently dropped.** `CreateWorkflow` returns `Warnings: ['ignored attributes: ...']` — this server surfaces that list instead of discarding it.
- **`trigger_rule` does work** at run time, despite appearing in the documentation's unsupported table.
- **A past `start_date` is accepted**, despite the docs saying it must be in the future.

### Canonical shape

```yaml
my_pipeline:
  description: "Crawl raw data, then query it"
  schedule: "0 2 * * *"                 # cron, "@daily", or null for on-demand only
  default_args:
    retries: 2
    retry_delay: 60                     # INTEGER SECONDS (0-300)
  params:
    db: analytics
    athena_out: "s3://my-results/athena/"
  tasks:
    crawl_raw:                          # task_id is the MAPPING KEY
      operator: airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator
      config:                           # operator args are FLAT on the task
        Name: my-crawler
      wait_for_completion: true
      execution_timeout:                # must be a timedelta MAPPING
        __type__: datetime.timedelta
        minutes: 30
    run_query:
      operator: airflow.providers.amazon.aws.operators.athena.AthenaOperator
      query: "SELECT count(*) FROM events"
      database: "{{ params.db }}"
      output_location: "{{ params.athena_out }}"
      dependencies: [crawl_raw]         # upstream task_ids
    wait_for_file:
      operator: airflow.providers.amazon.aws.sensors.s3.S3KeySensor
      bucket_name: my-landing-bucket
      bucket_key: incoming/events.json
      poke_interval: 60
      timeout: 3600                     # COST: always bound a wait (default is 7 days)
```

## Passing values between tasks

Tasks exchange values through XCom. An operator's return value is pushed automatically; a downstream task reads it with a Jinja reference, and **must also list that task in `dependencies`**:

```yaml
tasks:
  transform:
    operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
    job_name: "{{ params.glue_job_name }}"
    wait_for_completion: false          # return immediately; the sensor waits
  wait_transform:
    operator: airflow.providers.amazon.aws.sensors.glue.GlueJobSensor
    job_name: "{{ params.glue_job_name }}"
    run_id: "{{ ti.xcom_pull(task_ids='transform') }}"   # run ID from XCom
    dependencies: [transform]
```

`describe_operator` tells you what any operator returns. Two traps the validator catches:

- **`CloudFormationCreateStackOperator` returns `None`.** Stack Outputs are *not* available through XCom. Reading `['CreateStackResponse']['Outputs'][0]['OutputValue']` raises `TypeError: 'NoneType' object is not subscriptable` at run time. Pass known values in as params instead.
- **Pulling from a task that is not upstream** silently yields `None`, because the task may not have run.

XCom values are capped at 100 KB — pass an S3 URI, not the data.

## Python and Bash operators

`PythonOperator` and `BashOperator` are now supported. The workflow definition and the code are two separate S3 objects: the YAML goes in `DefinitionS3Location`, the code in the `Code` parameter.

```yaml
tasks:
  summarise:
    operator: airflow.providers.standard.operators.python.PythonOperator
    python_callable: transform.summarise      # module.function, at the archive ROOT
  notify:
    operator: airflow.providers.standard.operators.bash.BashOperator
    bash_command: "echo processed {{ ti.xcom_pull(task_ids='summarise')['count'] }}"
    dependencies: [summarise]
```

Constraints that catch people out:

- Modules must be at the **root** of the zip — nested directories are not importable.
- `python_callable` must be `module_name.function_name`. A bare function name is rejected.
- The worker is Linux/x86_64, 1 vCPU / 3 GiB, Python 3.12; code is extracted to `/usr/local/airflow/dags`, which is also the `BashOperator` working directory.
- **No internet access** unless you attach a VPC via `NetworkConfiguration`. S3, ECR and CloudWatch are reachable; third-party APIs are not.
- `boto3`, `botocore`, `apache-airflow` and `dag-factory` are pre-installed and take precedence over bundled copies — do not bundle them.
- Deploying code needs `boto3 >= 1.40`; older botocore has no `Code` parameter.

`build_code_bundle` zips your modules, checks they parse, verifies callables accept the Airflow context, flags imports that cannot work without internet access, and refuses hard-coded credentials. `check_dag_code_consistency` confirms every `python_callable` resolves to a real function in a real module.

## Authoring policy: build what was asked for

The server pushes agents toward minimal, working pipelines:

- One task per operation the user actually named.
- No monitoring, alerting, notification, provisioning or cleanup tasks unless requested.
- Named resources (Glue jobs, crawlers, databases, buckets) are assumed to exist and referenced via `params`.
- Missing best practices are **offered, not added**. `plan_pipeline` returns `deliberately_not_added` for exactly this.
- Values the DAG cannot work without come back in `questions_to_ask_the_user`. Inventing an ARN or bucket name is never acceptable.

## Cost: don't pay for waiting

MWAA Serverless bills for the time a task occupies a worker. A sensor that sits in a poll
loop for 20 minutes costs the same as 20 minutes of real work, so *how* a DAG waits is a
pricing decision.

**The lever is `mode: reschedule` on sensors.** In the default `poke` mode a sensor holds
its worker slot for the entire wait. In `reschedule` mode the task exits after each check
and is re-queued, so nothing is held in between.

`build_dag_yaml` applies this automatically — every sensor is emitted with
`mode: reschedule`, a `poke_interval` and a `timeout`, and the response lists what it set
in `cost_optimizations_applied`:

```yaml
wait_transform:
  operator: airflow.providers.amazon.aws.sensors.glue.GlueJobSensor
  job_name: '{{ params.glue_job }}'
  run_id: "{{ ti.xcom_pull(task_ids='transform_orders') }}"
  dependencies: [transform_orders]
  mode: reschedule      # released between checks instead of held for the whole wait
  poke_interval: 60     # each cycle is a task start — 30-120s is the useful range
  timeout: 3600         # ALWAYS bound the wait; Airflow's default is 7 days
```

Pass `params: {"mode": "poke"}` to opt out for a wait that resolves in a minute or two,
where the re-queue overhead is not worth it. The validator emits a `COST:` hint whenever a
sensor is left in poke mode.

### `deferrable: true` is a trap

The usual Airflow answer does not apply here. MWAA Serverless has **no triggerer**:
`CreateWorkflow` accepts `deferrable: true` and returns
`Warnings: ['ignored attributes: deferrable']`, then runs the task in blocking mode anyway,
so a DAG that looks cost-optimised is not. `repair_dag_yaml` rewrites `deferrable` on a
sensor into `mode: reschedule`.

### Reschedule cycles are not free

A cycle is a task start, and measured end to end it adds roughly **45 s** of scheduling
overhead on top of `poke_interval` — a 30 s interval behaves like a ~75 s one. Keep
`poke_interval` between 30 and 120 s; below that you pay churn without checking sooner.

### Long-running jobs: fire and watch, don't block

`wait_for_completion: true` keeps a worker for the job's full duration. For anything longer
than a few minutes it is cheaper to return immediately and wait in a reschedule-mode sensor:

| Job duration | Cheaper shape |
|---|---|
| Seconds to ~5 min | One operator with `wait_for_completion: true`. The extra task is not worth it. |
| Longer than that | `wait_for_completion: false` + a paired sensor with `mode: reschedule`. |

Only the user knows which case applies, so `plan_pipeline` returns a `cost_plan` with the
question rather than guessing, and lists what it will apply automatically. `mode` cannot be
set in `default_args` — it is not in the service's allowlist, so it goes on each sensor
task; `repair_dag_yaml` moves a misplaced one down onto the sensors.

> Reschedule support is gated on a single flag, `schema.RESCHEDULE_MODE_SUPPORTED`. Set it
> to `False` and the builder stops emitting `mode`, the validator reports it as an error,
> and repair downgrades it to `poke` — the guidance follows from that one place.

