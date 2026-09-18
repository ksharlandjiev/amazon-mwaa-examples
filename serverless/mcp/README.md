## Disclaimer

AWS code samples are example code that demonstrates practical implementations of AWS services for specific use cases and scenarios.

These application solutions are not supported products in their own right, but educational examples to help our customers use our products for their applications. As our customer, any applications you integrate these examples into should be thoroughly tested, secured, and optimized according to your business's security standards & policies before deploying to production or handling production workloads.

# MWAA Serverless MCP Server

MCP server on AWS Lambda that helps AI agents author, validate, deploy and debug Amazon MWAA Serverless workflows.

Its main job is to stop agents producing DAGs that look plausible and do not run. Every schema rule it enforces was verified against the live `mwaa-serverless` API rather than inferred from documentation, and where the two disagree the verified behaviour is what the server reports.

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

## Recommended flow

### Author a real pipeline

You need two things before step 4, and neither is created for you:

- **an S3 bucket you own**, for the definition and any code bundle;
- **a workflow execution role** — step 3 generates the policy and the CLI commands for it.

1. `plan_pipeline(["glue_job", "glue_job", "glue_crawler", "athena_query"])` — get the operators, the questions to ask, and what was left out.
2. Ask the user those questions, then `build_dag_yaml(...)` — correct-by-construction YAML, already validated. Check `values_adjusted` for anything it coerced or capped on your behalf.
3. `generate_execution_role(account_id=…, region=…)` — an ARN-scoped policy plus the CLI commands to create the role. Do this **before** preflight: preflight needs the role ARN. Read `how_to_scope_down`; if the DAG tears down what it creates, `teardown_tasks_affected` names the tasks that will fail until you pass `include_destructive_actions=True` (destruction is withheld by default).
4. `preflight_dag_yaml(yaml, bucket, role_arn)` — the service validates it for real, then the throwaway workflow is deleted. Branch on `verdict`: `valid`, `invalid`, or `indeterminate` (the check could not be completed, so nothing is known — never read that as a pass).
5. `mwaa_deploy_and_run(...)` — note this **updates** an existing workflow of the same name and starts a **billable** run.
6. `mwaa_poll_run(...)` then **`mwaa_verify_run_tasks(...)`**.

### Fix hand-written or inherited YAML

`validate_dag_yaml` → `repair_dag_yaml` → `preflight_dag_yaml`.

### Migrate a Python DAG

`analyze_python_dag_tool` → `convert_python_to_yaml_tool` → `validate_dag_yaml`. The converter emits mapping-shaped, fully-qualified YAML, converts `timedelta(...)` to the right form, and flags Python/Bash tasks that need a code bundle. Your Python is only ever parsed with `ast`, never executed.

**Check `faithful` and `dropped`, not just `valid`.** `valid` means the YAML matches the
schema; `faithful: false` means the emitted DAG is *not equivalent to the Python you
supplied*, and every difference is itemised in `dropped` with the source expression and
what to do about it. Things that land there rather than disappearing: arguments whose
value is a variable, function call or f-string; lists and dicts containing any of those;
`default_args` keys the service does not honour (`depends_on_past`, `sla`, `email`);
dependency edges whose endpoint is built in a helper or a loop; duplicate `task_id`s; and
a second DAG in the same file. `analyze_python_dag_tool` reports the same thing up front
as `conversion_would_drop`.

## Where a model is (and is not) involved

37 of the 38 tools are deterministic Python — schema validation, repair, planning, YAML
assembly, code bundling, deploy and run inspection all run without calling a model. Your
own agent supplies the intelligence; this server supplies verified rules and AWS calls.

Exactly one tool calls a model: `mwaa_get_failed_runs`.

> ### ⚠ Data flow: failure analysis is **on by default**
>
> `mwaa_get_failed_runs` takes `analyze=true` by default, and the deployed stack grants
> `bedrock:InvokeModel` by default (`EnableFailureAnalysis=true`), so the feature works
> out of the box. Know what that means before you use it.
>
> **What leaves your account.** The collected failure details **including CloudWatch task
> log excerpts** (up to 12,000 characters) are sent to Amazon Bedrock. Task logs routinely
> contain bucket names, ARNs, account ids, table names and sometimes fragments of your
> data.
>
> **Where it goes.** The default model chain leads with `us.anthropic.claude-haiku-4-5-…`
> and `us.amazon.nova-lite-v1:0` — both **cross-Region inference profiles** — so that
> content may be processed in a different AWS Region than your workflows run in. The
> `us.` prefixes are not incidental: most current models are not offered for direct
> on-demand invocation, which is why the chain leads with profiles rather than
> Region-local ids.
>
> **You have four ways to control it**, from most to least restrictive:
>
> | Goal | How | Effect |
> |---|---|---|
> | No Bedrock access at all | Deploy with `EnableFailureAnalysis=false` | `bedrock:InvokeModel` is not in the policy. Also set `analyze=false` per call, or every call wastes four AccessDenied attempts before reporting `analysis_unavailable`. |
> | Keep the capability, decide per call | Leave the default, pass `analyze=false` | Nothing is sent unless a caller explicitly asks for analysis. |
> | Keep the analysis, keep data in-Region | Set `BEDROCK_REGION`, **and** pin a non-`us.` `BEDROCK_MODEL_ID` | Inference stays in the Region you name. Confirm the model you pin is available there — an unreachable pinned id fails loudly rather than silently falling back. |
> | Local stdio mode | Nothing to configure | No Lambda role is involved; the call is made under your own credentials, and the same `analyze` / `BEDROCK_*` controls apply. |
>
> **You lose nothing by turning it off.** Everything else in that response — the failures,
> the task logs, the hidden failed tasks that a green `RunState` masked — is gathered
> without a model, is complete on its own, and is the authoritative source. The AI step
> summarises findings you already have. Review this before use if you have data-residency
> obligations.

Model selection:

- Default is a fallback chain, tried in order:
  `us.anthropic.claude-haiku-4-5-20251001-v1:0` → `us.amazon.nova-lite-v1:0` →
  `amazon.nova-lite-v1:0` → `amazon.nova-micro-v1:0`.
- Pin one with the `BEDROCK_MODEL_ID` environment variable.
- The response reports `analysis_model` so you know which one answered, and
  `analysis_unavailable` (with the reason) when none could be invoked.

It uses the Bedrock **Converse** API, which takes the same request shape for every
provider, so switching between Anthropic and Nova models needs no code change.

There is a chain rather than one id because Bedrock retires models. The previously
hardcoded `anthropic.claude-3-haiku-20240307-v1:0` has since reached end of life and now
returns `ResourceNotFoundException`, which disabled this feature without any visible
error. If no model can be invoked the tool now says so explicitly instead of returning a
sentence that reads like a finding.

## A green run does not mean every task passed

Verified against the live service: a task raised `NoSuchBucket`, a downstream `trigger_rule: all_done` task then succeeded, and the run reported **`RunState: SUCCESS`**.

Filtering on `RunState == FAILED` therefore misses real breakage. Two tools address this:

- **`mwaa_verify_run_tasks`** reads each task's CloudWatch log stream and reports the authoritative `final_state`, the exception type and message, and a `discrepancy` field when the run claims SUCCESS but tasks failed. Call it after every run.
- **`mwaa_get_failed_runs`** inspects SUCCESS runs for failed tasks by default (`include_hidden_failures`).

Log group is derived from the **ARN**, not the name returned by `ListWorkflows`: the ARN carries the random suffix the log group uses (`my-wf` → `/aws/mwaa-serverless/my-wf-a1b2c3d4e5/`). Streams are `workflow_id=…/run_id=…/task_id=…/attempt=N.log`.

## Tools

### Reference

| Tool | Description |
|---|---|
| `get_serverless_overview` | Read first. How the service works, the YAML schema, the authoring policy. |
| `get_dag_yaml_spec` | Authoritative schema with a worked example and the exact error each mistake produces. |
| `get_serverless_constraints` | Full reference: Jinja, DAG/task params, `default_args` allowlist, quotas, observability. |
| `list_supported_operators` | Every allowlisted operator with its fully qualified path. |
| `describe_operator` | One operator: FQN, required arguments, XCom output. |
| `suggest_operator` | Find an operator from a plain-language description. |
| `get_server_config` | Effective configuration, where each value came from, and how to change the model. |

### Authoring

| Tool | Description |
|---|---|
| `list_pipeline_steps` | Catalog of composable pipeline operations. |
| `plan_pipeline` | Turn requested operations into a plan + questions to ask + what was left out. |
| `build_dag_yaml` | **Preferred.** Correct-by-construction YAML from a structured task list. |
| `validate_dag_yaml` | Validate against the real schema: shape, operators, required args, XCom reachability, cycles, Jinja, size. |
| `repair_dag_yaml` | Auto-fix the mechanical mistakes and report every change. |
| `preflight_dag_yaml` | Have the service itself validate, then clean up. |
| `generate_execution_role` | Least-privilege IAM role scoped to the operators used. |
| `generate_dag_yaml` | Demo workflow for one service. **Check `params_you_must_set`** — only ~half are fully self-contained. Not a pipeline starting point. |
| `compose_dag_yaml_tool` | Chain several service demo blocks. |
| `get_service_tasks_tool` | Inspect one service's demo block. |
| `analyze_python_dag_tool` | Compatibility analysis of a Python DAG. |
| `convert_python_to_yaml_tool` | Convert a Python DAG to validated YAML. |

> **On the demo templates.** 15 of the 29 are fully self-contained and run as emitted:
> `s3`, `lambda`, `bedrock`, `redshift`, `rds`, `dms`, `neptune`, `glacier`, `appflow`,
> `quicksight`, `dynamodb`, `opensearch_serverless`, `emr_serverless`, `eventbridge`,
> `cloudformation`. The other 14 need an identifier their own stack generates — a
> `!Ref`'d bucket name, a `!GetAtt` role ARN — which cannot be known before the stack
> exists, because `CloudFormationCreateStackOperator` returns `None` via XCom. Those come
> back as params with `REPLACE_ME` defaults listed in `params_you_must_set`. Fill them in,
> or the DAG will provision its stack, fail the work task on the placeholder, and tear the
> stack back down. They are also **not** production starting points: they create and
> destroy real infrastructure. Use `plan_pipeline` + `build_dag_yaml` for a real pipeline.

### Python / Bash code

| Tool | Description |
|---|---|
| `get_code_bundle_guidance` | Packaging rules, worker environment, pre-installed packages, network limits. |
| `build_code_bundle` | Zip modules and scripts into a deployable bundle, with static checks. |
| `check_dag_code_consistency` | Verify every `python_callable` and script reference resolves. |

### Operations

| Tool | Description |
|---|---|
| `mwaa_deploy_and_run` | Upload definition + code bundle, create/update, start a run. Validates first. |
| `mwaa_verify_run_tasks` | **Authoritative per-task outcome from CloudWatch.** Call after every run. |
| `mwaa_poll_run` | Poll to a terminal state. |
| `mwaa_get_failed_runs` | Scan for failures incl. green runs hiding failed tasks; Bedrock root-cause analysis. |
| `mwaa_get_run_status` | Run status with per-task detail. |
| `mwaa_list_workflows` / `mwaa_get_workflow` | List, or full detail incl. the deployed YAML and a validation check. |
| `mwaa_get_workflow_summary` / `mwaa_bulk_status` | Compact status views. |
| `mwaa_start_run` / `mwaa_stop_run` / `mwaa_list_runs` | Run control. |
| `mwaa_redeploy` | Update YAML and rerun, reusing the existing S3 location and role. |
| `mwaa_compare_versions` | Diff the latest two versions. |
| `mwaa_find_workflows_by_service` | Find workflows using a service by inspecting their YAML. |
| `mwaa_delete_workflows` | Bulk delete by pattern or inactivity. Dry run by default. |

## Configuration

Settings resolve from three layers, highest precedence first:

1. **Environment variable** — takes effect immediately, no rebuild. Best for the deployed Lambda.
2. **JSON config file** — the layer a human edits. Best for local use.
3. **Built-in default** — nothing has to be configured.

The config file is looked up in this order, first hit wins:

| Path | Use |
|---|---|
| `$MWAA_MCP_CONFIG` | explicit path |
| `src/mcp_config.json` | next to the source (git-ignored) |
| `~/.mwaa-serverless-mcp/config.json` | per-user, survives `git clean` |

```bash
cd serverless/mcp/src
cp mcp_config.example.json mcp_config.json   # then edit
```

| Key | Env var | Default | Purpose |
|---|---|---|---|
| `bedrock_model_id` | `BEDROCK_MODEL_ID` | `null` | Pin ONE model. Disables the fallback chain. |
| `bedrock_model_candidates` | `BEDROCK_MODEL_CANDIDATES` | 4-model chain | Tried in order until one responds. |
| `bedrock_max_tokens` | `BEDROCK_MAX_TOKENS` | `2000` | Analysis response budget. |
| `bedrock_region` | `BEDROCK_REGION` | `null` | Bedrock-only Region override. |
| `default_poll_seconds` | `MWAA_MCP_DEFAULT_POLL_SECONDS` | `90` | Single `mwaa_poll_run` wait. |
| `max_poll_seconds` | `MWAA_MCP_MAX_POLL_SECONDS` | `110` | Hard cap; keep under the Lambda timeout. |
| `log_level` | `LOG_LEVEL` | `INFO` | |

Run the **`get_server_config`** tool to see effective values, the provenance of each, and
any `problems`. An unparseable file, unknown key or non-numeric integer is reported there
and the default is kept — nothing fails silently.

### Changing the model

Only one tool uses a model (see [Where a model is involved](#where-a-model-is-and-is-not-involved)).

**Locally** — edit `src/mcp_config.json`, restart the MCP server:

```json
{ "bedrock_model_id": "us.anthropic.claude-haiku-4-5-20251001-v1:0" }
```

or set it in your MCP client's `env` block, which avoids a file entirely:

```json
"env": { "BEDROCK_MODEL_ID": "us.amazon.nova-lite-v1:0" }
```

**Deployed** — either uncomment the line in `template.yaml` and redeploy:

```yaml
      Environment:
        Variables:
          BEDROCK_MODEL_ID: us.anthropic.claude-haiku-4-5-20251001-v1:0
```

or change it in the Lambda console for immediate effect with no rebuild:

```bash
aws lambda update-function-configuration \
  --function-name <McpFunction> \
  --environment 'Variables={LOG_LEVEL=INFO,BEDROCK_MODEL_ID=us.amazon.nova-lite-v1:0}'
```

Find ids you can actually call — most current models need a `us.`-prefixed cross-Region
inference profile rather than direct on-demand invocation:

```bash
aws bedrock list-inference-profiles \
  --query 'inferenceProfileSummaries[?status==`ACTIVE`].inferenceProfileId' --output text
```

Pinning a model disables the fallback chain, so an unreachable id fails loudly with
`analysis_unavailable` instead of quietly answering from a different model.

## Prerequisites

| | Local stdio | Deployed Function URL |
|---|---|---|
| Python | 3.10+ | — (Lambda runs 3.12) |
| `boto3` | `~=1.40` (see `src/requirements.txt`) | installed by `sam build` |
| AWS SAM CLI | — | 1.100+ |
| AWS CLI | v2, for the role/IAM commands | v2 |
| `uv` / `uvx` | — | needed for `mcp-proxy-for-aws` |
| AWS credentials | any principal that can call MWAA Serverless | a principal that can create IAM roles and Lambda functions (`CAPABILITY_IAM`) |

`sam build` for this stack is a plain Python zip build and does **not** require Docker.
Install `uv` (which provides `uvx`) from
[docs.astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/).

**Region.** MWAA Serverless is not available everywhere, and this sample defaults to
`us-east-1` in `samconfig.toml` — a value you should change rather than inherit. The
Region must be one where MWAA Serverless is offered and where your workflows live. Bedrock
is configured separately via `BEDROCK_REGION`, so the two do not have to match.

## Running it

**Run it locally unless you specifically need a shared endpoint.** That is a security
recommendation, not a convenience one, and it is the reason local stdio is first below.

In local mode every AWS call is made with *your* credentials, so the server can do exactly
what you can do and nothing more. There is no endpoint, no resource policy, and no shared
role. If the tools misbehave, the damage is bounded by permissions you already hold, and
CloudTrail attributes every call to you by name.

Deploying to Lambda changes that in a way no amount of code can undo. The function needs
`CreateWorkflow`, `s3:PutObject` and `iam:PassRole` to do its job, and it exposes tools
that build a code bundle and deploy a workflow containing arbitrary Python or Bash.
Composed, **anyone who can invoke the endpoint can run code under any role the function
can pass** — including roles more privileged than the function itself. Every caller shares
one execution role, so CloudTrail shows the role rather than the person. Deploy it when a
team or a hosted agent genuinely needs shared access, with the parameters set and the
invoke permission granted to one dedicated principal, and read
[Security considerations for a remote deployment](#security-considerations-for-a-remote-deployment)
first.

| | Local stdio | Deployed Function URL |
|---|---|---|
| Network exposure | none | HTTPS endpoint |
| Auth | your OS user | AWS IAM (SigV4) |
| Calls run as | **you** | the **Lambda role**, shared by all callers |
| Blast radius | your own permissions | the function's permissions, for every caller |
| Attribution in CloudTrail | your principal | the execution role |
| Arbitrary code execution | under your own identity | under any passable role |
| Setup | `pip install` | `sam deploy` + IAM parameters |
| Model/config changes | edit a file | env var or redeploy |
| Best for | **individual development — start here** | shared team or hosted agent |

### Option 1 — local stdio (recommended)

Runs as a child process of your MCP client, like any other local MCP server. No endpoint,
no URL to leak, no authorizer. Every AWS call is made as *you*, so the blast radius is
exactly your own permissions.

```bash
cd serverless/mcp/src
python3 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements-local.txt
python local_server.py               # optional smoke test; Ctrl-C to stop
```

It logs the resolved AWS identity and Region to stderr on startup, so a credential or
Region mistake is obvious immediately. There is no AWS deployment — `sam build` and
`sam deploy` are not used in this mode, and nothing in `template.yaml` applies.

Requires Python 3.10+ (the `mcp` package) and `boto3 >= 1.40`.

Two consequences of running as yourself are worth knowing. Use least-privilege
credentials, because the tools inherit whatever you hold — an admin session makes the
delete tool as dangerous as your own console. And the inline code-bundle ceiling does not
apply here: there is no request payload to fit inside, so the MWAA service quota is the
only limit.

### Option 2 — deploy to Lambda with a Function URL

Only worth doing when something other than your own machine needs to reach the server.
Everything in
[Security considerations for a remote deployment](#security-considerations-for-a-remote-deployment)
applies.

```bash
cd serverless/mcp
sam build
sam deploy --guided     # first time; afterwards just: sam deploy
```

`WorkflowBucketName` is **required and has no default** — `sam deploy` fails until you
supply it. That is deliberate: it used to accept an empty value and fall back to granting
S3 access across every bucket in the account, so the easiest deploy produced the widest
permissions.

`samconfig.toml` pins `stack_name`, `region = "us-east-1"` and
`capabilities = "CAPABILITY_IAM"`, so a plain `sam deploy` uses those — **change the
region there or pass `--region`** rather than silently deploying to us-east-1.

Take `McpFunctionUrl` from the Outputs, and check `S3AccessScope` in the same Outputs to
confirm which bucket the function is scoped to. The transport is a **Lambda Function URL
with `AuthType: AWS_IAM`** — there is no API Gateway.

Redeploy after changing code:

```bash
sam build && sam deploy
```

Tear down completely:

```bash
sam delete --stack-name mwaa-serverless-mcp
```

Callers must SigV4-sign, and no MCP client does that natively, so use AWS's signing proxy
(`mcp-proxy-for-aws`). Grant callers this and nothing broader — the `InvokePolicyHint`
output prints it with your function ARN already filled in:

```json
{ "Effect": "Allow", "Action": "lambda:InvokeFunctionUrl", "Resource": "<McpFunctionArn>" }
```

The proxy examples below pin an exact version. It runs in a process that receives your AWS
profile, so resolving it at `@latest` on every launch means a new release can start signing
your requests without review. Check for newer versions deliberately:
`uvx pip index versions mcp-proxy-for-aws`.

## Security considerations for a remote deployment

None of this applies to local stdio, where the tools run under your own credentials and
there is no endpoint. It all applies the moment you `sam deploy`.

### Endpoint authentication

- **`AuthType: AWS_IAM` is on by default.** Unsigned requests get `403 Forbidden`; SigV4
  signed requests get `200`. No anonymous path exists — with `AuthType: NONE`, SAM would
  add a `lambda:InvokeFunctionUrl` permission with `Principal: *`, and that permission is
  absent.
- **The URL is not a secret.** It appears in CloudFormation outputs, MCP config files,
  shell history and CI logs. Authentication, not obscurity, is what protects it.
- **IAM auth does not separate privileged principals.** `lambda:InvokeFunctionUrl` is
  included in `AdministratorAccess` and `PowerUserAccess`, so in an account where everyone
  is an admin this blocks the internet but not your colleagues. Grant a dedicated invoke
  role if you need that distinction:

  ```bash
  aws iam create-role --role-name mcp-invoker \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
      "Principal":{"AWS":"arn:aws:iam::<ACCOUNT>:user/<YOU>"},"Action":"sts:AssumeRole"}]}'
  aws iam put-role-policy --role-name mcp-invoker --policy-name invoke \
    --policy-document "$(aws cloudformation describe-stacks \
      --stack-name mwaa-serverless-mcp \
      --query 'Stacks[0].Outputs[?OutputKey==`InvokePolicyHint`].OutputValue' \
      --output text | python3 -c 'import json,sys; print(json.dumps({"Version":"2012-10-17","Statement":[json.load(sys.stdin)]}))')"
  ```
- **Signing needs live credentials.** Static temporary credentials in environment
  variables stop working at expiry; a profile with SSO or role refresh does not.

### The privilege chain is the whole risk

```text
lambda:InvokeFunctionUrl
  -> the Lambda execution role
  -> s3:PutObject (workflow definition + code bundle)
  -> airflow-serverless:CreateWorkflow / UpdateWorkflow
  -> iam:PassRole
  -> your Python or Bash running under the passed role
```

Granting the first link grants the last. `AuthType: AWS_IAM` stops anonymous access and
nothing else — an authorized caller reaches the end of that chain by design, because
deploying workflows that run code *is* the function's job. Treat
`lambda:InvokeFunctionUrl` on this function as equivalent to handing over the passable
roles, and grant it to one dedicated principal rather than to everyone with broad
administrator access.

### Constrain every link

| Parameter | Default | What it limits |
|---|---|---|
| `WorkflowBucketName` | **required** | S3 reads and writes to one bucket, in this account |
| `PassableExecutionRolePath` | `mwaa-serverless-*` | which roles can be passed. Narrow it to exact role names. |
| `AllowWorkflowDeletion` | `false` | removes `DeleteWorkflow` for the delete tool |
| `EnableFailureAnalysis` | `true` | removes `bedrock:InvokeModel` when false |
| `ReservedConcurrentExecutions` | `5` | caps runaway cost and abuse rate |

`PassableExecutionRolePath` is the one that matters most. It defaults to a name prefix,
which is a real constraint only if no over-privileged role in the account happens to match
it. Auditing that is worth more than tightening any other parameter. Never widen it to
`*`: the `iam:PassedToService` condition limits which *service* receives the role, not
which role can be passed, so `*` makes every role in the account passable — including an
`AdministratorAccess` role.

Consider a permission boundary on the roles this function can pass, so a later
configuration mistake cannot turn one of them into an administrator.

### What this sample deliberately does not do

Be aware of these before treating a deployment as production infrastructure:

- **One role for every tool.** Read-only discovery, deployment and deletion all execute
  under the same execution role, and there is no per-caller or per-tool authorization in
  the dispatch path. A production build should split the tool surface across separate
  functions and roles — read-only, start/stop, deploy, administer — so that reaching the
  endpoint does not confer everything at once. That is a redesign, not a parameter.
- **Attribution stops at the role.** Downstream CloudTrail events name the execution role,
  not the caller who triggered them. If you need to know who deployed what, log the
  authenticated principal per tool call before dispatch, or use local stdio where every
  call is already attributed.
- **Arbitrary code arrives inline.** `mwaa_deploy_and_run` accepts a base64 code bundle
  from the caller. A stricter pattern is to have CI place immutable artifacts in a fixed
  S3 prefix and pass only `code_s3_key`, so no code travels through the endpoint.
- **Deployment is all-at-once.** There is no alias, canary or alarm-gated rollback, so a
  broken release takes all traffic immediately. Add `AutoPublishAlias` and a
  `DeploymentPreference` if this becomes something you depend on.
- **No idempotency tokens.** A retried or duplicated request can overwrite a definition
  and start a second run. `mwaa_deploy_and_run` also turns a name conflict into an update
  of the existing workflow.

### Data flow

`mwaa_get_failed_runs` sends CloudWatch task log excerpts to Bedrock by default, and the
default model chain leads with cross-Region inference profiles, so that content may be
processed outside your Region. Credential-shaped values are redacted first, but resource
names, ARNs and account ids are not — they are the diagnostic content. The four ways to
control this are in the table under [Where a model is (and is not)
involved](#where-a-model-is-and-is-not-involved); `EnableFailureAnalysis=false` is the
only one that removes the permission.

Treat the returned analysis as an untrusted suggestion. Task logs are arbitrary text
written by whatever runs in a task, so they can contain instructions aimed at the model or
at the agent reading its reply. The response labels it accordingly, and the `failures`
list is the authoritative evidence.

### Tools that mutate, delete or cost money

These matter in both transports, but a shared endpoint means someone else can invoke them.

- **`mwaa_delete_workflows` targets EVERY workflow in the account when called with no
  filter** — not just the ones this server created. It previews by default, and an
  unfiltered non-dry-run call is **refused**: deleting everything requires
  `confirm_delete_all=true`. Always pass `name_contains` or `not_run_in_days`, and read the
  dry-run list before passing `dry_run=false`.
- **Three tools mutate or bill.** `mwaa_deploy_and_run` **updates** an existing workflow of
  the same name and starts a billable run; `mwaa_redeploy` overwrites the deployed
  definition and the S3 object behind it, then reruns; `mwaa_stop_run` kills an in-flight
  run. All three require an **exact** workflow name — a partial match is refused, because
  overwriting the wrong workflow is unrecoverable.
- **`preflight_dag_yaml` creates and deletes a throwaway workflow** to obtain the service's
  own verdict. It occupies one of the 100 workflow slots for a few seconds and writes then
  deletes two objects in the bucket you name. The delete is permitted even when
  `AllowWorkflowDeletion=false`, via a grant scoped to the `preflight-*` name prefix —
  withholding it would not make preflight safe, it would make it leak one workflow per
  call. The CloudWatch log group the service creates for the throwaway does outlive it and
  is named in `log_group_residue`.
- **All S3 writes use SSE-S3 and assert bucket ownership.** On a deployment the owning
  account comes from the stack, so a caller cannot substitute its own claim. Locally there
  is no stack, so pass `expected_bucket_owner` on `mwaa_deploy_and_run`, `mwaa_redeploy`
  and `preflight_dag_yaml` yourself.

### Local files

**Never commit `src/mcp_config.json`.** It is git-ignored. Keep secrets out of it
regardless — it holds configuration, not credentials.

### Why there is no API Gateway

It contributed nothing: one route with no authorizer in front of one Lambda. It also could
not be secured in place — `AWS::Serverless::HttpApi` cannot express IAM authorization at
all. `Auth: DefaultAuthorizer: AWS_IAM` is a REST API feature and SAM rejects it on an HTTP
API with *"Unable to set DefaultAuthorizer because 'AWS_IAM' was not defined in
'Authorizers'"*. Removing it also lifted two constraints:

- **Resource-level controls.** HTTP APIs do not support resource policies, so restricting
  by source IP or caller principal was impossible. A Function URL is governed by ordinary
  IAM.
- **The 29-second ceiling.** API Gateway capped every request at 29s, which is why
  `mwaa_poll_run` had to give up after 25s and be called repeatedly. The Lambda timeout is
  now 120s and a single poll waits up to 110s, so most task transitions finish in one call.

## One Region per running server

The server resolves its AWS Region once, when the process starts, and every workflow,
S3 and CloudWatch Logs call uses that Region for the life of the process. **No tool takes
a Region argument.** To work in a different Region you change the environment and restart
the server — in an MCP client, that means editing the server's `env` block and restarting
the client so the subprocess is respawned.

This matters more than it sounds, because a wrong Region is not an error. An empty
workflow list from the wrong Region looks exactly like an empty workflow list from the
right one. So:

- `get_server_config` reports `aws_region` — the effective Region, where it came from,
  and a reminder that it is fixed for the process. Check it before concluding that a
  workflow is missing.
- Passing an unsupported argument such as `region` is **rejected**, not ignored. The
  error names the accepted arguments and points at `get_server_config`.

### Set `AWS_DEFAULT_REGION`, not `AWS_REGION`

botocore resolves the session Region from `AWS_DEFAULT_REGION` only —
`botocore/configprovider.py` maps `'region'` to `('region', 'AWS_DEFAULT_REGION', None, None)`.
`AWS_REGION` is **not** consulted, so setting only that leaves the Region coming from your
profile, silently, in whatever Region that happens to be:

```bash
AWS_REGION=eu-west-1 python -c "import boto3; print(boto3.Session().region_name)"
# -> us-east-1   (the profile's Region; AWS_REGION ignored)

AWS_DEFAULT_REGION=eu-west-1 python -c "import boto3; print(boto3.Session().region_name)"
# -> eu-west-1
```

It works in Lambda because the runtime sets both. Locally it does not. `get_server_config`
reports `ignored_aws_region` when `AWS_REGION` is set but not taking effect, and the server
logs a warning at startup.

## Client configuration

Replace absolute paths and the Function URL with your own. Both transports expose the same
38 tools with identical descriptions.

### Kiro

`.kiro/settings/mcp.json` in the project, or `~/.kiro/settings/mcp.json` globally.

Local:

```json
{
  "mcpServers": {
    "mwaa-serverless": {
      "command": "/abs/path/serverless/mcp/src/.venv/bin/python",
      "args": ["/abs/path/serverless/mcp/src/local_server.py"],
      "env": {
        "AWS_PROFILE": "your-profile",
        "AWS_DEFAULT_REGION": "us-east-1",
        "BEDROCK_MODEL_ID": "us.anthropic.claude-haiku-4-5-20251001-v1:0"
      },
      "disabled": false
    }
  }
}
```

Remote:

```json
{
  "mcpServers": {
    "mwaa-serverless": {
      "command": "uvx",
      "args": ["mcp-proxy-for-aws==1.7.0", "https://<id>.lambda-url.us-east-1.on.aws/"],
      "env": { "AWS_PROFILE": "your-profile", "AWS_DEFAULT_REGION": "us-east-1" },
      "disabled": false
    }
  }
}
```

### Claude Desktop

`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS,
`%APPDATA%\Claude\claude_desktop_config.json` on Windows. Restart Claude after editing.

Local:

```json
{
  "mcpServers": {
    "mwaa-serverless": {
      "command": "/abs/path/serverless/mcp/src/.venv/bin/python",
      "args": ["/abs/path/serverless/mcp/src/local_server.py"],
      "env": { "AWS_PROFILE": "your-profile", "AWS_DEFAULT_REGION": "us-east-1" }
    }
  }
}
```

Remote — same shape as Kiro's remote block above. Claude Desktop launches a subprocess, so
it does not inherit your shell environment: set `AWS_PROFILE` and `AWS_DEFAULT_REGION` explicitly,
and make sure `uvx` is on the PATH the desktop app sees (an absolute path is safest).

### Claude Code

```bash
# local
claude mcp add mwaa-serverless \
  --env AWS_PROFILE=your-profile --env AWS_DEFAULT_REGION=us-east-1 \
  -- /abs/path/serverless/mcp/src/.venv/bin/python /abs/path/serverless/mcp/src/local_server.py

# remote
claude mcp add mwaa-serverless \
  --env AWS_PROFILE=your-profile --env AWS_DEFAULT_REGION=us-east-1 \
  -- uvx mcp-proxy-for-aws==1.7.0 https://<id>.lambda-url.us-east-1.on.aws/
```

### Cursor / Windsurf / other stdio clients

`.cursor/mcp.json` (or the client's equivalent) takes the same `command` / `args` / `env`
shape as the Kiro examples.

### Verifying the connection

Ask the agent to call `get_server_config`. It returns the effective configuration and
confirms the transport is reachable. `list_supported_operators` is a good second check —
it needs no AWS credentials, so a failure there points at the MCP wiring rather than IAM.

## What this sample costs

The [Cost](#cost-dont-pay-for-waiting) section above is about the DAGs you author. This is
about running the server itself. Nothing here is free-tier guaranteed; see the
[MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/pricing/),
[Lambda](https://aws.amazon.com/lambda/pricing/),
[S3](https://aws.amazon.com/s3/pricing/),
[CloudWatch](https://aws.amazon.com/cloudwatch/pricing/) and
[Bedrock](https://aws.amazon.com/bedrock/pricing/) pricing pages for current rates.

- **Local stdio mode costs nothing to run.** Only the AWS calls it makes are billed.
- **Lambda:** 512 MB, and a single `mwaa_poll_run` can occupy the function for up to 110
  seconds. `ReservedConcurrentExecutions: 5` bounds the worst case.
- **MWAA Serverless is the real cost.** `preflight_dag_yaml`, `mwaa_deploy_and_run` and
  `mwaa_redeploy` create **real** workflows and start **real** runs, billed for the time
  each task occupies a worker.
- **S3:** definitions are small; code bundles can be up to 250 MB each (but see the inline limit below — a bundle passed through a deployed endpoint is capped far lower).
- **CloudWatch Logs:** ingestion and storage for your workflows' task logs, plus this
  function's own logs (retention is set by the `LogRetentionDays` parameter, default 30 —
  an implicitly created Lambda log group would never expire).
- **Bedrock:** per-token, and **on by default** (`EnableFailureAnalysis=true` plus
  `analyze=true`). `mwaa_get_failed_runs` fans out across workflows, so one call can mean
  one inference per scan with up to `bedrock_max_tokens` (2000) of output. Deploy with
  `EnableFailureAnalysis=false`, or pass `analyze=false`, if you would rather not pay for
  it — the log-based findings are unaffected.
- **The demo templates provision real infrastructure** — EMR clusters, RDS instances,
  Redshift workgroups, EKS clusters. They tear it down again, but they bill while running,
  and a failed teardown leaves it running. Check with `mwaa_verify_run_tasks`.

## Cleaning up

`sam delete` removes the server. It does **not** remove anything the tools created, so do
these too:

```bash
# 1. Workflows this server created (ALWAYS review the dry run first)
#    Called with no filter this targets every workflow in the account — pass a filter.
#    Via your MCP client: mwaa_list_workflows, then
#                         mwaa_delete_workflows(name_contains="<your-prefix>")

# 2. The definitions and code bundles in your own bucket
aws s3 rm "s3://<your-workflow-bucket>/workflows/" --recursive
aws s3 rm "s3://<your-workflow-bucket>/preflight/" --recursive   # only if preflight left any

# 3. Task log groups — these retain and bill indefinitely
#    Note: each preflight_dag_yaml call also leaves one empty log group behind
#    (the service creates it for the throwaway workflow and it outlives it). Empty
#    groups store nothing and cost nothing, but they accumulate — the response's
#    `log_group_residue` names each one.
aws logs describe-log-groups --log-group-name-prefix /aws/mwaa-serverless/ \
  --query 'logGroups[].logGroupName' --output text \
  | tr '\t' '\n' | xargs -I{} aws logs delete-log-group --log-group-name {}

# 4. Any execution role you created from generate_execution_role
aws iam delete-role-policy --role-name mwaa-serverless-<dag_id>-role --policy-name <dag_id>-policy
aws iam delete-role --role-name mwaa-serverless-<dag_id>-role

# 5. Any CloudFormation stacks a demo DAG left behind after a failed teardown
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE DELETE_FAILED \
  --query "StackSummaries[?contains(StackName,'mwaa-')].StackName"

# 6. The server itself, and the SAM artifact bucket (resolve_s3 = true created it)
sam delete --stack-name mwaa-serverless-mcp
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Unknown service: 'mwaa-serverless'` | boto3 too old | `pip install 'boto3>=1.40'`. The legacy `airflow-serverless` service resolves to a non-existent host, so it is deliberately not used as a fallback. |
| `403 Forbidden` from the Function URL | request not signed, or caller lacks `lambda:InvokeFunctionUrl` | Use `mcp-proxy-for-aws`; grant the invoke policy above. |
| `The 'mcp' package is required for local stdio mode` | wrong requirements file | `pip install -r requirements-local.txt` |
| `analysis_unavailable` in `mwaa_get_failed_runs` | no invocable Bedrock model | Set `bedrock_model_id` to an active id; enable model access in the Region. Log-based failure detail is unaffected. |
| Tools work, AWS calls fail | credentials or Region | `get_server_config`, and check the identity line `local_server.py` logs to stderr. |
| Run says `SUCCESS` but nothing happened | a trailing `all_done` task masked an earlier failure | `mwaa_verify_run_tasks` — see [A green run does not mean every task passed](#a-green-run-does-not-mean-every-task-passed). |
| `Invalid tasks configuration` on deploy | `tasks` written as a list | `repair_dag_yaml`, or use `build_dag_yaml`. |

## Quotas

| Resource | Limit |
|---|---|
| Workflows per account | 100 |
| Versions per workflow | 50 |
| Concurrent runs per account / per workflow | 100 / 20 |
| XCom value | 100 KB |
| DAG definition | 50 KB |
| Code bundle (MWAA service quota) | 250 MB (75 GB per account) |
| Code bundle passed **inline** to a deployed server | ~4.5 MB — a Lambda request is capped at 6 MB and base64 inflates by a third. Upload to S3 and pass `code_s3_key` instead. No such limit in local stdio mode. |
| Task execution timeout | 60 minutes |
| Retries per task | 0–3 |
| Retry delay | 0–300 seconds |

## Architecture

```
serverless/mcp/
├── template.yaml           # SAM template (Lambda + IAM-auth Function URL)
├── samconfig.toml
├── README.md
└── src/
    ├── app.py              # MCP tool definitions (shared by both transports)
    ├── local_server.py     # Local stdio entrypoint — no network exposure
    ├── config.py           # env var > config file > default resolution
    ├── constraints.py      # Verified ground truth: schema, quotas, authoring policy
    ├── schema.py           # Operator allowlist, required args, XCom outputs
    ├── validator.py        # Schema validation + auto-repair
    ├── builder.py          # Pipeline planning + correct-by-construction assembly
    ├── codebundle.py       # Python/Bash code bundles
    ├── tools.py            # Demo templates, IAM role generation
    ├── operations.py       # Workflow deploy/run/inspect, per-task verification
    ├── python_analyzer.py  # Python DAG compatibility analysis
    ├── python_converter.py # Python-to-YAML conversion
    ├── mcp_config.example.json # copy to mcp_config.json and edit
    ├── requirements.txt        # Lambda runtime deps (pinned)
    └── requirements-local.txt  # adds `mcp` for local stdio mode
├── pytest.ini
├── ruff.toml
├── verify_generated_policies.py  # simulates generated IAM against live IAM (needs creds)
└── tests/                  # no test calls AWS
    ├── conftest.py
    ├── test_validator.py            # schema rules + untrusted-input bounds
    ├── test_python_migration.py     # conversion fidelity, nothing dropped silently
    ├── test_builder_and_schema.py   # correct-by-construction + cross-module invariants
    ├── test_security.py             # IAM output, demo templates, destructive ops
    └── test_tool_surface.py         # both transports expose the same 38 tools
```

## Lambda IAM permissions

Almost every statement is ARN-scoped to this account and Region. Three actions use
`Resource: '*'` because the API cannot scope them — they are collection-level operations,
all read-or-create, none destructive:

| Action | Why it cannot be scoped |
|---|---|
| `airflow-serverless:ListWorkflows` | Enumerates the account; there is no per-item ARN to name |
| `airflow-serverless:CreateWorkflow` | The workflow does not exist yet, so there is nothing to name |
| `logs:DescribeLogGroups` | Enumerates log groups; it does not read one |

This was found by deploying, not by linting: scoped to `workflow/*` these evaluate to
`implicitDeny` (confirm with `aws iam simulate-principal-policy`), so every
name-resolving tool failed with `AccessDeniedException` at run time while `cfn-lint` and
`sam build` both reported success. A test now enforces that the wildcard list stays
exactly these three and that nothing destructive joins it.

| Permission | Resource scope | Purpose |
|---|---|---|
| `airflow-serverless:ListWorkflows`, `CreateWorkflow` | `*` — collection-level, see above | Discover workflows; create one |
| `airflow-serverless:GetWorkflow`, `GetWorkflowRun`, `ListWorkflowRuns`, `ListWorkflowVersions` | `…:workflow/*` in this account+Region | Read a workflow and its runs |
| `airflow-serverless:UpdateWorkflow`, `StartWorkflowRun`, `StopWorkflowRun` | `…:workflow/*` in this account+Region | Deploy and run |
| `airflow-serverless:DeleteWorkflow` | `…:workflow/preflight-*` — **always granted** | Lets preflight delete its own throwaway. Withholding it would leak one workflow per call. |
| `airflow-serverless:DeleteWorkflow` | `…:workflow/*` — **omitted unless `AllowWorkflowDeletion=true`** | The delete tool |
| `s3:GetObject/PutObject/DeleteObject` | `WorkflowBucketName/*`, with `s3:ResourceAccount` | Definitions and code bundles; preflight cleanup |
| `s3:ListBucket/GetBucketLocation` | `WorkflowBucketName`, with `s3:ResourceAccount` | Same |
| `logs:DescribeLogGroups` | `*` — collection-level, see above | Find a workflow's log group |
| `logs:DescribeLogStreams`, `logs:GetLogEvents` | `log-group:/aws/mwaa-serverless/*` | Per-task outcome verification |
| `bedrock:InvokeModel` | foundation models + inference profiles in this Region — **omitted when `EnableFailureAnalysis=false`** | Failure root-cause analysis. Granted by default; see the data-flow callout. |
| `iam:PassRole` | `role/${PassableExecutionRolePath}`, **and** `iam:PassedToService: airflow-serverless.amazonaws.com` | Pass the execution role on create |

The two conditions on `iam:PassRole` do different jobs and you need both:
`iam:PassedToService` limits which **service** receives the role; the `Resource` pattern
limits **which role** can be passed. With only the first, any role in the account is
passable — see [Security considerations for a remote deployment](#security-considerations-for-a-remote-deployment).

Callers of the deployed endpoint need only `lambda:InvokeFunctionUrl` on the function.
In local stdio mode there is no endpoint, and the tools use your own credentials, so
none of the above is granted to anyone.

## Tests

```bash
cd serverless/mcp
pip install -r src/requirements-local.txt pytest ruff cfn-lint bandit
python -m pytest
ruff check .
cfn-lint template.yaml
bandit -r src/
```

No test calls AWS. Workflow operations run against a stub client that records destructive
calls, so the tests assert those calls did **not** happen. The suite covers the schema
rules, the untrusted-input bounds, every generated IAM policy, all 29 demo templates
(including that the CloudFormation they embed parses and has no dangling references), and
the cross-module invariants between `schema.py` and `constraints.py`.

These four checks run in CI on any change under `serverless/mcp/`
([.github/workflows/serverless-mcp.yml](../../.github/workflows/serverless-mcp.yml)). The
workflow is path-scoped to this directory, needs no AWS credentials, and is expected to
stay at zero findings rather than carry a baseline of accepted ones.

Passing all four is necessary but not sufficient. Several real defects in this sample were
invisible to every one of them and only surfaced by deploying and running against the
service: an IAM policy that lints clean but denies at run time, an operator that writes no
log stream, a false positive on the documented XCom idiom, and preflight leaking a workflow
on every call because the delete permission it needs was gated off by default. Run a live
workflow before trusting a change to the IAM generator, the log readers, or the validator.

One of those was actively held in place by a test. It asserted that no generated policy may
use `Resource: "*"`, which is precisely the broken form for actions IAM refuses to scope —
so the suite demanded the bug. When a test encodes an absolute, check that the absolute is
true before trusting the green.

`verify_generated_policies.py` closes part of that gap by asking IAM itself:

```bash
python verify_generated_policies.py          # all 29 demo policies
python verify_generated_policies.py ec2 s3   # or just these
```

It uses `iam:SimulateCustomPolicy`, which evaluates a policy document against the real
authorisation engine without attaching it to anything — no role, policy or resource is
created. It checks both directions: an action scoped to an ARN that IAM would deny, and an
action granted `"*"` that an ARN would in fact have authorised. It needs credentials, so it
is not part of CI.

## License and contributing

This sample is released under the **MIT-0** license — see [LICENSE](../../LICENSE).
Contribution guidance is in [CONTRIBUTING.md](../../CONTRIBUTING.md), and the code of
conduct is in [CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md).
