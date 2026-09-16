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

1. `plan_pipeline(["glue_job", "glue_job", "glue_crawler", "athena_query"])` — get the operators, the questions to ask, and what was left out.
2. Ask the user those questions.
3. `build_dag_yaml(...)` — correct-by-construction YAML, already validated.
4. `preflight_dag_yaml(...)` — the service validates it for real, then the throwaway workflow is deleted.
5. `generate_execution_role(...)` — least-privilege policy plus CLI commands.
6. `mwaa_deploy_and_run(...)`.
7. `mwaa_poll_run(...)` then **`mwaa_verify_run_tasks(...)`**.

### Fix hand-written or inherited YAML

`validate_dag_yaml` → `repair_dag_yaml` → `preflight_dag_yaml`.

### Migrate a Python DAG

`analyze_python_dag_tool` → `convert_python_to_yaml_tool` → `validate_dag_yaml`. The converter emits mapping-shaped, fully-qualified YAML, converts `timedelta(...)` to the right form, and flags Python/Bash tasks that need a code bundle.

## Where a model is (and is not) involved

36 of the 37 tools are deterministic Python — schema validation, repair, planning, YAML
assembly, code bundling, deploy and run inspection all run without calling a model. Your
own agent supplies the intelligence; this server supplies verified rules and AWS calls.

Exactly one tool calls a model: `mwaa_get_failed_runs` with `analyze=true` sends the
collected failure details and CloudWatch task logs to Amazon Bedrock for root-cause
analysis. Everything else in that response — the failures, the task logs, the hidden
failed tasks — is gathered without a model and stands on its own.

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
| `generate_dag_yaml` | Self-contained demo workflow for one service. Not a pipeline starting point. |
| `compose_dag_yaml_tool` | Chain several service demo blocks. |
| `get_service_tasks_tool` | Inspect one service's demo block. |
| `analyze_python_dag_tool` | Compatibility analysis of a Python DAG. |
| `convert_python_to_yaml_tool` | Convert a Python DAG to validated YAML. |

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

## Running it

Two transports. Pick based on who needs to reach it.

| | Local stdio | Deployed Function URL |
|---|---|---|
| Network exposure | none | HTTPS endpoint |
| Auth | your OS user | AWS IAM (SigV4) |
| Calls run as | **you** | the **Lambda role** |
| Blast radius | your own permissions | the function's permissions |
| Setup | `pip install` | `sam deploy` |
| Model/config changes | edit a file | env var or redeploy |
| Best for | individual development | shared team or hosted agent |

### Option 1 — local stdio (recommended for individual use)

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
`sam deploy` are not used in this mode.

Requires Python 3.10+ (the `mcp` package) and `boto3 >= 1.40`.

### Option 2 — deploy to Lambda with a Function URL

```bash
cd serverless/mcp
sam build
sam deploy --guided     # first time; afterwards just: sam deploy
```

Take `McpFunctionUrl` from the Outputs. The transport is a **Lambda Function URL with
`AuthType: AWS_IAM`** — there is no API Gateway.

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
        "AWS_REGION": "us-east-1",
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
      "args": ["mcp-proxy-for-aws@latest", "https://<id>.lambda-url.us-east-1.on.aws/"],
      "env": { "AWS_PROFILE": "your-profile", "AWS_REGION": "us-east-1" },
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
      "env": { "AWS_PROFILE": "your-profile", "AWS_REGION": "us-east-1" }
    }
  }
}
```

Remote — same shape as Kiro's remote block above. Claude Desktop launches a subprocess, so
it does not inherit your shell environment: set `AWS_PROFILE` and `AWS_REGION` explicitly,
and make sure `uvx` is on the PATH the desktop app sees (an absolute path is safest).

### Claude Code

```bash
# local
claude mcp add mwaa-serverless \
  --env AWS_PROFILE=your-profile --env AWS_REGION=us-east-1 \
  -- /abs/path/serverless/mcp/src/.venv/bin/python /abs/path/serverless/mcp/src/local_server.py

# remote
claude mcp add mwaa-serverless \
  --env AWS_PROFILE=your-profile --env AWS_REGION=us-east-1 \
  -- uvx mcp-proxy-for-aws@latest https://<id>.lambda-url.us-east-1.on.aws/
```

### Cursor / Windsurf / other stdio clients

`.cursor/mcp.json` (or the client's equivalent) takes the same `command` / `args` / `env`
shape as the Kiro examples.

### Verifying the connection

Ask the agent to call `get_server_config`. It returns the effective configuration and
confirms the transport is reachable. `list_supported_operators` is a good second check —
it needs no AWS credentials, so a failure there points at the MCP wiring rather than IAM.

## Security considerations

**Local stdio is the safer default.** There is no listener, and tools act with your own
credentials, so a caller can never exceed what you can already do.

**The deployed Function URL inverts that.** Callers act with the *function's* permissions,
which are broader than most individuals need: `CreateWorkflow`, `UpdateWorkflow`,
`DeleteWorkflow`, `iam:PassRole` and S3 read/write. Treat invoke access as equivalent to
granting all of that.

- **`AuthType: AWS_IAM` is on by default.** Unsigned requests get `403 Forbidden`; SigV4
  signed requests get `200`. No anonymous path exists — with `AuthType: NONE`, SAM would
  add a `lambda:InvokeFunctionUrl` permission with `Principal: *`, and that permission is
  absent.
- **The URL is not a secret.** It appears in CloudFormation outputs, MCP config files,
  shell history and CI logs. Authentication, not obscurity, is what protects it.
- **IAM auth does not separate privileged principals.** `lambda:InvokeFunctionUrl` is
  included in `AdministratorAccess` and `PowerUserAccess`, so in an account where everyone
  is an admin this blocks the internet but not your colleagues. Grant a dedicated invoke
  role if you need that distinction.
- **Scope the function's policy down.** The template uses `Resource: "*"`. Restrict it to
  the workflows and buckets you use, and remove `DeleteWorkflow` if the server never
  deletes.
- **`mwaa_delete_workflows` is destructive and irreversible.** It previews by default;
  confirm the dry-run output before passing `dry_run=false`.
- **Signing needs live credentials.** Static temporary credentials in environment
  variables stop working at expiry; a profile with SSO or role refresh does not.
- **Never commit `src/mcp_config.json`.** It is git-ignored. Keep secrets out of it
  regardless — it holds configuration, not credentials.
- **`preflight_dag_yaml` creates and deletes a throwaway workflow** to obtain the
  service's own verdict. It counts against the 100-workflow quota for a few seconds.

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
| Code bundle | 250 MB (75 GB per account) |
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
    ├── requirements.txt        # Lambda runtime deps
    └── requirements-local.txt  # adds `mcp` for local stdio mode
```

## Lambda IAM permissions

| Permission | Purpose |
|---|---|
| `airflow-serverless:*Workflow*`, `*WorkflowRun*` | Workflow and run management |
| `s3:GetObject/PutObject/DeleteObject/ListBucket` | Definitions and code bundles; preflight cleanup |
| `logs:DescribeLogGroups/DescribeLogStreams/GetLogEvents` | Per-task outcome verification |
| `bedrock:InvokeModel` | Failure root-cause analysis |
| `iam:PassRole` (scoped to `airflow-serverless.amazonaws.com`) | Pass the execution role on create |

Callers of the deployed endpoint need only `lambda:InvokeFunctionUrl` on the function.
In local stdio mode there is no endpoint, and the tools use your own credentials, so
none of the above is granted to anyone.
