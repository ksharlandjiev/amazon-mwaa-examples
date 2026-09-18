## Disclaimer

AWS code samples are example code that demonstrates practical implementations of AWS services for specific use cases and scenarios.

These application solutions are not supported products in their own right, but educational examples to help our customers use our products for their applications. As our customer, any applications you integrate these examples into should be thoroughly tested, secured, and optimized according to your business's security standards & policies before deploying to production or handling production workloads.

# MWAA Serverless MCP Server

Model Context Protocol (MCP) server for Amazon Managed Workflows for Apache Airflow (Amazon MWAA) Serverless.

It helps AI agents author, validate, deploy, and debug Amazon MWAA Serverless workflows, with a focus on producing DAGs that run the first time. Every schema rule it enforces was verified against the live `mwaa-serverless` API rather than inferred from documentation. It runs locally over stdio as a child process of your MCP client (recommended), or deployed to AWS Lambda behind an IAM-authenticated Function URL when a team needs a shared endpoint.

## Features

- **Correct-by-construction authoring.** `build_dag_yaml` emits the exact YAML shape the service accepts, so the common mistakes that make a DAG deploy and then fail are not expressible.
- **Validation and auto-repair.** `validate_dag_yaml` checks hand-written YAML against the real schema, and `repair_dag_yaml` fixes the mechanical errors.
- **Service-side preflight.** `preflight_dag_yaml` has Amazon MWAA Serverless itself validate a definition, then cleans up.
- **Least-privilege IAM.** `generate_execution_role` produces an execution-role policy scoped to the operators a DAG actually uses.
- **Deploy, run, and verify.** Deploy a workflow, trigger a run, and read per-task outcomes from Amazon CloudWatch, including failures hidden inside a run the service reports as `SUCCESS`.
- **Python DAG migration.** Analyze and convert existing Apache Airflow Python DAGs to YAML. Your Python is parsed with `ast`, never executed.
- **Python and Bash tasks.** Package code bundles for `PythonOperator` and `BashOperator`.
- **Optional failure analysis.** Summarize a failed run with Amazon Bedrock (opt-out; see [Security considerations](#security-considerations-for-a-remote-deployment)).
- **Two transports, same 38 tools.** Local stdio, or an IAM-authenticated Lambda Function URL.

For the DAG schema itself, XCom between tasks, code-bundle rules, and the cost of waiting, see **[docs/dag-reference.md](docs/dag-reference.md)**. You don't need it to use the server, because agents read the same material through the `get_serverless_overview`, `get_dag_yaml_spec`, and `get_serverless_constraints` tools.

## Prerequisites

| | Local stdio | Deployed Function URL |
|---|---|---|
| Python | 3.10+ | Lambda runs 3.12 |
| `boto3` | `>= 1.40` (see `src/requirements.txt`) | installed by `sam build` |
| AWS SAM CLI | not needed | 1.100+ |
| AWS CLI | v2 | v2 |
| `uv` / `uvx` | not needed | for the `mcp-proxy-for-aws` signing proxy |
| AWS credentials | any principal that can call Amazon MWAA Serverless | a principal that can create IAM roles and Lambda functions (`CAPABILITY_IAM`) |

Install `uv` (which provides `uvx`) from [docs.astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/). `sam build` for this stack is a plain Python zip build and doesn't require Docker.

**Region.** Amazon MWAA Serverless isn't available in every Region. `samconfig.toml` defaults to `us-east-1`. Change it to a Region where Amazon MWAA Serverless is offered and where your workflows live. See [One Region per running server](#one-region-per-running-server).

## Installation

Both transports expose the same 38 tools. Run locally unless you specifically need a shared endpoint. That is a security choice, explained in [Security considerations](#security-considerations-for-a-remote-deployment).

| | Local stdio | Deployed Function URL |
|---|---|---|
| Network exposure | none | HTTPS endpoint |
| Auth | your OS user | AWS IAM (SigV4) |
| Calls run as | you | the Lambda role, shared by all callers |
| Blast radius | your own permissions | the function's permissions, for every caller |
| CloudTrail attribution | your principal | the execution role |
| Setup | `pip install` | `sam deploy` + IAM parameters |
| Best for | individual development | shared team or hosted agent |

### Option 1 — local stdio (recommended)

Runs as a child process of your MCP client, with no endpoint, no URL to expose, and no shared role. Every AWS call is made as you, so the blast radius is exactly your own permissions and CloudTrail attributes each call to you by name.

```bash
cd serverless/mcp/src
python3 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements-local.txt
python local_server.py               # optional smoke test; Ctrl-C to stop
```

It logs the resolved AWS identity and Region to stderr on startup, so a credential or Region mistake is easy to spot. Then point your MCP client at it (see [Client configuration](#client-configuration)). It's a good idea to use least-privilege credentials, since the tools inherit whatever you hold: an admin session gives the delete tool the same reach as your own console.

### Option 2 — deploy to Lambda with a Function URL

Worth doing when something other than your own machine needs to reach the server. Please read [Security considerations for a remote deployment](#security-considerations-for-a-remote-deployment) first.

```bash
cd serverless/mcp
sam build
sam deploy --guided     # first time; afterwards just: sam deploy
```

`WorkflowBucketName` is required and has no default, so `sam deploy` prompts for it. `samconfig.toml` pins `stack_name`, `region = "us-east-1"`, and `capabilities = "CAPABILITY_IAM"`, so change the Region there or pass `--region` rather than deploying to us-east-1 by default.

Take `McpFunctionUrl` from the stack outputs, and check `S3AccessScope` to confirm which bucket the function is scoped to. The transport is a Lambda Function URL with `AuthType: AWS_IAM`; there is no Amazon API Gateway. Redeploy after changing code with `sam build && sam deploy`, and tear down with `sam delete --stack-name mwaa-serverless-mcp`.

Callers must SigV4-sign their requests, and no MCP client does that natively, so use the AWS signing proxy `mcp-proxy-for-aws` (see [Client configuration](#client-configuration)). Grant callers only `lambda:InvokeFunctionUrl` on the function; the `InvokePolicyHint` output prints the policy with your function ARN filled in.

## Client configuration

Replace the absolute paths and the Function URL with your own.

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
        "AWS_DEFAULT_REGION": "us-east-1"
      },
      "disabled": false
    }
  }
}
```

Remote (the proxy version is pinned deliberately; see [Security considerations](#security-considerations-for-a-remote-deployment)):

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

`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows). Restart Claude after editing. The `command` / `args` / `env` shape is the same as Kiro. Claude Desktop doesn't inherit your shell environment, so set `AWS_PROFILE` and `AWS_DEFAULT_REGION` explicitly and use an absolute path to `uvx`.

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

`.cursor/mcp.json` (or the client's equivalent) takes the same `command` / `args` / `env` shape as the Kiro local example.

### Verifying the connection

Ask the agent to call `get_server_config`. It returns the effective configuration and confirms the transport is reachable. `list_supported_operators` is a good second check: it needs no AWS credentials, so a failure there points at the MCP wiring rather than IAM.

## Configuration

Settings resolve from three layers, highest precedence first:

1. **Environment variable.** Takes effect immediately, no rebuild. Best for the deployed Lambda.
2. **JSON config file.** The layer a human edits. Best for local use.
3. **Built-in default.** Nothing has to be configured.

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

Run the `get_server_config` tool to see effective values, the provenance of each, and any `problems`. An unparseable file, unknown key, or non-numeric integer is reported there and the default is kept, so nothing fails silently.

### Changing the model

Only `mwaa_get_failed_runs` uses a model. Pin one by setting `BEDROCK_MODEL_ID`, which disables the fallback chain so an unreachable id fails clearly (`analysis_unavailable`) instead of quietly answering from a different model. Set it in your client's `env` block, in `src/mcp_config.json`, or, on a deployment, in `template.yaml` or the Lambda console. Find invocable ids (most current models need a `us.`-prefixed cross-Region inference profile) with:

```bash
aws bedrock list-inference-profiles \
  --query 'inferenceProfileSummaries[?status==`ACTIVE`].inferenceProfileId' --output text
```

## Environment variables

| Variable | Config key | Default | Purpose |
|---|---|---|---|
| `AWS_DEFAULT_REGION` | — | from profile | The Region for every AWS call. Set this, not `AWS_REGION` (see [One Region per running server](#one-region-per-running-server)). |
| `AWS_PROFILE` | — | `default` | AWS credentials profile. |
| `BEDROCK_MODEL_ID` | `bedrock_model_id` | `null` | Pin one model; disables the fallback chain. |
| `BEDROCK_MODEL_CANDIDATES` | `bedrock_model_candidates` | 4-model chain | Tried in order until one responds. |
| `BEDROCK_MAX_TOKENS` | `bedrock_max_tokens` | `2000` | Analysis response budget. |
| `BEDROCK_REGION` | `bedrock_region` | `null` | Bedrock-only Region override for failure analysis. |
| `MWAA_MCP_DEFAULT_POLL_SECONDS` | `default_poll_seconds` | `90` | Single `mwaa_poll_run` wait. |
| `MWAA_MCP_MAX_POLL_SECONDS` | `max_poll_seconds` | `110` | Hard cap; keep under the Lambda timeout. |
| `LOG_LEVEL` | `log_level` | `INFO` | |

## Tools

The server exposes 38 tools. 37 are deterministic Python: schema validation, repair, planning, YAML assembly, code bundling, and deploy and run inspection all run without calling a model. Exactly one, `mwaa_get_failed_runs`, optionally calls Amazon Bedrock.

### Reference

| Tool | Description |
|---|---|
| `get_serverless_overview` | Read first. How the service works, the YAML schema, the authoring policy. |
| `get_dag_yaml_spec` | Authoritative schema with a worked example and the exact error each mistake produces. |
| `get_serverless_constraints` | Full reference: Jinja, DAG and task params, `default_args` allowlist, quotas, observability. |
| `list_supported_operators` | Every allowlisted operator with its fully qualified path. |
| `describe_operator` | One operator: FQN, required arguments, XCom output. |
| `suggest_operator` | Find an operator from a plain-language description. |
| `get_server_config` | Effective configuration, where each value came from, the bound Region, and how to change the model. |

### Authoring

| Tool | Description |
|---|---|
| `list_pipeline_steps` | Catalog of composable pipeline operations. |
| `plan_pipeline` | Turn requested operations into a plan, the questions to ask, and what was deliberately left out. |
| `build_dag_yaml` | **Preferred.** Correct-by-construction YAML from a structured task list; `values_adjusted` reports anything coerced or capped. |
| `validate_dag_yaml` | Validate against the real schema: shape, operators, required args, XCom reachability, cycles, Jinja, size. |
| `repair_dag_yaml` | Auto-fix the mechanical mistakes and report every change. |
| `preflight_dag_yaml` | Have the service itself validate, then clean up. Branch on `verdict`: `valid` / `invalid` / `indeterminate`. |
| `generate_execution_role` | Least-privilege IAM role scoped to the operators used. |
| `generate_dag_yaml` | Demo workflow for one service. Check `params_you_must_set`, since only about half are fully self-contained. Not a pipeline starting point. |
| `compose_dag_yaml_tool` | Chain several service demo blocks. |
| `get_service_tasks_tool` | Inspect one service's demo block. |
| `analyze_python_dag_tool` | Compatibility analysis of a Python DAG. |
| `convert_python_to_yaml_tool` | Convert a Python DAG to validated YAML; check `faithful` and `dropped`, not just `valid`. |

### Python / Bash code

| Tool | Description |
|---|---|
| `get_code_bundle_guidance` | Packaging rules, worker environment, pre-installed packages, network limits. |
| `build_code_bundle` | Zip modules and scripts into a deployable bundle, with static checks. |
| `check_dag_code_consistency` | Verify every `python_callable` and script reference resolves. |

### Operations

| Tool | Description |
|---|---|
| `mwaa_deploy_and_run` | Upload definition + code bundle, create or update, start a run. Validates first. |
| `mwaa_verify_run_tasks` | **Authoritative per-task outcome from CloudWatch.** Call after every run. |
| `mwaa_poll_run` | Poll to a terminal state. |
| `mwaa_get_failed_runs` | Scan for failures, including green runs hiding failed tasks; optional Bedrock root-cause analysis. |
| `mwaa_get_run_status` | Run status with per-task detail. |
| `mwaa_list_workflows` / `mwaa_get_workflow` | List, or full detail including the deployed YAML and a validation check. |
| `mwaa_get_workflow_summary` / `mwaa_bulk_status` | Compact status views. |
| `mwaa_start_run` / `mwaa_stop_run` / `mwaa_list_runs` | Run control. |
| `mwaa_redeploy` | Update YAML and rerun, reusing the existing S3 location and role. |
| `mwaa_compare_versions` | Diff the latest two versions. |
| `mwaa_find_workflows_by_service` | Find workflows using a service by inspecting their YAML. |
| `mwaa_delete_workflows` | Bulk delete by pattern or inactivity. Dry run by default. |

## Usage

Author a pipeline:

1. `plan_pipeline([...])` gives the operators, the questions to ask, and what was left out.
2. Ask the user those questions, then `build_dag_yaml(...)` returns validated YAML. Check `values_adjusted`.
3. `generate_execution_role(account_id=…, region=…)` returns an ARN-scoped policy and the CLI to create the role. Do this before preflight, which needs the role ARN.
4. `preflight_dag_yaml(yaml, bucket, role_arn)` asks the service to validate it, then deletes the throwaway workflow. Branch on `verdict`, and don't read `indeterminate` as a pass.
5. `mwaa_deploy_and_run(...)` updates an existing workflow of the same name and starts a billable run.
6. `mwaa_poll_run(...)`, then `mwaa_verify_run_tasks(...)`.

Fix hand-written YAML: `validate_dag_yaml` → `repair_dag_yaml` → `preflight_dag_yaml`.

Migrate a Python DAG: `analyze_python_dag_tool` → `convert_python_to_yaml_tool` → `validate_dag_yaml`.

Before step 4 you supply an S3 bucket you own (for the definition and any code bundle) and a workflow execution role (step 3 generates the policy for it). Neither is created for you.

> **A `SUCCESS` run doesn't mean every task passed.** Verified against the live service: a task raised `NoSuchBucket`, a downstream `trigger_rule: all_done` task then succeeded, and the run reported `RunState: SUCCESS`. Filtering on `RunState == FAILED` misses this. `mwaa_verify_run_tasks` reads each task's CloudWatch log stream for the authoritative outcome, so call it after every run.

## Security considerations for a remote deployment

None of this applies to local stdio, where the tools run under your own credentials and there is no endpoint. It all applies once you `sam deploy`.

### Understand the privilege chain

```text
lambda:InvokeFunctionUrl
  -> the Lambda execution role
  -> s3:PutObject (workflow definition + code bundle)
  -> airflow-serverless:CreateWorkflow / UpdateWorkflow
  -> iam:PassRole
  -> your Python or Bash running under the passed role
```

Granting the first link grants the last. `AuthType: AWS_IAM` stops anonymous access and nothing more. An authorized caller reaches the end of that chain by design, because deploying workflows that run code is the function's job. Treat `lambda:InvokeFunctionUrl` on this function as equivalent to handing over the passable roles, and grant it to one dedicated principal rather than to everyone with broad administrator access.

### Endpoint authentication

- **`AuthType: AWS_IAM` is on by default.** Unsigned requests get `403 Forbidden`, and SigV4-signed requests get `200`. There is no anonymous path.
- **The URL isn't a secret.** It appears in stack outputs, config files, and logs. Authentication, not obscurity, is what protects it.
- **IAM auth doesn't separate privileged principals.** `lambda:InvokeFunctionUrl` is in `AdministratorAccess` and `PowerUserAccess`, so in an all-admin account this blocks the internet but not your colleagues. Grant a dedicated invoke role if you need that distinction.

### Constrain every link

| Parameter | Default | What it limits |
|---|---|---|
| `WorkflowBucketName` | required | S3 reads and writes to one bucket, in this account |
| `PassableExecutionRolePath` | `mwaa-serverless-*` | which roles can be passed. Narrow it to exact role names. |
| `AllowWorkflowDeletion` | `false` | removes `DeleteWorkflow` for the delete tool |
| `EnableFailureAnalysis` | `true` | removes `bedrock:InvokeModel` when false |
| `ReservedConcurrentExecutions` | `5` | caps runaway cost and abuse rate |

`PassableExecutionRolePath` matters most. Avoid widening it to `*`: the `iam:PassedToService` condition limits which *service* receives the role, not which role can be passed, so `*` makes every role in the account passable, including an `AdministratorAccess` role. Consider a permissions boundary on the passable roles, so a later mistake can't turn one into an administrator.

### Known limitations of this sample

Worth knowing before you treat a deployment as production infrastructure:

- **One role for every tool.** Read-only discovery, deployment, and deletion all execute under the same execution role, with no per-caller or per-tool authorization in the dispatch path. A production build would split the tool surface across separate functions and roles.
- **Attribution stops at the role.** Downstream CloudTrail events name the execution role, not the caller. Log the authenticated principal per tool call before dispatch, or use local stdio, where every call is already attributed.
- **Arbitrary code arrives inline.** `mwaa_deploy_and_run` accepts a base64 code bundle from the caller. A stricter pattern is to have CI place immutable artifacts in a fixed S3 prefix and pass only `code_s3_key`, so no code travels through the endpoint.
- **Deployment is all-at-once.** There is no alias, canary, or alarm-gated rollback, so a broken release takes all traffic immediately. Add `AutoPublishAlias` and a `DeploymentPreference` if you come to depend on this.
- **No idempotency tokens.** A retried or duplicated request can overwrite a definition and start a second run. `mwaa_deploy_and_run` also turns a name conflict into an update of the existing workflow.

### Failure analysis sends task logs to Bedrock (on by default)

`mwaa_get_failed_runs` takes `analyze=true` by default, and the deployed stack grants `bedrock:InvokeModel` by default (`EnableFailureAnalysis=true`).

- **What leaves your account.** Failure details, including CloudWatch task log excerpts (up to 12,000 characters), are sent to Amazon Bedrock. Task logs routinely contain bucket names, ARNs, account ids, and sometimes fragments of your data. Credential-shaped values are redacted first; resource names are not.
- **Where it goes.** The default model chain leads with `us.`-prefixed cross-Region inference profiles, so that content may be processed in a different Region than your workflows.
- **How to control it.** Deploy with `EnableFailureAnalysis=false` to remove the permission entirely; or leave it and pass `analyze=false` per call, so nothing is sent unless asked; or keep analysis in-Region by setting `BEDROCK_REGION` and pinning a non-`us.` `BEDROCK_MODEL_ID`. Turning it off costs you no diagnostic detail, because the failures, the task logs, and the hidden failed tasks are all gathered without a model and are the authoritative source. Treat the returned analysis as an untrusted suggestion, since task logs can contain text aimed at the model.

### Destructive and billable tools

- **`mwaa_delete_workflows` targets every workflow in the account when called with no filter.** It previews by default, and an unfiltered non-dry-run call is refused: deleting everything requires `confirm_delete_all=true`. Pass `name_contains` or `not_run_in_days` to scope it.
- **Three tools mutate or bill.** `mwaa_deploy_and_run` updates a same-named workflow and starts a billable run; `mwaa_redeploy` overwrites the deployed definition and its S3 object, then reruns; `mwaa_stop_run` stops an in-flight run. All three require an exact workflow name.
- **`preflight_dag_yaml` creates and deletes a throwaway workflow.** Its delete is permitted even when `AllowWorkflowDeletion=false`, through a grant scoped to the `preflight-*` name prefix. Withholding it wouldn't make preflight safer; it would leak one workflow per call. The empty CloudWatch log group the service creates for the throwaway is named in `log_group_residue`.
- **S3 writes use SSE-S3 and assert bucket ownership.** On a deployment the owning account comes from the stack. Locally, pass `expected_bucket_owner` on `mwaa_deploy_and_run`, `mwaa_redeploy`, and `preflight_dag_yaml`.

### Pin the signing proxy

`mcp-proxy-for-aws` runs in a process that receives your AWS profile, so resolving it at `@latest` on every launch means a new release can start signing your requests without review. The examples pin an exact version (`mcp-proxy-for-aws==1.7.0`); check for newer ones deliberately with `uvx pip index versions mcp-proxy-for-aws`.

### Local config files

Don't commit `src/mcp_config.json` (it is git-ignored). Keep secrets out of it regardless, since it holds configuration, not credentials.

## One Region per running server

The server resolves its AWS Region once, at startup, and every workflow, S3, and CloudWatch Logs call uses that Region for the life of the process. No tool takes a Region argument. To work in another Region, change the environment and restart the server (in an MCP client, edit the server's `env` block and restart the client so the subprocess is respawned).

A wrong Region isn't reported as an error, because an empty workflow list from the wrong Region looks the same as an empty one from the right Region. So `get_server_config` reports `aws_region` (the effective Region and where it came from). Check it before concluding a workflow is missing. Passing an unsupported argument such as `region` is rejected rather than ignored.

**Set `AWS_DEFAULT_REGION`, not `AWS_REGION`.** botocore resolves the session Region from `AWS_DEFAULT_REGION` only; `AWS_REGION` isn't consulted, so setting only that leaves the Region coming from your profile:

```bash
AWS_REGION=eu-west-1 python -c "import boto3; print(boto3.Session().region_name)"
# -> us-east-1   (the profile's Region; AWS_REGION ignored)

AWS_DEFAULT_REGION=eu-west-1 python -c "import boto3; print(boto3.Session().region_name)"
# -> eu-west-1
```

It works in Lambda because the runtime sets both. Locally it doesn't, so `get_server_config` reports `ignored_aws_region` when `AWS_REGION` is set but not taking effect, and the server warns at startup.

## Costs

Nothing here is free-tier guaranteed; see the
[Amazon MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/pricing/),
[Lambda](https://aws.amazon.com/lambda/pricing/),
[S3](https://aws.amazon.com/s3/pricing/),
[CloudWatch](https://aws.amazon.com/cloudwatch/pricing/), and
[Bedrock](https://aws.amazon.com/bedrock/pricing/) pricing pages.

- **Local stdio costs nothing to run.** Only the AWS calls it makes are billed.
- **Amazon MWAA Serverless is the real cost.** `preflight_dag_yaml`, `mwaa_deploy_and_run`, and `mwaa_redeploy` create real workflows and start real runs, billed for the time each task occupies a worker.
- **Lambda:** 512 MB, and a single `mwaa_poll_run` can occupy the function up to 110 seconds. `ReservedConcurrentExecutions: 5` bounds the worst case.
- **CloudWatch Logs:** ingestion and storage for task logs, plus the function's own logs (retention set by `LogRetentionDays`, default 30).
- **Bedrock:** per-token, and on by default. Pass `analyze=false`, or deploy with `EnableFailureAnalysis=false`, to avoid it. The log-based findings are unaffected.
- **Demo templates provision real infrastructure** (Amazon EMR, Amazon RDS, Amazon Redshift, Amazon EKS). They tear it down again, but bill while running, and a failed teardown leaves it running. Check with `mwaa_verify_run_tasks`.

## Cleaning up

`sam delete` removes the server. It doesn't remove anything the tools created, so clean those up too:

```bash
# 1. Workflows this server created. Review the dry run first.
#    From your MCP client: mwaa_list_workflows, then
#                         mwaa_delete_workflows(name_contains="<your-prefix>")

# 2. Definitions and code bundles in your own bucket
aws s3 rm "s3://<your-workflow-bucket>/workflows/" --recursive
aws s3 rm "s3://<your-workflow-bucket>/preflight/" --recursive   # if preflight left any

# 3. Task log groups. These retain and bill until removed. Each preflight_dag_yaml call
#    also leaves one empty log group behind (named in the response's log_group_residue).
aws logs describe-log-groups --log-group-name-prefix /aws/mwaa-serverless/ \
  --query 'logGroups[].logGroupName' --output text \
  | tr '\t' '\n' | xargs -I{} aws logs delete-log-group --log-group-name {}

# 4. Any execution role you created from generate_execution_role
aws iam delete-role-policy --role-name mwaa-serverless-<dag_id>-role --policy-name <dag_id>-policy
aws iam delete-role --role-name mwaa-serverless-<dag_id>-role

# 5. Any CloudFormation stacks a demo DAG left behind after a failed teardown
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE DELETE_FAILED \
  --query "StackSummaries[?contains(StackName,'mwaa-')].StackName"

# 6. The server itself
sam delete --stack-name mwaa-serverless-mcp
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Unknown service: 'mwaa-serverless'` | boto3 too old | `pip install 'boto3>=1.40'`. The legacy `airflow-serverless` service resolves to a non-existent host, so it isn't used as a fallback. |
| `403 Forbidden` from the Function URL | request not signed, or caller lacks `lambda:InvokeFunctionUrl` | Use `mcp-proxy-for-aws`; grant the invoke policy. |
| `The 'mcp' package is required for local stdio mode` | wrong requirements file | `pip install -r requirements-local.txt` |
| `analysis_unavailable` in `mwaa_get_failed_runs` | no invocable Bedrock model | Pin `BEDROCK_MODEL_ID` to an active id; enable model access in the Region. Log-based detail is unaffected. |
| Tools work, AWS calls fail | credentials or Region | `get_server_config`; check the identity line `local_server.py` logs to stderr. |
| Empty workflow list, but you expected results | wrong Region | `get_server_config` → `aws_region`. Set `AWS_DEFAULT_REGION`, not `AWS_REGION`. |
| Run says `SUCCESS` but nothing happened | a trailing `all_done` task masked an earlier failure | `mwaa_verify_run_tasks`. |
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
| Code bundle passed **inline** to a deployed server | about 4.5 MB. A Lambda request is capped at 6 MB and base64 inflates by a third. Upload to S3 and pass `code_s3_key` instead. No such limit in local stdio mode. |
| Task execution timeout | 60 minutes |
| Retries per task | 0–3 |
| Retry delay | 0–300 seconds |

## Architecture

```
serverless/mcp/
├── template.yaml           # SAM template (Lambda + IAM-auth Function URL)
├── samconfig.toml
├── README.md
├── docs/dag-reference.md   # the DAG schema, XCom, Python/Bash, cost of waiting
└── src/
    ├── app.py              # MCP tool definitions (shared by both transports)
    ├── local_server.py     # Local stdio entrypoint, no network exposure
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
    ├── requirements.txt        # Lambda runtime deps
    └── requirements-local.txt  # adds `mcp` for local stdio mode
```

## Lambda IAM permissions

Almost every statement is ARN-scoped to this account and Region. Three actions use `Resource: '*'` because the API can't scope them: they are collection-level, read-or-create, and none are destructive.

| Action | Why it can't be scoped |
|---|---|
| `airflow-serverless:ListWorkflows` | Enumerates the account; there is no per-item ARN to name |
| `airflow-serverless:CreateWorkflow` | The workflow doesn't exist yet, so there is nothing to name |
| `logs:DescribeLogGroups` | Enumerates log groups; it doesn't read one |

This was found by deploying, not by linting. Scoped to `workflow/*`, these evaluate to `implicitDeny`, so every name-resolving tool failed with `AccessDeniedException` at run time while `cfn-lint` and `sam build` reported success. A test enforces that the wildcard list stays exactly these three and that nothing destructive joins it.

| Permission | Resource scope |
|---|---|
| `airflow-serverless:ListWorkflows`, `CreateWorkflow` | `*`, collection-level (see above) |
| `airflow-serverless:GetWorkflow`, `GetWorkflowRun`, `ListWorkflowRuns`, `ListWorkflowVersions` | `…:workflow/*` in this account and Region |
| `airflow-serverless:UpdateWorkflow`, `StartWorkflowRun`, `StopWorkflowRun` | `…:workflow/*` in this account and Region |
| `airflow-serverless:DeleteWorkflow` | `…:workflow/preflight-*`, always granted, so preflight can delete its throwaway |
| `airflow-serverless:DeleteWorkflow` | `…:workflow/*`, omitted unless `AllowWorkflowDeletion=true` |
| `s3:GetObject/PutObject/DeleteObject` | `WorkflowBucketName/*`, with `s3:ResourceAccount` |
| `s3:ListBucket/GetBucketLocation` | `WorkflowBucketName`, with `s3:ResourceAccount` |
| `logs:DescribeLogGroups` | `*`, collection-level (see above) |
| `logs:DescribeLogStreams`, `logs:GetLogEvents` | `log-group:/aws/mwaa-serverless/*` |
| `bedrock:InvokeModel` | models and inference profiles in-Region, omitted when `EnableFailureAnalysis=false` |
| `iam:PassRole` | `role/${PassableExecutionRolePath}`, with `iam:PassedToService: airflow-serverless.amazonaws.com` |

Both conditions on `iam:PassRole` are needed: `iam:PassedToService` limits which service receives the role, and the `Resource` pattern limits which role can be passed. Callers of the deployed endpoint need only `lambda:InvokeFunctionUrl` on the function. In local stdio mode there is no endpoint, and the tools use your own credentials.

## Tests

```bash
cd serverless/mcp
pip install -r src/requirements-local.txt pytest ruff cfn-lint bandit
python -m pytest
ruff check .
cfn-lint template.yaml
bandit -r src/
```

No test calls AWS. Workflow operations run against a stub client that records destructive calls, so the tests assert those calls did not happen. The suite covers the schema rules, untrusted-input bounds, every generated IAM policy, all 29 demo templates, and the cross-module invariants between `schema.py` and `constraints.py`. These checks run in CI on any change under `serverless/mcp/` (see [.github/workflows/serverless-mcp.yml](../../.github/workflows/serverless-mcp.yml)).

Passing them is necessary but not sufficient. Several real defects in this sample were invisible to all of them and only surfaced by deploying and running against the service: an IAM policy that lints clean but denies at run time, an operator that writes no log stream, and preflight leaking a workflow per call. `verify_generated_policies.py` closes part of that gap by simulating the generated policies against IAM itself (`iam:SimulateCustomPolicy`, which needs credentials and isn't part of CI):

```bash
python verify_generated_policies.py          # all 29 demo policies
python verify_generated_policies.py ec2 s3   # or just these
```

## License and contributing

This sample is released under the **MIT-0** license; see [LICENSE](../../LICENSE). Contribution guidance is in [CONTRIBUTING.md](../../CONTRIBUTING.md), and the code of conduct is in [CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md).
