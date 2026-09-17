# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""The tests that matter most for a sample: what a customer could paste and regret.

Nothing here calls AWS. The workflow-operations tests use a stub client and assert
that destructive calls were NOT made.
"""

import json
import re
from pathlib import Path

import pytest
import yaml

import operations
import tools
from conftest import ALL_DEMO_SERVICES

REPO_ROOT = Path(__file__).resolve().parent.parent


class _CfnLoader(yaml.SafeLoader):
    """SafeLoader that tolerates CloudFormation's !Ref / !GetAtt / !Sub tags.

    It subclasses SafeLoader and its multi-constructor returns None for every tag, so
    it cannot instantiate arbitrary objects. ruff's S506 flags any non-safe_load call
    and cannot see the base class, hence the noqa on each use.
    """


_CfnLoader.add_multi_constructor("!", lambda loader, suffix, node: None)

_PSEUDO_PARAMS = {"AWS", "Partition", "Region", "AccountId", "StackName",
                  "NoValue", "URLSuffix"}


def _load_cfn(text):
    """Parse CloudFormation YAML. _CfnLoader is a SafeLoader subclass whose tag
    constructor returns None, so it cannot instantiate objects; ruff's S506 only
    checks that the loader is not literally SafeLoader."""
    return yaml.load(text, Loader=_CfnLoader)  # noqa: S506


def _embedded_cfn_bodies(dag_yaml):
    dag = next(iter(yaml.safe_load(dag_yaml).values()))
    for task_id, cfg in dag["tasks"].items():
        body = (cfg.get("cloudformation_parameters") or {}).get("TemplateBody")
        if body:
            yield task_id, body


# ══════════════════════════════════════════════════════════════════════════
#  IAM execution-role generator
# ══════════════════════════════════════════════════════════════════════════

def _policy_for(service="glue", **kwargs):
    dag_yaml = tools.generate_yaml(f"demo_{service}", service)["dag_yaml"]
    return tools.generate_execution_role_policy(dag_yaml, **kwargs)


@pytest.mark.parametrize("service", ALL_DEMO_SERVICES)
def test_generated_policy_never_uses_resource_wildcard(service):
    """A Resource "*" policy plus a copy-paste CLI command is the single most
    dangerous thing this sample could ship."""
    result = _policy_for(service, account_id="123456789012", region="us-east-1")
    for statement in result["permissions_policy"]["Statement"]:
        resource = statement["Resource"]
        resources = resource if isinstance(resource, list) else [resource]
        assert "*" not in resources, f"{statement['Sid']} in {service} uses Resource: '*'"
        for arn in resources:
            assert arn.startswith("arn:"), f"{statement['Sid']}: {arn!r} is not an ARN"


def test_passrole_to_cloudformation_is_withheld_without_named_roles():
    """CloudFormation acts with whatever role it is handed, so an unscoped grant is
    a path to anything any passable role can do."""
    result = _policy_for("glue", account_id="123456789012", region="us-east-1")
    passrole = [s for s in result["permissions_policy"]["Statement"]
                if "iam:PassRole" in s["Action"]]
    assert passrole, "the glue demo passes a role, so a PassRole statement is expected"
    principals = passrole[0]["Condition"]["StringEquals"]["iam:PassedToService"]
    assert "cloudformation.amazonaws.com" not in principals
    assert any("NOT included" in n for n in result["passrole_notes"])


def test_passrole_to_cloudformation_requires_explicit_role_arns():
    role = "arn:aws:iam::123456789012:role/my-cfn-service-role"
    result = _policy_for("glue", account_id="123456789012", region="us-east-1",
                         passable_role_arns=[role])
    passrole = [s for s in result["permissions_policy"]["Statement"]
                if "iam:PassRole" in s["Action"]][0]
    assert passrole["Resource"] == role


def test_passrole_defaults_to_a_name_prefix_not_a_wildcard():
    result = _policy_for("sagemaker", account_id="123456789012", region="us-east-1")
    passrole = [s for s in result["permissions_policy"]["Statement"]
                if "iam:PassRole" in s["Action"]][0]
    assert passrole["Resource"].endswith(":role/mwaa-serverless-*")


@pytest.mark.parametrize("region,partition", [
    ("us-east-1", "aws"),
    ("eu-west-1", "aws"),
    ("us-gov-west-1", "aws-us-gov"),
    ("cn-north-1", "aws-cn"),
])
def test_policy_partition_follows_the_region(region, partition):
    """arn:aws in GovCloud or China matches nothing, so tasks lose permissions with
    no error to explain it."""
    result = _policy_for("s3", account_id="123456789012", region=region)
    assert result["partition"] == partition
    for statement in result["permissions_policy"]["Statement"]:
        resources = statement["Resource"]
        for arn in (resources if isinstance(resources, list) else [resources]):
            assert arn.startswith(f"arn:{partition}:")


def test_placeholder_policy_cannot_be_pasted_as_a_one_liner():
    """Without an account id the policy is unusable, so the CLI must be a
    file-based sequence with an explicit substitution step."""
    result = _policy_for("glue")
    assert result["requires_substitution"]
    assert "put_policy" not in result["cli_commands"]
    assert any("substitute" in k for k in result["cli_commands"])


def test_destructive_actions_are_isolated_in_their_own_statement():
    result = _policy_for("s3", account_id="123456789012", region="us-east-1")
    destructive_sids = [s["Sid"] for s in result["permissions_policy"]["Statement"]
                        if s["Sid"].startswith("Destructive")]
    assert destructive_sids
    for statement in result["permissions_policy"]["Statement"]:
        if statement["Sid"].startswith("Destructive"):
            continue
        actions = statement["Action"]
        for action in (actions if isinstance(actions, list) else [actions]):
            verb = action.split(":", 1)[-1]
            assert not verb.startswith(("Delete", "Terminate")), \
                f"{action} belongs in a Destructive* statement"


def test_destructive_actions_can_be_withheld_entirely():
    result = _policy_for("s3", account_id="123456789012", region="us-east-1",
                         include_destructive_actions=False)
    assert result["destructive_actions_withheld"]
    assert not any(s["Sid"].startswith("Destructive")
                   for s in result["permissions_policy"]["Statement"])


def test_role_name_stays_within_the_iam_limit():
    long_dag = "x" * 90
    yaml_doc = (f"{long_dag}:\n  tasks:\n    t:\n"
                f"      operator: airflow.providers.standard.operators.empty.EmptyOperator\n")
    result = tools.generate_execution_role_policy(yaml_doc)
    assert len(result["role_name"]) <= 64
    assert re.fullmatch(r"[A-Za-z0-9+=,.@_-]+", result["role_name"])


def test_trust_policy_has_confused_deputy_conditions():
    result = _policy_for("s3", account_id="123456789012", region="us-east-1")
    condition = result["trust_policy"]["Statement"][0]["Condition"]
    assert condition["StringEquals"]["aws:SourceAccount"] == "123456789012"
    assert "aws:SourceArn" in condition["ArnLike"]


# ══════════════════════════════════════════════════════════════════════════
#  Demo templates
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("service", ALL_DEMO_SERVICES)
def test_demo_template_is_valid(service):
    result = tools.generate_yaml(f"demo_{service}", service)
    assert result["valid"] is True, result.get("errors")


@pytest.mark.parametrize("service", ALL_DEMO_SERVICES)
def test_composed_template_is_valid(service):
    """compose_dag_yaml did not rewrite xcom task_ids, so it emitted invalid YAML
    referencing task ids that do not exist in the composed DAG."""
    result = tools.compose_dag_yaml(f"c_{service}", [{"service": service}])
    assert result["valid"] is True, result.get("errors")


@pytest.mark.parametrize("service", ALL_DEMO_SERVICES)
def test_demo_owned_resource_names_are_run_unique(service):
    """This is what makes the trigger_rule: all_done cleanup safe. A fixed default
    stack name meant a collision caused the DAG to DELETE the customer's stack."""
    dag_yaml = tools.generate_yaml(f"demo_{service}", service)["dag_yaml"]
    unscoped = re.findall(
        r"\{\{ params\.(stack_name|bucket_name) \}\}(?!-\{\{ ts_nodash)", dag_yaml
    )
    assert not unscoped, f"{service} references an owned resource name with no run suffix"


@pytest.mark.parametrize("service", ALL_DEMO_SERVICES)
def test_embedded_cloudformation_parses_and_has_no_dangling_refs(service):
    dag_yaml = tools.generate_yaml(f"demo_{service}", service)["dag_yaml"]
    for task_id, body in _embedded_cfn_bodies(dag_yaml):
        doc = _load_cfn(body)
        assert isinstance(doc, dict), f"{service}/{task_id}: template is not a mapping"
        declared = set(doc.get("Resources") or {}) | set(doc.get("Parameters") or {})
        referenced = (set(re.findall(r"!Ref\s+(\w+)", body))
                      | set(re.findall(r"!GetAtt\s+(\w+)\.", body))
                      | set(re.findall(r"\$\{(\w+)[.}]", body)))
        missing = referenced - declared - _PSEUDO_PARAMS
        assert not missing, f"{service}/{task_id}: dangling references {sorted(missing)}"


def test_no_demo_ships_a_plaintext_password():
    source = (REPO_ROOT / "src" / "tools.py").read_text()
    assert "REPLACE_ME_SecurePassword" not in source
    assert "resolve:secretsmanager" in source, \
        "database demos should generate a password in Secrets Manager"


def test_no_demo_grants_wildcard_s3_inside_a_stack_role():
    source = (REPO_ROOT / "src" / "tools.py").read_text()
    assert "Action: s3:*" not in source
    assert "Resource: '*'" not in source


def test_no_demo_creates_an_account_global_role():
    """dms-vpc-role is a fixed account-wide name every DMS instance depends on:
    creating it fails in an account that has it, and deleting it breaks the rest."""
    source = (REPO_ROOT / "src" / "tools.py").read_text()
    assert "RoleName: dms-vpc-role" not in source


def test_no_demo_opens_a_collection_to_the_public():
    source = (REPO_ROOT / "src" / "tools.py").read_text()
    assert '\\"AllowFromPublic\\":true' not in source


def test_created_buckets_are_encrypted_and_private():
    source = (REPO_ROOT / "src" / "tools.py").read_text()
    bucket_count = source.count('"    Type: AWS::S3::Bucket\\n"')
    assert bucket_count > 0
    assert source.count("SSEAlgorithm: AES256") == bucket_count
    assert source.count("RestrictPublicBuckets: true") == bucket_count


def test_placeholder_defaults_are_surfaced_to_the_caller():
    """A plausible-looking wrong default (a us-east-1-only ECR URI) is worse than an
    obvious placeholder, because it looks like it should work."""
    result = tools.generate_yaml("demo_sagemaker", "sagemaker")
    assert "image_uri" in (result.get("params_you_must_set") or [])
    assert "683313688378" not in result["dag_yaml"]


# ══════════════════════════════════════════════════════════════════════════
#  Destructive workflow operations
# ══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def stubbed(stub_mwaa_client, monkeypatch):
    stub = stub_mwaa_client()
    monkeypatch.setattr(operations, "_client", stub)
    monkeypatch.setattr(operations, "_new_client", lambda: stub)
    return stub


def test_list_workflows_follows_pagination(stubbed):
    """Only the list tool paginated; every name-resolution path used one page, so a
    workflow past the first page intermittently 'did not exist'."""
    names = [w["name"] for w in operations.list_workflows()["workflows"]]
    assert "page2-only" in names


def test_workflow_on_the_second_page_is_resolvable(stubbed):
    arn, name, _ = operations._resolve_workflow_arn("page2-only")
    assert arn and name == "page2-only"


def test_unfiltered_delete_is_refused_and_deletes_nothing(stubbed):
    result = operations.delete_workflows(dry_run=False)
    assert "error" in result
    assert result["workflows_that_would_be_deleted"]
    assert stubbed.deleted == [], "a refused call must not delete anything"


def test_unfiltered_dry_run_carries_a_warning(stubbed):
    result = operations.delete_workflows()
    assert result["dry_run"] is True
    assert result["unfiltered_warning"]
    assert stubbed.deleted == []


def test_deleting_everything_requires_explicit_confirmation(stubbed):
    result = operations.delete_workflows(dry_run=False, confirm_delete_all=True)
    assert result["deleted"] == 3
    assert len(stubbed.deleted) == 3


def test_filtered_delete_does_not_need_confirmation(stubbed):
    result = operations.delete_workflows(name_contains="billing", dry_run=False)
    assert result["deleted"] == 1
    assert stubbed.deleted == [
        "arn:aws:airflow-serverless:us-east-1:111122223333:workflow/prod-billing"
    ]


@pytest.mark.parametrize("call", [
    lambda: operations.start_workflow_run("prod"),
    lambda: operations.stop_workflow_run("prod"),
    # redeploy is given a VALID definition so the ambiguity check is what rejects it,
    # not the validation that now runs first.
    lambda: operations.redeploy_workflow(
        "prod",
        "d:\n  tasks:\n    t:\n"
        "      operator: airflow.providers.standard.operators.empty.EmptyOperator\n",
    ),
])
def test_mutating_tools_refuse_an_ambiguous_name(stubbed, call):
    """`stop_run('prod')` used to silently take the first substring match, so it
    could kill the wrong workflow's run or overwrite the wrong definition."""
    result = call()
    assert "error" in result
    assert "matches 2 workflows" in result["error"]
    assert stubbed.started == [] and stubbed.stopped == [] and stubbed.updated == []


def test_read_only_lookup_still_accepts_a_partial_name(stubbed):
    arn, name, _ = operations._resolve_workflow_arn("billing")
    assert name == "prod-billing"


def test_redeploy_refuses_a_definition_that_fails_validation(stubbed):
    result = operations.redeploy_workflow("prod-billing", "d:\n  tasks: {}\n")
    assert "error" in result
    assert stubbed.updated == [], "the deployed definition must be left untouched"


def test_s3_writes_are_encrypted_and_assert_ownership():
    calls = []

    class S3:
        def put_object(self, **kwargs):
            calls.append(kwargs)
            return {}

    operations._put_object(S3(), "b", "k", b"body", "123456789012")
    assert calls[0]["ServerSideEncryption"] == "AES256"
    assert calls[0]["ExpectedBucketOwner"] == "123456789012"


def test_attempt_ordering_is_numeric():
    """A string compare made attempt '10' lose to '9', so past ten attempts the
    reported outcome was the wrong one."""
    assert operations._attempt_number("10") > operations._attempt_number("9")
    assert operations._attempt_number(None) == 0
    assert operations._attempt_number("attempt=3") == 0


def test_oversized_code_bundle_is_rejected_before_decoding(stubbed):
    huge = "A" * (operations._MAX_CODE_ZIP_BYTES * 4 // 3 + 8)
    result = operations.deploy_and_run("wf", "d:\n  tasks: {}\n", "bucket", "role", 
                                       code_zip_base64=huge)
    assert "error" in result


# ══════════════════════════════════════════════════════════════════════════
#  Deployment template
# ══════════════════════════════════════════════════════════════════════════

def test_deployment_template_wildcards_only_where_the_api_demands_them():
    """A "no Resource: '*' anywhere" absolute is not achievable, and pretending
    otherwise produced a policy that did not work.

    Found by deploying: ListWorkflows, CreateWorkflow and logs:DescribeLogGroups are
    COLLECTION-level operations. Scoped to a workflow ARN they evaluate to implicitDeny
    (confirmed with iam:SimulatePrincipalPolicy), so every name-resolving tool failed
    with AccessDeniedException at run time while cfn-lint and `sam build` both passed.

    So the rule this test enforces is the honest one: a wildcard is allowed ONLY for
    actions that cannot be resource-scoped, and every such action must be on this list.
    Nothing destructive may use one.
    """
    text = (REPO_ROOT / "template.yaml").read_text()
    doc = _load_cfn(text)

    may_use_wildcard = {
        "airflow-serverless:ListWorkflows",   # enumerates the account; no per-item ARN
        "airflow-serverless:CreateWorkflow",  # the workflow does not exist yet
        "logs:DescribeLogGroups",             # enumerates; does not read one group
    }

    policies = doc["Resources"]["McpFunction"]["Properties"]["Policies"]
    # !If-wrapped statements render as None through the tag-ignoring loader. Those are
    # the conditional DeleteWorkflow / FailureAnalysis grants, both ARN-scoped in the
    # source; the literal statements are the ones this audit can see.
    statements = [s for block in policies for s in block["Statement"] if isinstance(s, dict)]
    assert statements, "the template should still declare inline policy statements"

    wildcard_actions = set()
    for st in statements:
        # !Sub / !If render as None through the tag-ignoring loader; a literal '*' does not.
        if st.get("Resource") != "*":
            continue
        actions = st["Action"]
        wildcard_actions.update(actions if isinstance(actions, list) else [actions])

    unjustified = wildcard_actions - may_use_wildcard
    assert not unjustified, (
        f"these actions use Resource: '*' without justification: {sorted(unjustified)}. "
        f"Scope them to an ARN, or add them here with a comment explaining why the API "
        f"cannot scope them."
    )
    for action in wildcard_actions:
        verb = action.split(":", 1)[-1]
        assert not verb.startswith(("Delete", "Terminate", "Put", "Update")), \
            f"{action} is destructive or mutating and must never use Resource: '*'"


def test_deployment_template_scopes_everything_else_to_an_arn():
    """Every non-wildcard statement must name an ARN, not be left implicit."""
    text = (REPO_ROOT / "template.yaml").read_text()
    # Each scoped statement uses !Sub with a partition/account-qualified ARN.
    assert text.count("arn:${AWS::Partition}:") >= 6
    assert 'Resource: "*"' not in text, "use single quotes so the wildcard audit above sees it"


def test_deployment_template_scopes_down_by_parameter():
    doc = _load_cfn((REPO_ROOT / "template.yaml").read_text())
    params = doc["Parameters"]
    # Irreversible, so opt-in.
    assert params["AllowWorkflowDeletion"]["Default"] == "false"
    # A name pattern, never "*": iam:PassedToService alone would leave every role in the
    # account passable.
    assert params["PassableExecutionRolePath"]["Default"] == "mwaa-serverless-*"


def test_failure_analysis_default_matches_the_tool_default():
    """The IAM permission and the tool argument must agree.

    They disagreed once: the template withheld bedrock:InvokeModel while the tool still
    defaulted analyze=True, so every call burned four AccessDenied attempts and returned
    analysis_unavailable on the happy path. Whichever way this is set, set it in both
    places.
    """
    import inspect

    import app

    doc = _load_cfn((REPO_ROOT / "template.yaml").read_text())
    permission_granted = doc["Parameters"]["EnableFailureAnalysis"]["Default"] == "true"

    sig = inspect.signature(app.mcp_server.tool_implementations["mwaa_get_failed_runs"])
    analyze_default = sig.parameters["analyze"].default

    assert permission_granted == analyze_default, (
        f"EnableFailureAnalysis default ({permission_granted}) and the mwaa_get_failed_runs "
        f"analyze default ({analyze_default}) must match"
    )


def test_failure_analysis_data_flow_is_documented():
    """It is on by default, so the README must say what leaves the account and how to
    stop it."""
    readme = (REPO_ROOT / "README.md").read_text()
    for phrase in ("task log", "cross-Region", "EnableFailureAnalysis=false",
                   "analyze=false", "BEDROCK_REGION"):
        assert phrase in readme, f"README should explain {phrase!r}"


def test_lambda_log_group_has_explicit_retention():
    doc = _load_cfn((REPO_ROOT / "template.yaml").read_text())
    assert "McpFunctionLogGroup" in doc["Resources"], \
        "an implicit Lambda log group never expires and accrues cost forever"


def test_every_source_file_carries_the_spdx_header():
    for path in sorted((REPO_ROOT / "src").glob("*.py")):
        head = path.read_text().split("\n", 2)[:2]
        assert any("SPDX-License-Identifier: MIT-0" in line for line in head), path.name


def test_runtime_dependencies_are_pinned():
    for line in (REPO_ROOT / "src" / "requirements.txt").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "-")):
            continue
        assert re.search(r"[~=<]", line), f"{line!r} is unbounded"



# ══════════════════════════════════════════════════════════════════════════
#  Per-task verification
# ══════════════════════════════════════════════════════════════════════════

def test_operators_that_never_log_are_not_reported_as_missing():
    """Found by the end-to-end test against the live service.

    EmptyOperator runs no code on a worker, so MWAA Serverless creates NO CloudWatch
    log stream for it. Treating the absent stream as "not flushed yet" reported a
    perfectly healthy run as incomplete forever: a DAG of
    EmptyOperator -> BashOperator -> EmptyOperator came back with
    all_tasks_succeeded: false and "call again to confirm" that would never change.
    """
    outcome = operations._read_task_outcomes.__wrapped__ \
        if hasattr(operations._read_task_outcomes, "__wrapped__") \
        else operations._read_task_outcomes

    # Drive the classifier directly: one task logged, two never will.
    import types

    class Logs:
        exceptions = types.SimpleNamespace(ResourceNotFoundException=KeyError)

        def describe_log_streams(self, **kwargs):
            return {"logStreams": [{"logStreamName":
                                    "workflow_id=w/run_id=r/task_id=say_hello/attempt=1.log"}]}

        def get_log_events(self, **kwargs):
            return {"events": [{"message": json.dumps(
                {"final_state": "success", "duration": 1.65})}]}

    original = operations.boto3.client
    operations.boto3.client = lambda name, **kw: Logs()
    try:
        result = outcome(
            "arn:aws:airflow-serverless:us-east-1:111122223333:workflow/w",
            "r",
            ["start", "say_hello", "finish"],
            include_logs=False,
            max_error_lines=3,
            silent_tasks={"start", "finish"},
        )
    finally:
        operations.boto3.client = original

    assert result["succeeded"] == ["say_hello"]
    assert result["no_log_stream_expected"] == ["finish", "start"]
    assert result["no_logs_yet"] == []
    assert result["all_tasks_succeeded"] is True


def test_a_genuinely_missing_log_stream_is_still_reported():
    """The fix must not turn every absent stream into a pass."""
    import types

    class Logs:
        exceptions = types.SimpleNamespace(ResourceNotFoundException=KeyError)

        def describe_log_streams(self, **kwargs):
            return {"logStreams": []}

        def get_log_events(self, **kwargs):
            return {"events": []}

    original = operations.boto3.client
    operations.boto3.client = lambda name, **kw: Logs()
    try:
        result = operations._read_task_outcomes(
            "arn:aws:airflow-serverless:us-east-1:111122223333:workflow/w",
            "r", ["real_task"], include_logs=False, max_error_lines=3,
            silent_tasks=frozenset(),
        )
    finally:
        operations.boto3.client = original

    assert result["no_logs_yet"] == ["real_task"]
    assert result["all_tasks_succeeded"] is False


def test_no_log_operator_set_is_derived_from_the_allowlist():
    import schema

    for short in operations._NO_LOG_STREAM_OPERATORS:
        assert short in schema.SUPPORTED_OPERATORS



def test_demo_docs_do_not_claim_every_template_is_self_contained():
    """The docstrings used to say the demos "run in an empty account" while the tool
    itself reported REPLACE_ME params for 14 of the 29 — a customer would provision a
    stack, fail on a placeholder and tear it back down."""
    import inspect

    import app

    for fn in (tools.generate_yaml, app.mcp_server.tool_implementations["generate_dag_yaml"]):
        doc = inspect.getdoc(fn) or ""
        assert "empty account" not in doc, \
            "this claim is false for the templates that need stack-generated identifiers"
        assert "params_you_must_set" in doc, "the docstring must point at the honest field"

    readme = (REPO_ROOT / "README.md").read_text()
    assert "params_you_must_set" in readme


def test_self_contained_demo_list_in_the_docs_is_accurate():
    """The tool docstring names the templates that need no params. Keep it true."""
    import inspect

    import app

    actually_self_contained = {
        service for service in ALL_DEMO_SERVICES
        if not tools.generate_yaml("d", service).get("params_you_must_set")
    }
    doc = inspect.getdoc(app.mcp_server.tool_implementations["generate_dag_yaml"]) or ""
    claimed_block = doc.split("Self-contained today (no params required):", 1)
    assert len(claimed_block) == 2, "the docstring should list the self-contained templates"
    claimed = {s.strip(" .\n") for s in claimed_block[1].split("Args:")[0].replace("\n", " ").split(",")}
    claimed = {c for c in claimed if c}
    assert claimed == actually_self_contained, (
        f"docstring claims {sorted(claimed - actually_self_contained)} are self-contained "
        f"when they are not, and omits {sorted(actually_self_contained - claimed)}"
    )



# --- The code-bundle -> python_callable -> XCom path -------------------------
# This mirrors, in a test, the definition that was verified end to end against
# live MWAA Serverless: a PythonOperator whose callable lives in a code bundle,
# and a downstream task that pulls its return value out of XCom.

LIVE_VERIFIED_DAG = """
mcp_e4_python:
  schedule: null
  params:
    bucket: some-bucket
  tasks:
    run_python:
      operator: airflow.providers.standard.operators.python.PythonOperator
      python_callable: transform.summarise
    read_xcom:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: echo count={{ ti.xcom_pull(task_ids='run_python')['key_count'] }}
      dependencies:
      - run_python
"""

LIVE_VERIFIED_CALLABLE = '''
import boto3


def summarise(**context):
    bucket = context["params"]["bucket"]
    keys = boto3.client("s3").list_objects_v2(Bucket=bucket).get("Contents", [])
    return {"key_count": len(keys), "ran": True}
'''


def test_code_bundle_carries_the_callable_named_by_the_dag():
    """python_callable: transform.summarise must resolve to transform.py in the bundle
    root. MWAA Serverless imports it by that dotted path, so a nested path or a
    renamed function is a deploy-time failure, not a validation warning."""
    import base64
    import io
    import zipfile

    import codebundle

    bundle = codebundle.build_code_bundle({"transform.py": LIVE_VERIFIED_CALLABLE})
    assert not bundle.get("errors"), bundle.get("errors")

    zf = zipfile.ZipFile(io.BytesIO(base64.b64decode(bundle["zip_base64"])))
    assert "transform.py" in zf.namelist(), \
        f"the module must sit at the bundle root, got {zf.namelist()}"
    assert "def summarise" in zf.read("transform.py").decode()


def test_consistency_check_accepts_the_live_verified_pair():
    """The local cross-check must agree that this DAG and this bundle match. It ran
    green against the service, so a complaint here is a false positive."""
    import codebundle

    result = codebundle.check_dag_code_consistency(
        LIVE_VERIFIED_DAG, {"transform.py": LIVE_VERIFIED_CALLABLE}
    )
    assert not result.get("errors"), result.get("errors")


def test_xcom_pull_task_ids_is_not_reported_as_an_unknown_jinja_variable():
    """Regression: `task_ids=` inside xcom_pull is a keyword argument, not a Jinja
    variable. Flagging it made the documented XCom idiom look broken."""
    import validator

    result = validator.validate(LIVE_VERIFIED_DAG)
    assert result["valid"], result["errors"]
    noise = [
        w for w in result["warnings"] + result["hints"]
        if "task_ids" in w or "xcom_pull" in w
    ]
    assert not noise, f"the XCom idiom should not produce diagnostics: {noise}"


# --- preflight residue ------------------------------------------------------


def test_preflight_reports_the_log_group_it_cannot_clean_up(monkeypatch):
    """preflight deletes the throwaway workflow and its S3 objects, but the service
    creates a CloudWatch log group that outlives the workflow. Verified live: the
    group is left behind empty with no retention policy. Callers must be told."""
    arn = "arn:aws:airflow-serverless:us-west-1:111122223333:workflow/preflight-abc-XyZ"

    class Client:
        def create_workflow(self, **kwargs):
            return {"WorkflowArn": arn, "Warnings": []}

        def delete_workflow(self, **kwargs):
            return {}

    class S3:
        def put_object(self, **kwargs):
            return {}

        def delete_object(self, **kwargs):
            return {}

    monkeypatch.setattr(operations, "_get_client", lambda: Client())
    monkeypatch.setattr(operations.boto3, "client", lambda name, **kw: S3())

    result = operations.preflight_definition(
        "d:\n  tasks:\n    t:\n      operator: airflow.providers.standard.operators.empty.EmptyOperator\n",
        "some-bucket",
        "arn:aws:iam::111122223333:role/r",
    )

    assert result["valid"] is True
    assert result["cleanup"] == "throwaway workflow deleted"
    assert result["log_group_residue"] == "/aws/mwaa-serverless/preflight-abc-XyZ/"
    assert "outlives" in result["log_group_residue_note"]



# --- CI ---------------------------------------------------------------------


def test_ci_workflow_runs_the_four_checks_the_readme_promises():
    """The README tells contributors CI runs these. If a step is dropped from the
    workflow, the README becomes a false claim about what is enforced."""
    workflow = REPO_ROOT.parent.parent / ".github" / "workflows" / "serverless-mcp.yml"
    assert workflow.exists(), f"the README links to {workflow}, which does not exist"

    spec = yaml.safe_load(workflow.read_text())
    commands = " ".join(
        str(step.get("run", "")) for step in spec["jobs"]["check"]["steps"]
    )
    for expected in ("ruff check", "pytest", "cfn-lint template.yaml", "bandit -r src/"):
        assert expected in commands, f"CI no longer runs {expected!r}"


def test_ci_workflow_is_scoped_to_this_sample_and_needs_no_credentials():
    """This sample lives in a shared repository. Its CI must not run on unrelated
    changes, and must not be granted anything beyond reading the checkout."""
    workflow = REPO_ROOT.parent.parent / ".github" / "workflows" / "serverless-mcp.yml"
    spec = yaml.safe_load(workflow.read_text())

    # PyYAML parses the bare key `on:` as the boolean True.
    triggers = spec[True]
    for event in ("push", "pull_request"):
        paths = triggers[event]["paths"]
        assert any(p.startswith("serverless/mcp/") for p in paths), \
            f"{event} is not path-scoped to serverless/mcp"

    assert spec["permissions"] == {"contents": "read"}, \
        "CI needs nothing but read access to the checkout"
    body = workflow.read_text()
    for forbidden in ("aws-actions/configure-aws-credentials", "id-token", "secrets."):
        assert forbidden not in body, f"CI must not use {forbidden!r}"
