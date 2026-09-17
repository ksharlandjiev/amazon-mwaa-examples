# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""YAML generation for MWAA Serverless DAG factory definitions.

Validation lives in validator.py; structured assembly lives in builder.py.
Everything generated here is passed through validator.repair() before it is
returned, so template output is guaranteed to match the schema the service
actually accepts.
"""

import re
import yaml
import validator
from schema import (SUPPORTED_OPERATORS, OPERATOR_REQUIRED_PARAMS,
                    OPERATOR_XCOM_RETURNS, SENSOR_SAFETY_DEFAULTS, is_sensor)
from constraints import (
    SUPPORTED_JINJA_VARIABLES, SUPPORTED_MACROS,
    VALIDATED_DAG_PARAMS, IGNORED_DAG_PARAMS,
    VALIDATED_TASK_PARAMS, IGNORED_TASK_PARAMS,
    AWS_BASE_OPERATOR_ATTRS, UNSUPPORTED_FEATURES,
    MWAA_API_ACTIONS, SERVICE_OVERVIEW,
    YAML_SCHEMA, AUTHORING_POLICY, PARAMETER_PASSING,
    CODE_SUPPORT, QUOTAS, OBSERVABILITY, DEFAULT_ARGS_ALLOWLIST,
    UNSUPPORTED_JINJA_REPLACEMENTS, RECENTLY_ADDED_FEATURES,
)

_JINJA_VAR_RE = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_.]*)")
_DURATION_RE = re.compile(r"^(\d+)([smhd])$")
_TASK_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
_DAG_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

_DURATION_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def validate_yaml(yaml_content: str) -> dict:
    """Validate DAG YAML against the real MWAA Serverless schema.

    Thin delegation to validator.validate() so there is exactly one source of
    truth for what the service accepts.
    """
    return validator.validate(yaml_content)


def repair_yaml(yaml_content: str) -> dict:
    """Rewrite common schema mistakes into the form the service accepts."""
    return validator.repair(yaml_content)


def _finalise(dag_dict, note=""):
    """Dump, repair and validate a generated DAG. Returns a structured result."""
    raw = yaml.dump(dag_dict, default_flow_style=False, sort_keys=False, width=4096, allow_unicode=True)
    fixed = validator.repair(raw)
    result = fixed["validation"] or validator.validate(fixed["repaired_yaml"])
    out = {
        "dag_yaml": fixed["repaired_yaml"],
        "valid": result["valid"],
        "errors": result["errors"],
        "warnings": result["warnings"],
        "hints": result["hints"],
        "summary": result["summary"],
        "normalisations_applied": fixed["changes"],
    }
    if note:
        out["note"] = note
    return out


# Matches the CloudFormation-outputs-via-XCom pattern, e.g.
#   {{ ti.xcom_pull(task_ids='create_glue_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}
_CFN_OUTPUT_XCOM_RE = re.compile(
    r"\{\{\s*ti\.xcom_pull\(\s*task_ids\s*=\s*['\"](?P<task>[^'\"]+)['\"]\s*\)"
    r"\s*\[\s*['\"]CreateStackResponse['\"]\s*\]"
    r"\s*\[\s*['\"]Outputs['\"]\s*\]"
    r"\s*\[\s*(?P<idx>\d+)\s*\]"
    r"\s*\[\s*['\"]OutputValue['\"]\s*\]\s*\}\}"
)


def _replace_cfn_output_xcoms(tasks, params):
    """Rewrite unresolvable CloudFormation-output XCom references into params.

    CloudFormationCreateStackOperator returns None, so reading
    ['CreateStackResponse']['Outputs'][n]['OutputValue'] from its XCom raises
    TypeError at run time (verified against the live service). These templates
    predate that finding. Each such reference is replaced with a DAG param whose
    default is an obvious placeholder, so the demo is honest about needing a real
    value instead of failing with a confusing NoneType error.

    Returns the list of param names introduced.
    """
    introduced = []

    def _fix(value, field):
        if not isinstance(value, str):
            return value

        def _sub(m):
            base = f"{m.group('task')}_output_{m.group('idx')}"
            base = base.replace("create_", "").replace("_stack", "")
            key = re.sub(r"[^a-zA-Z0-9_]", "_", base)
            if key not in params:
                params[key] = f"REPLACE_ME__{field}_from_stack_output_{m.group('idx')}"
                introduced.append(key)
            return "{{ params.%s }}" % key

        return _CFN_OUTPUT_XCOM_RE.sub(_sub, value)

    def _walk(node, field):
        if isinstance(node, dict):
            for k, v in list(node.items()):
                if isinstance(v, str):
                    node[k] = _fix(v, k)
                else:
                    _walk(v, k)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                if isinstance(v, str):
                    node[i] = _fix(v, field)
                else:
                    _walk(v, field)

    entries = tasks.values() if isinstance(tasks, dict) else tasks
    for tcfg in entries:
        if isinstance(tcfg, dict):
            _walk(tcfg, "value")
    return introduced




def generate_yaml(dag_id, service, description="", schedule="None", params=None):
    """Produce a demo DAG for one AWS service, to try that service out end to end.

    These templates provision their own prerequisites with CloudFormation and tear them
    down again, so they are a poor starting point for a real pipeline — a real pipeline
    should reference resources that already exist. Use plan_pipeline + build_dag_yaml
    for that.

    ALWAYS CHECK `params_you_must_set` IN THE RESPONSE. Only about half the templates are
    fully self-contained. For the rest, the DAG needs an identifier the stack itself
    GENERATES — a !Ref'd bucket name, a !GetAtt role ARN — and those cannot be known
    before the stack is created. CloudFormationCreateStackOperator returns None via XCom
    (verified against the live service), so reading stack Outputs from it fails at run
    time; the reference is therefore surfaced as a param with a REPLACE_ME default. A
    template with entries in `params_you_must_set` will provision its stack, fail the
    work task on the placeholder, and tear the stack down again unless you supply real
    values first.
    """
    templates = _get_service_templates()
    svc = service.lower().replace(" ", "_")
    if svc not in templates:
        return {"error": f"Unknown service '{service}'. Available: {', '.join(sorted(templates.keys()))}"}

    t = templates[svc]
    tasks = _prepare_template_tasks(t)
    resolved_params = dict(params or t.get("default_params") or {})
    _replace_cfn_output_xcoms(tasks, resolved_params)
    dag = {dag_id: {"schedule": schedule if schedule not in ("None", "none", "") else None,
                    "tasks": tasks}}
    for k, v in t.get("extra_fields", {}).items():
        if k == "dag_id":
            continue  # ignored by the service; the root key is the dag_id
        dag[dag_id][k] = v
    if description:
        dag[dag_id]["description"] = description
    if resolved_params:
        dag[dag_id]["params"] = resolved_params

    note = (
        "This is a self-contained DEMO for the '%s' service: it provisions its own "
        "prerequisites and cleans them up. For a production pipeline that uses existing "
        "resources, call plan_pipeline then build_dag_yaml instead." % svc
    )
    out = _finalise(dag, note=note)
    out["resource_naming"] = (
        "Every resource this demo creates is named with a '{{ ts_nodash }}' suffix, so the name "
        "is unique to the run that creates it. That is what makes the trigger_rule: all_done "
        "cleanup tasks safe: they can only ever delete a stack or bucket this run just created, "
        "never a pre-existing resource of yours that happened to share the default name."
    )
    # Any REPLACE_ME_ default is a value only the customer can supply. Report all of
    # them together — a placeholder that stays in the definition fails at run time,
    # and a plausible-looking default (an ECR URI, a docs example bucket) is worse
    # because it looks like it should work.
    placeholders = sorted(
        k for k, v in resolved_params.items()
        if isinstance(v, str) and "REPLACE_ME" in v
    )
    if placeholders:
        out["params_you_must_set"] = placeholders
        out["params_note"] = (
            "Each of these is a placeholder the demo cannot know: a stack output that only "
            "exists after the stack is created, or a Region-specific identifier. "
            "CloudFormationCreateStackOperator returns None via XCom, so reading stack Outputs "
            "from it fails at run time — that is why they are params. Set every one to a real "
            "value before deploying."
        )
    return out


def _apply_sensor_safety_defaults(tasks):
    """Apply the sensor cost defaults unless the template already sets them.

    These demo templates wait on CloudFormation stacks and Glue jobs, and a sensor
    holds a worker slot for its entire wait, which MWAA Serverless bills for, and
    Airflow's default timeout is 7 days. Reschedule mode is applied for the same reason:
    these templates wait on CloudFormation stacks and Glue jobs, exactly the case where
    holding a worker to poll is pure waste.
    """
    applied = []
    if not isinstance(tasks, dict):
        return applied
    for tid, tcfg in tasks.items():
        if not isinstance(tcfg, dict) or not is_sensor(tcfg.get("operator")):
            continue
        for key, value in SENSOR_SAFETY_DEFAULTS.items():
            if key not in tcfg:
                tcfg[key] = value
                applied.append(f"{tid}.{key}={value}")
    return applied


def _normalize_tasks_to_dict(tasks):
    """Convert list-format tasks to dict-format and flatten 'parameters' into task body."""
    if isinstance(tasks, dict):
        # Already dict format — just flatten any nested 'parameters'
        for tcfg in tasks.values():
            _flatten_parameters(tcfg)
        return tasks
    if not isinstance(tasks, list):
        return tasks
    result = {}
    for tcfg in tasks:
        tid = tcfg.get("task_id", "unknown")
        _flatten_parameters(tcfg)
        # Convert upstream_tasks to dependencies
        if "upstream_tasks" in tcfg:
            tcfg["dependencies"] = tcfg.pop("upstream_tasks")
        if "downstream_tasks" in tcfg:
            del tcfg["downstream_tasks"]
        result[tid] = tcfg
    return result


def _flatten_parameters(tcfg):
    """Move contents of 'parameters' dict to top level of task config."""
    params = tcfg.pop("parameters", None)
    if isinstance(params, dict):
        for k, v in params.items():
            if k not in tcfg:
                tcfg[k] = v


def _resolve_operator_fqns(tasks):
    """Replace short operator names with fully qualified names."""
    import copy
    resolved = copy.deepcopy(tasks)
    if isinstance(resolved, dict):
        for tcfg in resolved.values():
            op = tcfg.get("operator", "")
            if op in SUPPORTED_OPERATORS:
                tcfg["operator"] = SUPPORTED_OPERATORS[op]
    elif isinstance(resolved, list):
        for tcfg in resolved:
            op = tcfg.get("operator", "")
            if op in SUPPORTED_OPERATORS:
                tcfg["operator"] = SUPPORTED_OPERATORS[op]
    return resolved


def list_operators(service_filter=""):
    filt = service_filter.lower()
    return [{"name": k, "fqn": v} for k, v in SUPPORTED_OPERATORS.items() if not filt or filt in k.lower() or filt in v.lower()]


def _prepare_template_tasks(template):
    """The one preparation path every demo template goes through.

    Order matters: resolve short operator names to FQNs, normalise the two task shapes
    the templates are written in, drop attributes the service rejects, apply sensor
    cost defaults, then make demo-owned resource names run-unique.

    Having a single function for this is the point. get_service_tasks() used to skip
    the cleanup step, so callers of that tool got back tasks still carrying
    aws_conn_id, emr_conn_id and a redundant inner task_id — attributes the service
    either ignores or chokes on.
    """
    tasks = _resolve_operator_fqns(template["tasks"])
    tasks = _normalize_tasks_to_dict(tasks)
    _strip_service_rejected_task_keys(tasks)
    _apply_sensor_safety_defaults(tasks)
    _uniquify_owned_resource_names(tasks)
    return tasks


# Connection-style attributes MWAA Serverless has no way to honour: it supports no
# Airflow Connections, so an *_conn_id is either ignored or looked up and fails.
_CONN_ID_RE = re.compile(r"^\w*_?conn_id$")


def _strip_service_rejected_task_keys(tasks):
    """Remove per-task keys the service ignores or rejects. Returns what was removed."""
    removed = []
    entries = tasks.items() if isinstance(tasks, dict) else enumerate(tasks)
    for tid, tcfg in entries:
        if not isinstance(tcfg, dict):
            continue
        # The mapping key IS the task_id; a repeated inner one is redundant.
        if "task_id" in tcfg:
            tcfg.pop("task_id")
            removed.append(f"{tid}.task_id")
        for key in [k for k in tcfg if _CONN_ID_RE.match(k)]:
            tcfg.pop(key)
            removed.append(f"{tid}.{key}")
    return removed


def get_service_tasks(service):
    """Return the task definitions for a single service as a reusable block."""
    templates = _get_service_templates()
    svc = service.lower().replace(" ", "_")
    if svc not in templates:
        return {"error": f"Unknown service '{service}'. Available: {', '.join(sorted(templates.keys()))}"}
    t = templates[svc]
    tasks = _prepare_template_tasks(t)
    return {
        "service": svc,
        "tasks": tasks,
        "default_params": t.get("default_params", {}),
    }


def compose_dag_yaml(dag_id, services_config, description="", schedule="None", params=None):
    """Chain several self-contained service DEMO blocks into one DAG.

    Each block still provisions and tears down its own prerequisites, so the
    result is large. This is for demonstrating several services together, not for
    building a production pipeline — for that use plan_pipeline + build_dag_yaml,
    which produces one task per operation you actually asked for.

    services_config is a list of dicts:
      [
        {"service": "s3", "task_prefix": "s3", "depends_on": []},
        {"service": "glue", "task_prefix": "glue", "depends_on": ["s3"]},
      ]
    """
    import copy
    templates = _get_service_templates()
    all_tasks = {}
    all_params = {}
    service_task_ids = {}  # prefix -> [ordered task_ids]
    service_first_task = {}  # prefix -> first task_id
    service_last_work_task = {}  # prefix -> last non-cleanup task_id

    for cfg in services_config:
        svc = cfg["service"].lower().replace(" ", "_")
        prefix = cfg.get("task_prefix", svc).replace("-", "_")
        depends_on = cfg.get("depends_on", [])

        if svc not in templates:
            return {"error": f"Unknown service '{svc}'. Available: {', '.join(sorted(templates.keys()))}"}

        t = templates[svc]
        tasks = _prepare_template_tasks(t)

        # Prefix all task IDs and rewrite dependency references
        old_to_new = {}
        ordered_ids = []
        for tid in tasks:
            new_tid = f"{prefix}_{tid}"
            old_to_new[tid] = new_tid
            ordered_ids.append(new_tid)

        prefixed_tasks = {}
        for tid, tcfg in tasks.items():
            new_tid = old_to_new[tid]
            new_tcfg = copy.deepcopy(tcfg)
            # Rewrite dependencies
            if "dependencies" in new_tcfg:
                new_tcfg["dependencies"] = [old_to_new.get(d, d) for d in new_tcfg["dependencies"]]
            # Rewrite xcom_pull(task_ids='...') to the prefixed ids. Without this,
            # composing produced YAML referencing task ids that do not exist in the
            # composed DAG — invalid for 15 of the 29 services.
            _rewrite_xcom_task_ids(new_tcfg, old_to_new)
            prefixed_tasks[new_tid] = new_tcfg

        service_task_ids[prefix] = ordered_ids
        service_first_task[prefix] = ordered_ids[0] if ordered_ids else None

        # Find last non-cleanup task (not trigger_rule: all_done)
        last_work = ordered_ids[0] if ordered_ids else None
        for new_tid in ordered_ids:
            if prefixed_tasks[new_tid].get("trigger_rule") == "all_done":
                break
            last_work = new_tid
        service_last_work_task[prefix] = last_work

        # Wire cross-service dependencies
        if depends_on and service_first_task[prefix]:
            first_task = prefixed_tasks[service_first_task[prefix]]
            deps = first_task.get("dependencies", [])
            for upstream_prefix in depends_on:
                up = upstream_prefix.replace("-", "_")
                if up in service_last_work_task and service_last_work_task[up]:
                    deps.append(service_last_work_task[up])
            if deps:
                first_task["dependencies"] = deps

        all_tasks.update(prefixed_tasks)

        # Merge params with prefix
        for k, v in t.get("default_params", {}).items():
            all_params[f"{prefix}_{k}"] = v

        # Rewrite param references in task values
        for tcfg in prefixed_tasks.values():
            _rewrite_param_refs(tcfg, prefix)

    # Build DAG
    dag = {dag_id: {"schedule": schedule if schedule not in ("None", "none", "") else None,
                    "tasks": all_tasks}}
    if description:
        dag[dag_id]["description"] = description
    merged_params = {**(params or {})}
    for k, v in all_params.items():
        if k not in merged_params:
            merged_params[k] = v

    # CloudFormationCreateStackOperator returns None, so reading its Outputs from XCom
    # fails at run time. generate_yaml has always stripped this pattern; compose_dag_yaml
    # did not, so the same defect came back through the other entry point.
    _replace_cfn_output_xcoms(all_tasks, merged_params)

    if merged_params:
        dag[dag_id]["params"] = merged_params

    return _finalise(dag, note=(
        "Composed from self-contained demo blocks, so it includes provisioning and cleanup "
        "tasks for every service. For a production pipeline use plan_pipeline + build_dag_yaml."
    ))


def _rewrite_param_refs(obj, prefix):
    """Rewrite {{ params.X }} references to {{ params.prefix_X }} in all string values."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and "{{ params." in v:
                obj[k] = re.sub(r"\{\{\s*params\.(\w+)\s*\}\}", r"{{ params." + prefix + r"_\1 }}", v)
            else:
                _rewrite_param_refs(v, prefix)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, str) and "{{ params." in v:
                obj[i] = re.sub(r"\{\{\s*params\.(\w+)\s*\}\}", r"{{ params." + prefix + r"_\1 }}", v)
            else:
                _rewrite_param_refs(v, prefix)


_XCOM_TASK_IDS_RE = re.compile(r"(task_ids\s*=\s*)(['\"])([^'\"]+)\2")


def _rewrite_xcom_task_ids(obj, old_to_new):
    """Rewrite xcom_pull(task_ids='old') to the composed DAG's prefixed task ids."""
    def _fix(value):
        def _sub(m):
            return f"{m.group(1)}{m.group(2)}{old_to_new.get(m.group(3), m.group(3))}{m.group(2)}"
        return _XCOM_TASK_IDS_RE.sub(_sub, value)

    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str):
                obj[k] = _fix(v)
            else:
                _rewrite_xcom_task_ids(v, old_to_new)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, str):
                obj[i] = _fix(v)
            else:
                _rewrite_xcom_task_ids(v, old_to_new)


# Params that name a resource the DEMO ITSELF creates and then deletes. Every
# reference to these is made unique per run — see _uniquify_owned_resource_names.
_RUN_SCOPED_PARAMS = ("stack_name", "bucket_name")

# ts_nodash is unique per logical run (e.g. 20260917T142530) and is on the supported
# Jinja list. Bucket names must be lowercase, so they get the |lower form.
_RUN_SUFFIX = "{{ ts_nodash }}"
_RUN_SUFFIX_LOWER = "{{ ts_nodash | lower }}"

_OWNED_PARAM_RE = re.compile(
    r"\{\{\s*params\.(" + "|".join(_RUN_SCOPED_PARAMS) + r")\s*\}\}"
    r"(?P<existing>-\{\{\s*ds_nodash\s*\}\})?"
)


def _uniquify_owned_resource_names(tasks):
    """Make every demo-created resource name unique to the run that creates it.

    This is what makes the templates' `trigger_rule: all_done` cleanup safe.
    Previously a template used a FIXED default stack name — the athena demo used
    `covid-lake-stack`, the name AWS's own COVID-19 data-lake walkthrough uses, and
    the cloudformation demo used the generic `my-cfn-stack`. In an account that
    already had that stack, create_stack failed with AlreadyExistsException, the work
    tasks failed, and then the unconditional delete_stack ran anyway and DELETED THE
    CUSTOMER'S STACK. constraints.AUTHORING_POLICY says it plainly: do not add cleanup
    tasks unless the DAG itself created the resource.

    Appending the run timestamp makes a collision impossible, so the DAG can only ever
    delete a stack it just created. Returns the number of references rewritten.
    """
    rewritten = 0

    def _fix(value):
        nonlocal rewritten

        def _sub(m):
            nonlocal rewritten
            rewritten += 1
            param = m.group(1)
            # ds_nodash (date only) is not unique across runs on the same day.
            suffix = _RUN_SUFFIX_LOWER if param == "bucket_name" else _RUN_SUFFIX
            return "{{ params.%s }}-%s" % (param, suffix)

        return _OWNED_PARAM_RE.sub(_sub, value)

    def _walk(node):
        if isinstance(node, dict):
            for k, v in list(node.items()):
                if isinstance(v, str):
                    node[k] = _fix(v)
                else:
                    _walk(v)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                if isinstance(v, str):
                    node[i] = _fix(v)
                else:
                    _walk(v)

    entries = tasks.values() if isinstance(tasks, dict) else tasks
    for tcfg in entries:
        if isinstance(tcfg, dict):
            _walk(tcfg)
    return rewritten


def list_unsupported():
    return UNSUPPORTED_FEATURES


def get_constraints():
    return {
        "yaml_schema": YAML_SCHEMA,
        "authoring_policy": AUTHORING_POLICY,
        "parameter_passing": PARAMETER_PASSING,
        "supported_jinja_variables": sorted(SUPPORTED_JINJA_VARIABLES),
        "supported_macros": sorted(SUPPORTED_MACROS),
        "unsupported_jinja_variables": UNSUPPORTED_JINJA_REPLACEMENTS,
        "validated_dag_params": VALIDATED_DAG_PARAMS,
        "ignored_dag_params": sorted(IGNORED_DAG_PARAMS),
        "validated_task_params": VALIDATED_TASK_PARAMS,
        "ignored_task_params": sorted(IGNORED_TASK_PARAMS),
        "default_args_allowlist": sorted(DEFAULT_ARGS_ALLOWLIST),
        "aws_base_operator_attrs": AWS_BASE_OPERATOR_ATTRS,
        "unsupported_features": UNSUPPORTED_FEATURES,
        "recently_added_features": RECENTLY_ADDED_FEATURES,
        "python_bash_code_support": CODE_SUPPORT,
        "quotas": QUOTAS,
        "observability": OBSERVABILITY,
        "mwaa_api_actions": MWAA_API_ACTIONS,
    }


def get_dag_yaml_spec():
    """The authoritative YAML schema, with a worked example and the exact
    service error each mistake produces."""
    return {
        "schema": YAML_SCHEMA,
        "parameter_passing": PARAMETER_PASSING,
        "authoring_policy": AUTHORING_POLICY,
        "default_args_allowlist": sorted(DEFAULT_ARGS_ALLOWLIST),
        "quotas": QUOTAS,
        "python_bash": {
            "operators": CODE_SUPPORT["operators"],
            "how_code_is_delivered": CODE_SUPPORT["how_code_is_delivered"],
            "when_to_use": CODE_SUPPORT["when_to_use"],
        },
    }


def describe_operator(operator: str):
    """Required arguments and XCom behaviour for one operator."""
    from schema import resolve_operator_fqn
    fqn, short, was_short = resolve_operator_fqn(operator)
    if not fqn:
        bare = operator.rsplit(".", 1)[-1] if operator else ""
        close = [k for k in SUPPORTED_OPERATORS if bare and bare.lower()[:6] in k.lower()][:8]
        return {"error": f"'{operator}' is not in the MWAA Serverless allowlist.",
                "did_you_mean": close}
    return {
        "operator": short,
        "operator_fqn": fqn,
        "use_this_value_in_yaml": fqn,
        "short_name_is_invalid": "MWAA Serverless rejects short operator names; always emit the FQN.",
        "required_arguments": OPERATOR_REQUIRED_PARAMS.get(short, []),
        "xcom_output": OPERATOR_XCOM_RETURNS.get(short, "Not documented — assume no useful XCom value."),
        "reminder": "Do not set aws_conn_id, region_name, verify or botocore_config.",
    }


def get_overview():
    return SERVICE_OVERVIEW


# ── Operator module -> least-privilege IAM actions ──
# Scoped to the calls the operators actually make. Previously every entry was a
# service-wide wildcard, which produced roles far broader than the DAG needed.
_OPERATOR_IAM_MAP = {
    "s3": {"actions": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
        "s3:GetBucketLocation", "s3:CreateBucket", "s3:DeleteBucket",
        "s3:GetBucketTagging", "s3:PutBucketTagging", "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts",
    ]},
    "s3_tables": {"actions": [
        "s3tables:CreateTableBucket", "s3tables:DeleteTableBucket", "s3tables:CreateNamespace",
        "s3tables:DeleteNamespace", "s3tables:CreateTable", "s3tables:DeleteTable",
        "s3tables:GetTableBucket", "s3tables:GetNamespace", "s3tables:GetTable",
    ]},
    "s3_vectors": {"actions": [
        "s3vectors:CreateVectorBucket", "s3vectors:DeleteVectorBucket",
        "s3vectors:CreateIndex", "s3vectors:DeleteIndex", "s3vectors:GetIndex",
    ]},
    "glue": {"actions": [
        "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:GetJob",
        "glue:BatchStopJobRun", "glue:CreateJob", "glue:UpdateJob",
        "glue:StartDataQualityRulesetEvaluationRun", "glue:GetDataQualityRulesetEvaluationRun",
        "glue:CreateDataQualityRuleset", "glue:GetDataQualityRuleset",
        "glue:StartDataQualityRuleRecommendationRun", "glue:GetDataQualityRuleRecommendationRun",
    ]},
    "glue_databrew": {"actions": ["databrew:StartJobRun", "databrew:DescribeJobRun", "databrew:DescribeJob"]},
    "glue_crawler": {"actions": ["glue:StartCrawler", "glue:GetCrawler", "glue:GetCrawlerMetrics",
                                 "glue:CreateCrawler", "glue:UpdateCrawler"]},
    "glue_catalog": {"actions": [
        "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:DeleteDatabase",
        "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:DeleteTable",
        "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition",
    ]},
    "glue_catalog_partition": {"actions": ["glue:GetPartition", "glue:GetPartitions", "glue:GetTable"]},
    "athena": {"actions": [
        "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
        "athena:StopQueryExecution", "athena:GetWorkGroup", "athena:GetDataCatalog",
        "glue:GetDatabase", "glue:GetTable", "glue:GetPartitions",
        "s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation",
        # Athena writes results as a multipart upload, so it needs both halves of that
        # API. Omitting ListMultipartUploadParts surfaces as an opaque "Server error"
        # from StartQueryExecution with nothing recorded in Athena's query history.
        "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]},
    "bedrock": {"actions": [
        "bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream",
        "bedrock:CreateModelCustomizationJob", "bedrock:GetModelCustomizationJob",
        "bedrock:Retrieve", "bedrock:RetrieveAndGenerate",
        "bedrock:CreateGuardrail", "bedrock:DeleteGuardrail", "bedrock:GetGuardrail",
    ]},
    "lambda_function": {"actions": [
        "lambda:InvokeFunction", "lambda:GetFunction", "lambda:GetFunctionConfiguration",
        "lambda:CreateFunction",
    ]},
    "step_function": {"actions": [
        "states:StartExecution", "states:DescribeExecution", "states:StopExecution",
        "states:DescribeStateMachine", "states:GetExecutionHistory",
    ]},
    "emr": {"actions": [
        "elasticmapreduce:RunJobFlow", "elasticmapreduce:AddJobFlowSteps",
        "elasticmapreduce:DescribeStep", "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:TerminateJobFlows", "elasticmapreduce:ListSteps",
        "elasticmapreduce:ModifyCluster",
        "emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun",
        "emr-serverless:CreateApplication", "emr-serverless:GetApplication",
        "emr-serverless:StartApplication", "emr-serverless:StopApplication",
        "emr-serverless:DeleteApplication",
        "emr-containers:StartJobRun", "emr-containers:DescribeJobRun",
        "emr-containers:CreateVirtualCluster",
    ]},
    "batch": {"actions": [
        "batch:SubmitJob", "batch:DescribeJobs", "batch:TerminateJob",
        "batch:DescribeJobQueues", "batch:DescribeComputeEnvironments",
        "batch:CreateComputeEnvironment",
    ]},
    "ecs": {"actions": [
        "ecs:RunTask", "ecs:DescribeTasks", "ecs:StopTask", "ecs:CreateCluster",
        "ecs:DeleteCluster", "ecs:DescribeClusters", "ecs:RegisterTaskDefinition",
        "ecs:DeregisterTaskDefinition", "ecs:DescribeTaskDefinition",
    ]},
    "eks": {"actions": [
        "eks:CreateCluster", "eks:DeleteCluster", "eks:DescribeCluster",
        "eks:CreateNodegroup", "eks:DeleteNodegroup", "eks:DescribeNodegroup",
        "eks:CreateFargateProfile", "eks:DeleteFargateProfile", "eks:DescribeFargateProfile",
    ]},
    "cloud_formation": {"actions": [
        "cloudformation:CreateStack", "cloudformation:DeleteStack",
        "cloudformation:DescribeStacks", "cloudformation:DescribeStackEvents",
        "cloudformation:DescribeStackResources", "cloudformation:GetTemplate",
    ]},
    "sagemaker": {"actions": [
        "sagemaker:CreateTrainingJob", "sagemaker:DescribeTrainingJob",
        "sagemaker:CreateProcessingJob", "sagemaker:DescribeProcessingJob",
        "sagemaker:CreateTransformJob", "sagemaker:DescribeTransformJob",
        "sagemaker:CreateModel", "sagemaker:DeleteModel",
        "sagemaker:CreateEndpoint", "sagemaker:CreateEndpointConfig", "sagemaker:DescribeEndpoint",
        "sagemaker:StartPipelineExecution", "sagemaker:DescribePipelineExecution",
        "sagemaker:CreateHyperParameterTuningJob", "sagemaker:DescribeHyperParameterTuningJob",
        "sagemaker:CreateAutoMLJob", "sagemaker:DescribeAutoMLJob",
    ]},
    "sagemaker_unified_studio": {"actions": [
        "sagemaker:StartNotebookInstance", "sagemaker:StopNotebookInstance",
        "sagemaker:DescribeNotebookInstance", "sagemaker:CreateNotebookInstance",
    ]},
    "rds": {"actions": [
        "rds:CreateDBInstance", "rds:DeleteDBInstance", "rds:DescribeDBInstances",
        "rds:StartDBInstance", "rds:StopDBInstance",
        "rds:CreateDBSnapshot", "rds:CopyDBSnapshot", "rds:DeleteDBSnapshot",
        "rds:DescribeDBSnapshots", "rds:StartExportTask", "rds:CancelExportTask",
        "rds:DescribeExportTasks", "rds:CreateEventSubscription", "rds:DeleteEventSubscription",
    ]},
    "redshift_cluster": {"actions": [
        "redshift:CreateCluster", "redshift:DeleteCluster", "redshift:DescribeClusters",
        "redshift:PauseCluster", "redshift:ResumeCluster",
        "redshift:CreateClusterSnapshot", "redshift:DeleteClusterSnapshot",
        "redshift:DescribeClusterSnapshots",
    ]},
    "redshift_data": {"actions": [
        "redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement",
        "redshift-data:DescribeStatement", "redshift-data:GetStatementResult",
        "redshift-data:CancelStatement",
        "redshift:GetClusterCredentials", "redshift-serverless:GetCredentials",
    ]},
    "dms": {"actions": [
        "dms:CreateReplicationTask", "dms:DeleteReplicationTask",
        "dms:DescribeReplicationTasks", "dms:StartReplicationTask", "dms:StopReplicationTask",
    ]},
    "ec2": {"actions": [
        "ec2:RunInstances", "ec2:TerminateInstances", "ec2:StartInstances",
        "ec2:StopInstances", "ec2:RebootInstances", "ec2:DescribeInstances",
        "ec2:DescribeInstanceStatus",
    ]},
    "sns": {"actions": ["sns:Publish", "sns:GetTopicAttributes"]},
    "sqs": {"actions": [
        "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage",
        "sqs:GetQueueAttributes", "sqs:GetQueueUrl",
    ]},
    "eventbridge": {"actions": [
        "events:PutEvents", "events:PutRule", "events:EnableRule",
        "events:DisableRule", "events:DescribeRule",
    ]},
    "comprehend": {"actions": [
        "comprehend:StartPiiEntitiesDetectionJob", "comprehend:DescribePiiEntitiesDetectionJob",
        "comprehend:CreateDocumentClassifier", "comprehend:DescribeDocumentClassifier",
    ]},
    "kinesis_analytics": {"actions": [
        "kinesisanalytics:CreateApplication", "kinesisanalytics:StartApplication",
        "kinesisanalytics:StopApplication", "kinesisanalytics:DescribeApplication",
    ]},
    "neptune": {"actions": ["rds:StartDBCluster", "rds:StopDBCluster", "rds:DescribeDBClusters"]},
    "glacier": {"actions": [
        "glacier:InitiateJob", "glacier:DescribeJob", "glacier:GetJobOutput",
        "glacier:UploadArchive",
    ]},
    "datasync": {"actions": [
        "datasync:StartTaskExecution", "datasync:DescribeTaskExecution",
        "datasync:CreateTask", "datasync:UpdateTask", "datasync:DeleteTask",
        "datasync:ListTasks", "datasync:DescribeTask", "datasync:ListLocations",
    ]},
    "appflow": {"actions": ["appflow:StartFlow", "appflow:DescribeFlow",
                            "appflow:DescribeFlowExecutionRecords", "appflow:UpdateFlow"]},
    "quicksight": {"actions": ["quicksight:CreateIngestion", "quicksight:DescribeIngestion"]},
    "dynamodb": {"actions": ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:DescribeTable"]},
    "opensearch_serverless": {"actions": ["aoss:BatchGetCollection", "aoss:APIAccessAll"]},
    "mwaa_serverless": {"actions": [
        "airflow-serverless:CreateWorkflow", "airflow-serverless:StartWorkflowRun",
        "airflow-serverless:GetWorkflow", "airflow-serverless:GetWorkflowRun",
    ]},
    "python": {"actions": []},
    "bash": {"actions": []},
    "empty": {"actions": []},
}

# Operator modules whose tasks hand a role to another AWS service, so the
# execution role needs iam:PassRole, mapped to the service principal that role is
# passed to. A dict rather than a chain of ternaries: the previous nested-ternary
# form ended in a bare `else "comprehend.amazonaws.com"`, so adding a module here
# without editing the chain would have silently granted PassRole to Comprehend.
_PASSROLE_SERVICE_PRINCIPALS = {
    "glue": "glue.amazonaws.com",
    "glue_crawler": "glue.amazonaws.com",
    "emr": "elasticmapreduce.amazonaws.com",
    "sagemaker": "sagemaker.amazonaws.com",
    "sagemaker_unified_studio": "sagemaker.amazonaws.com",
    "ecs": "ecs-tasks.amazonaws.com",
    "eks": "eks.amazonaws.com",
    "batch": "batch.amazonaws.com",
    "cloud_formation": "cloudformation.amazonaws.com",
    "dms": "dms.amazonaws.com",
    "kinesis_analytics": "kinesisanalytics.amazonaws.com",
    "datasync": "datasync.amazonaws.com",
    "rds": "rds.amazonaws.com",
    "comprehend": "comprehend.amazonaws.com",
}
_PASSROLE_MODULES = set(_PASSROLE_SERVICE_PRINCIPALS)

# Passing a role to CloudFormation is not "one more permission" — it is a full
# privilege-escalation primitive. CloudFormation acts with whatever role it is
# handed, so iam:PassRole to cloudformation.amazonaws.com plus CreateStack lets the
# holder do anything ANY passable role in the account can do. It is therefore never
# granted implicitly: the caller must name the exact role ARNs.
_PASSROLE_ESCALATION_PRINCIPALS = {"cloudformation.amazonaws.com"}

# Actions that destroy or terminate customer resources. Kept in a separate policy
# statement so a reviewer sees them as a group and can delete the statement outright
# when the DAG does not create the resources it operates on.
_DESTRUCTIVE_ACTION_VERBS = ("Delete", "Terminate", "Purge", "Destroy", "Deregister")

# Services whose ARNs carry no region or account (S3) or no account (Bedrock's
# foundation models). Everything else follows arn:PARTITION:SERVICE:REGION:ACCOUNT:*.
_ACCOUNTLESS_ARN_SERVICES = {"s3"}
_REGIONLESS_ARN_SERVICES = {"iam", "s3"}

_ACCOUNT_PLACEHOLDER = "${ACCOUNT_ID}"
_REGION_PLACEHOLDER = "${REGION}"
_BUCKET_PLACEHOLDER = "${BUCKET_NAME}"

# IAM role names are limited to 64 characters.
_MAX_ROLE_NAME_LEN = 64


def _partition_for_region(region: str) -> str:
    """AWS partition for a region. A policy written with arn:aws is inert in
    GovCloud and China: it matches nothing, so tasks lose permissions silently."""
    if region.startswith("us-gov-"):
        return "aws-us-gov"
    if region.startswith("cn-"):
        return "aws-cn"
    if region.startswith("us-iso"):
        return "aws-iso"
    return "aws"


def _role_name_for(dag_id: str) -> str:
    """A valid IAM role name for a DAG. IAM allows [\\w+=,.@-]{1,64}; a long dag_id
    would otherwise produce a name IAM rejects only when the CLI command is run."""
    safe = re.sub(r"[^A-Za-z0-9+=,.@_-]", "-", dag_id or "workflow")
    name = f"mwaa-serverless-{safe}-role"
    if len(name) <= _MAX_ROLE_NAME_LEN:
        return name
    keep = _MAX_ROLE_NAME_LEN - len("mwaa-serverless--role")
    return f"mwaa-serverless-{safe[:keep]}-role"


def _resources_for_actions(actions, partition, region, account):
    """Group actions by their IAM service prefix and give each group a scoped
    resource ARN, instead of one blanket Resource: "*".

    The ARN is derived from the action's own service prefix rather than a
    hand-maintained per-operator table, so it cannot drift out of sync with
    _OPERATOR_IAM_MAP. It scopes every grant to one service, in one region, in one
    account — narrower than "*" by construction, and still a pattern the reader is
    told to narrow further to individual job/table/queue ARNs.
    """
    by_service = {}
    for action in actions:
        service = action.split(":", 1)[0]
        by_service.setdefault(service, []).append(action)

    grouped = []
    for service, svc_actions in sorted(by_service.items()):
        if service == "s3":
            resources = [
                f"arn:{partition}:s3:::{_BUCKET_PLACEHOLDER}",
                f"arn:{partition}:s3:::{_BUCKET_PLACEHOLDER}/*",
            ]
        elif service == "bedrock":
            resources = [
                f"arn:{partition}:bedrock:{region}::foundation-model/*",
                f"arn:{partition}:bedrock:{region}:{account}:*",
            ]
        else:
            arn_region = "" if service in _REGIONLESS_ARN_SERVICES else region
            arn_account = "" if service in _ACCOUNTLESS_ARN_SERVICES else account
            resources = [f"arn:{partition}:{service}:{arn_region}:{arn_account}:*"]
        grouped.append((service, sorted(svc_actions), resources))
    return grouped


def generate_execution_role_policy(yaml_content, account_id: str = "", region: str = "",
                                   passable_role_arns=None,
                                   include_destructive_actions: bool = True):
    """Generate an IAM execution role policy and CLI commands for a given DAG YAML.

    Args:
        yaml_content: The DAG YAML to analyse.
        account_id: Your AWS account id. Supplied means real ARNs; omitted means the
            policy comes back with ${ACCOUNT_ID} placeholders that must be
            substituted before it can be applied.
        region: The region the workflow runs in. Also selects the ARN partition, so
            GovCloud and China get arn:aws-us-gov / arn:aws-cn rather than a policy
            that silently matches nothing.
        passable_role_arns: Exact role ARNs the DAG's tasks hand to other services.
            Required to grant iam:PassRole to CloudFormation, which is a
            privilege-escalation path when left unscoped.
        include_destructive_actions: Keep Delete*/Terminate* actions. Set false for a
            DAG that only reads and runs jobs.
    """
    import json as _json
    try:
        data = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return {"error": f"YAML parse error: {e}"}

    if not isinstance(data, dict) or not data:
        return {"error": "Root must be a non-empty YAML mapping keyed by dag_id."}

    all_actions = set()
    operator_services = set()
    unmapped = set()
    _extract_operators(data, operator_services)

    for svc in operator_services:
        if svc in _OPERATOR_IAM_MAP:
            all_actions.update(_OPERATOR_IAM_MAP[svc]["actions"])
        else:
            unmapped.add(svc)

    dag_id = list(data.keys())[0]
    role_name = _role_name_for(dag_id)

    account = account_id.strip() or _ACCOUNT_PLACEHOLDER
    arn_region = region.strip() or _REGION_PLACEHOLDER
    partition = _partition_for_region(region.strip())
    requires_substitution = sorted(
        {p for p, used in ((_ACCOUNT_PLACEHOLDER, not account_id.strip()),
                           (_REGION_PLACEHOLDER, not region.strip()),
                           (_BUCKET_PLACEHOLDER, "s3" in {a.split(":", 1)[0] for a in all_actions}))
         if used}
    )

    destructive = sorted(a for a in all_actions
                         if a.split(":", 1)[-1].startswith(_DESTRUCTIVE_ACTION_VERBS))
    non_destructive = sorted(all_actions - set(destructive))
    if not include_destructive_actions:
        withheld, granted = destructive, non_destructive
    else:
        withheld, granted = [], non_destructive

    # CloudWatch Logs is required for every workflow so task logs are captured.
    statements = [{
        "Sid": "WorkflowTaskLogging",
        "Effect": "Allow",
        "Action": ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup"],
        "Resource": f"arn:{partition}:logs:{arn_region}:{account}:log-group:/aws/mwaa-serverless/*",
    }]

    for service, svc_actions, resources in _resources_for_actions(
            granted, partition, arn_region, account):
        statements.append({
            "Sid": f"Operator{service.replace('-', '').title()}",
            "Effect": "Allow",
            "Action": svc_actions,
            "Resource": resources[0] if len(resources) == 1 else resources,
        })

    if include_destructive_actions and destructive:
        for service, svc_actions, resources in _resources_for_actions(
                destructive, partition, arn_region, account):
            statements.append({
                "Sid": f"Destructive{service.replace('-', '').title()}",
                "Effect": "Allow",
                "Action": svc_actions,
                "Resource": resources[0] if len(resources) == 1 else resources,
            })

    # iam:PassRole only when a task actually hands a role to another service, and
    # only to the roles the caller names.
    passrole_services = sorted(operator_services & _PASSROLE_MODULES)
    principals = sorted({_PASSROLE_SERVICE_PRINCIPALS[s] for s in passrole_services})
    named_roles = [r.strip() for r in (passable_role_arns or []) if str(r).strip()]
    escalation_principals = sorted(set(principals) & _PASSROLE_ESCALATION_PRINCIPALS)
    passrole_notes = []

    if principals and not named_roles and escalation_principals:
        # Refuse to emit the escalation grant with no role scope at all.
        principals = [p for p in principals if p not in _PASSROLE_ESCALATION_PRINCIPALS]
        passrole_notes.append(
            f"iam:PassRole to {', '.join(escalation_principals)} was NOT included. "
            f"CloudFormation acts with whatever role it is given, so an unscoped grant "
            f"lets this role do anything any passable role in the account can do — "
            f"including an AdministratorAccess role. Re-run with "
            f"passable_role_arns=['arn:{partition}:iam::{account}:role/your-cfn-service-role'] "
            f"to grant it against exactly that role."
        )

    if principals:
        passrole_resource = named_roles or [
            f"arn:{partition}:iam::{account}:role/mwaa-serverless-*"
        ]
        if not named_roles:
            passrole_notes.append(
                "iam:PassRole is scoped to roles named mwaa-serverless-* because no "
                "passable_role_arns were supplied. Name the exact role ARNs your tasks pass, "
                "or rename those roles to match the prefix. Never widen this to \"*\": the "
                "iam:PassedToService condition limits which SERVICE receives the role, not "
                "which role can be passed."
            )
        statements.append({
            "Sid": "PassRoleToAwsServices",
            "Effect": "Allow",
            "Action": ["iam:PassRole"],
            "Resource": passrole_resource[0] if len(passrole_resource) == 1 else passrole_resource,
            "Condition": {"StringEquals": {"iam:PassedToService": principals}},
        })

    policy = {"Version": "2012-10-17", "Statement": statements}
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "airflow-serverless.amazonaws.com"},
            "Action": "sts:AssumeRole",
            # Guards against a confused-deputy: without these the role can be assumed
            # on behalf of any account's workflow that reaches the service.
            "Condition": {
                "StringEquals": {"aws:SourceAccount": account},
                "ArnLike": {
                    "aws:SourceArn": f"arn:{partition}:airflow-serverless:{arn_region}:{account}:workflow/*"
                },
            },
        }],
    }

    needs_code = _dag_needs_code_bundle(data)
    scope_down = [
        f"Narrow each Operator* statement from arn:{partition}:SERVICE:{arn_region}:{account}:* "
        f"to the individual job, table, queue and state-machine ARNs the DAG names.",
    ]
    if "s3" in {a.split(":", 1)[0] for a in all_actions}:
        scope_down.append(
            f"Replace {_BUCKET_PLACEHOLDER} with your bucket. S3 needs both entries: the bucket "
            f"ARN for s3:ListBucket and the /* form for object actions."
        )
    if destructive and include_destructive_actions:
        scope_down.append(
            "Delete the Destructive* statement(s) unless this DAG created the resources it "
            "deletes. They currently allow "
            + ", ".join(destructive[:6]) + ("..." if len(destructive) > 6 else "")
            + " within the scoped ARNs."
        )
    if principals:
        scope_down.append(
            "Replace the iam:PassRole Resource with the exact role ARNs your tasks pass."
        )

    if requires_substitution:
        cli_commands = {
            "1_write_policy": (
                f"cat > {role_name}-policy.json <<'JSON'\n{_json.dumps(policy, indent=2)}\nJSON"
            ),
            "2_substitute_placeholders": (
                "# Edit the file and replace: " + ", ".join(requires_substitution)
                + f"\n#   e.g. sed -i.bak 's/\\{_ACCOUNT_PLACEHOLDER}/123456789012/g' "
                  f"{role_name}-policy.json"
            ),
            "3_create_role": (
                f"aws iam create-role --role-name {role_name} "
                f"--assume-role-policy-document '{_json.dumps(trust_policy)}'"
            ),
            "4_put_policy": (
                f"aws iam put-role-policy --role-name {role_name} "
                f"--policy-name {dag_id}-policy --policy-document file://{role_name}-policy.json"
            ),
            "5_get_role_arn": (
                f"aws iam get-role --role-name {role_name} --query 'Role.Arn' --output text"
            ),
        }
    else:
        cli_commands = {
            "create_role": (
                f"aws iam create-role --role-name {role_name} "
                f"--assume-role-policy-document '{_json.dumps(trust_policy)}'"
            ),
            "put_policy": (
                f"aws iam put-role-policy --role-name {role_name} "
                f"--policy-name {dag_id}-policy --policy-document '{_json.dumps(policy)}'"
            ),
            "get_role_arn": (
                f"aws iam get-role --role-name {role_name} --query 'Role.Arn' --output text"
            ),
        }

    return {
        "role_name": role_name,
        "trust_policy": trust_policy,
        "permissions_policy": policy,
        "partition": partition,
        "requires_substitution": requires_substitution or None,
        "detected_services": sorted(operator_services),
        "unmapped_services": sorted(unmapped) or None,
        "destructive_actions_granted": destructive if include_destructive_actions else None,
        "destructive_actions_withheld": withheld or None,
        "passrole_required_for": passrole_services or None,
        "passrole_notes": passrole_notes or None,
        "code_bundle_note": (
            "This DAG has Python/Bash tasks. Their code runs under this same role, so it also "
            "needs whatever AWS permissions the code itself calls."
        ) if needs_code else None,
        "cli_commands": cli_commands,
        "note": (
            "Every statement is scoped to one service, region and account — no Resource: \"*\". "
            + (f"The policy still contains {', '.join(requires_substitution)}; it cannot be "
               f"applied until those are replaced (pass account_id and region to get real ARNs). "
               if requires_substitution else "")
            + "Narrow to individual resource ARNs before production."
        ),
        "how_to_scope_down": scope_down,
    }


def _dag_needs_code_bundle(data):
    from schema import resolve_operator_fqn, CODE_OPERATORS
    for dag_cfg in data.values():
        tasks = (dag_cfg or {}).get("tasks")
        if isinstance(tasks, dict):
            entries = tasks.values()
        elif isinstance(tasks, list):
            entries = tasks
        else:
            continue
        for tcfg in entries:
            if not isinstance(tcfg, dict):
                continue
            _, short, _ = resolve_operator_fqn(tcfg.get("operator", "") or "")
            if short in CODE_OPERATORS:
                return True
    return False


def _extract_operators(obj, services):
    """Walk a DAG YAML and extract operator service modules."""
    if isinstance(obj, dict):
        op = obj.get("operator", "")
        if isinstance(op, str) and op:
            # Extract service module from FQN or short name
            if "." in op:
                # FQN like airflow.providers.amazon.aws.operators.s3.S3CreateBucketOperator
                parts = op.split(".")
                for i, p in enumerate(parts):
                    if p in ("operators", "sensors") and i + 1 < len(parts):
                        services.add(parts[i + 1])
                        break
            else:
                # Short name — look up FQN
                if op in SUPPORTED_OPERATORS:
                    fqn = SUPPORTED_OPERATORS[op]
                    parts = fqn.split(".")
                    for i, p in enumerate(parts):
                        if p in ("operators", "sensors") and i + 1 < len(parts):
                            services.add(parts[i + 1])
                            break
        for v in obj.values():
            _extract_operators(v, services)
    elif isinstance(obj, list):
        for v in obj:
            _extract_operators(v, services)


def _get_service_templates():
    """Templates using the array-based task schema with short operator names."""
    return {
        "s3": {
            "default_params": {"bucket_name": "mwaa-test-s3"},
            "extra_fields": {"dag_id": "s3_dag", "default_args": {"start_date": "2024-01-01"}},
            "tasks": {
                "create_test_bucket": {
                    "operator": "S3CreateBucketOperator",
                    "bucket_name": "{{ params.bucket_name }}-{{ ds_nodash }}",
                    "task_id": "create_test_bucket",
                },
                "create_test_object": {
                    "operator": "S3CreateObjectOperator",
                    "data": "Hello World",
                    "s3_bucket": "{{ params.bucket_name }}-{{ ds_nodash }}",
                    "s3_key": "test-file.txt",
                    "task_id": "create_test_object",
                    "replace": True,
                    "dependencies": ["create_test_bucket"],
                },
                "wait_for_key": {
                    "operator": "S3KeySensor",
                    "bucket_key": "test-file.txt",
                    "bucket_name": "{{ params.bucket_name }}-{{ ds_nodash }}",
                    "task_id": "wait_for_key",
                    "use_regex": False,
                    "wildcard_match": False,
                    "dependencies": ["create_test_object"],
                },
                "list_objects": {
                    "operator": "S3ListOperator",
                    "apply_wildcard": False,
                    "bucket": "{{ params.bucket_name }}-{{ ds_nodash }}",
                    "delimiter": "",
                    "task_id": "list_objects",
                    "dependencies": ["wait_for_key"],
                },
                "delete_objects": {
                    "operator": "S3DeleteObjectsOperator",
                    "bucket": "{{ params.bucket_name }}-{{ ds_nodash }}",
                    "keys": ["test-file.txt"],
                    "task_id": "delete_objects",
                    "dependencies": ["list_objects"],
                },
                "delete_test_bucket": {
                    "operator": "S3DeleteBucketOperator",
                    "aws_conn_id": "aws_default",
                    "bucket_name": "{{ params.bucket_name }}-{{ ds_nodash }}",
                    "force_delete": True,
                    "task_id": "delete_test_bucket",
                    "trigger_rule": "all_done",
                    "dependencies": ["delete_objects"],
                },
            },
        },
        "glue": {
            "default_params": {"stack_name": "mwaa-test-glue-stack", "script_key": "scripts/glue-sample-job.py"},
            "extra_fields": {"dag_id": "glue_dag", "default_args": {"start_date": "2024-01-01"}},
            "tasks": {
                "create_glue_stack": {
                    "operator": "CloudFormationCreateStackOperator",
                    "stack_name": "{{ params.stack_name }}",
                    "cloudformation_parameters": {
                        "StackName": "{{ params.stack_name }}",
                        "Capabilities": ["CAPABILITY_IAM"],
                        "TemplateBody": (
                            "AWSTemplateFormatVersion: '2010-09-09'\n"
                            "Resources:\n"
                            "  ScriptBucket:\n"
                            "    Type: AWS::S3::Bucket\n"
                            "    Properties:\n"
                            "      BucketEncryption:\n"
                            "        ServerSideEncryptionConfiguration:\n"
                            "          - ServerSideEncryptionByDefault:\n"
                            "              SSEAlgorithm: AES256\n"
                            "      PublicAccessBlockConfiguration:\n"
                            "        BlockPublicAcls: true\n"
                            "        BlockPublicPolicy: true\n"
                            "        IgnorePublicAcls: true\n"
                            "        RestrictPublicBuckets: true\n"
                            "  GlueRole:\n"
                            "    Type: AWS::IAM::Role\n"
                            "    Properties:\n"
                            "      AssumeRolePolicyDocument:\n"
                            "        Version: '2012-10-17'\n"
                            "        Statement:\n"
                            "          - Effect: Allow\n"
                            "            Principal:\n"
                            "              Service: glue.amazonaws.com\n"
                            "            Action: sts:AssumeRole\n"
                            "      ManagedPolicyArns:\n"
                            "        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole\n"
                            "      Policies:\n"
                            "        - PolicyName: S3Access\n"
                            "          PolicyDocument:\n"
                            "            Version: '2012-10-17'\n"
                            "            Statement:\n"
                            "              - Effect: Allow\n"
                            "                Action:\n"
                            "                  - s3:GetObject\n"
                            "                  - s3:PutObject\n"
                            "                  - s3:DeleteObject\n"
                            "                Resource: !Sub '${ScriptBucket.Arn}/*'\n"
                            "              - Effect: Allow\n"
                            "                Action: s3:ListBucket\n"
                            "                Resource: !GetAtt ScriptBucket.Arn\n"
                            "Outputs:\n"
                            "  BucketName:\n"
                            "    Value: !Ref ScriptBucket\n"
                            "  RoleName:\n"
                            "    Value: !Ref GlueRole\n"
                        ),
                    },
                    "task_id": "create_glue_stack",
                },
                "wait_for_glue_stack": {
                    "operator": "CloudFormationCreateStackSensor",
                    "stack_name": "{{ params.stack_name }}",
                    "task_id": "wait_for_glue_stack",
                    "dependencies": ["create_glue_stack"],
                },
                "create_script": {
                    "operator": "S3CreateObjectOperator",
                    "data": (
                        "#!/usr/bin/env python3\n"
                        "import sys\n"
                        "from awsglue.transforms import *\n"
                        "from awsglue.utils import getResolvedOptions\n"
                        "from pyspark.context import SparkContext\n"
                        "from awsglue.context import GlueContext\n"
                        "from awsglue.job import Job\n"
                        "from pyspark.sql import SparkSession\n"
                        "\n"
                        "args = getResolvedOptions(sys.argv, ['JOB_NAME'])\n"
                        "sc = SparkContext()\n"
                        "glueContext = GlueContext(sc)\n"
                        "spark = glueContext.spark_session\n"
                        "job = Job(glueContext)\n"
                        "job.init(args['JOB_NAME'], args)\n"
                        "\n"
                        'print("=== Glue 5.0 Job Started ===")\n'
                        'print(f"Python version: {sys.version}")\n'
                        'print(f"Spark version: {spark.version}")\n'
                        'print(f"Job name: {args[\'JOB_NAME\']}")\n'
                        "\n"
                        'data = [("test", 1, "success"), ("glue", 2, "running"), ("job", 3, "complete")]\n'
                        'columns = ["name", "id", "status"]\n'
                        "df = spark.createDataFrame(data, columns)\n"
                        'print("Created test DataFrame:")\n'
                        "df.show()\n"
                        "\n"
                        'print("=== Glue 5.0 Job Completed Successfully ===")\n'
                        "job.commit()\n"
                    ),
                    "replace": True,
                    "s3_bucket": "{{ ti.xcom_pull(task_ids='create_glue_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                    "s3_key": "{{ params.script_key }}",
                    "task_id": "create_script",
                    "dependencies": ["wait_for_glue_stack"],
                },
                "run_glue_job": {
                    "operator": "GlueJobOperator",
                    "concurrent_run_limit": 1,
                    "create_job_kwargs": {
                        "GlueVersion": "5.0",
                        "Command": {
                            "Name": "glueetl",
                            "ScriptLocation": "s3://{{ ti.xcom_pull(task_ids='create_glue_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}/{{ params.script_key }}",
                            "PythonVersion": "3",
                        },
                        "DefaultArguments": {
                            "--job-language": "python",
                            "--enable-metrics": "",
                            "--enable-continuous-cloudwatch-log": "true",
                        },
                        "MaxRetries": 0,
                        "Timeout": 60,
                    },
                    "iam_role_name": "{{ ti.xcom_pull(task_ids='create_glue_stack')['CreateStackResponse']['Outputs'][1]['OutputValue'] }}",
                    "job_desc": "AWS Glue Job with Airflow",
                    "job_name": "glue5-python3-job-{{ ds_nodash }}",
                    "task_id": "run_glue_job",
                    "wait_for_completion": True,
                    "dependencies": ["create_script"],
                },
                "delete_glue_stack": {
                    "operator": "CloudFormationDeleteStackOperator",
                    "stack_name": "{{ params.stack_name }}",
                    "task_id": "delete_glue_stack",
                    "trigger_rule": "all_done",
                    "dependencies": ["run_glue_job"],
                },
                "wait_for_glue_delete": {
                    "operator": "CloudFormationDeleteStackSensor",
                    "stack_name": "{{ params.stack_name }}",
                    "task_id": "wait_for_glue_delete",
                    "dependencies": ["delete_glue_stack"],
                },
            },
        },
        "athena": {
            "default_params": {
                # The COVID-19 open data lake this demo queries lives in us-east-2, and
                # the CloudFormation template that registers its Glue tables is served
                # from a us-east-2 bucket. Running the demo elsewhere means Athena reads
                # across Regions: it works, but it is slower and incurs transfer cost.
                "stack_name": "mwaa-demo-covid-lake",
                # REPLACE_ME_ so the value is reported in params_you_must_set instead of
                # silently shipping the AWS docs placeholder bucket, which fails with
                # NoSuchBucket on the first query.
                "output_location": "s3://REPLACE_ME_your-athena-results-bucket/athena-results/",
            },
            "extra_fields": {"dag_id": "athena_dag"},
            "tasks": {
                "create_stack": {
                    "operator": "CloudFormationCreateStackOperator",
                    "cloudformation_parameters": {
                        "StackName": "{{ params.stack_name }}",
                        "TemplateURL": "https://covid19-lake.s3.us-east-2.amazonaws.com/cfn/CovidLakeStack.template.json",
                        "TimeoutInMinutes": 5,
                        "OnFailure": "DELETE",
                    },
                    "stack_name": "{{ params.stack_name }}",
                    "task_id": "create_stack",
                    "dependencies": [],
                },
                "wait_for_stack_create": {
                    "operator": "CloudFormationCreateStackSensor",
                    "stack_name": "{{ params.stack_name }}",
                    "task_id": "wait_for_stack_create",
                    "dependencies": ["create_stack"],
                },
                "query_1": {
                    "operator": "AthenaOperator",
                    "database": "default",
                    "output_location": "{{ params.output_location }}",
                    "query": (
                        'SELECT cases.fips, admin2 as county, province_state, confirmed, growth_count,\n'
                        '  sum(num_licensed_beds) as num_licensed_beds,\n'
                        '  sum(num_staffed_beds) as num_staffed_beds,\n'
                        '  sum(num_icu_beds) as num_icu_beds\n'
                        'FROM "covid-19"."hospital_beds" beds,\n'
                        '  (SELECT fips, admin2, province_state, confirmed,\n'
                        '    last_value(confirmed) over (partition by fips order by last_update) -\n'
                        '    first_value(confirmed) over (partition by fips order by last_update) as growth_count,\n'
                        '    first_value(last_update) over (partition by fips order by last_update desc) as most_recent,\n'
                        '    last_update\n'
                        '  FROM "covid-19"."enigma_jhu"\n'
                        "  WHERE from_iso8601_timestamp(last_update) > now() - interval '200' day\n"
                        "    AND country_region = 'US') cases\n"
                        'WHERE beds.fips = cases.fips AND last_update = most_recent\n'
                        'GROUP BY cases.fips, confirmed, growth_count, admin2, province_state\n'
                        'ORDER BY growth_count desc\n'
                    ),
                    "task_id": "query_1",
                    "dependencies": ["wait_for_stack_create"],
                },
                "query_2": {
                    "operator": "AthenaOperator",
                    "database": "default",
                    "output_location": "{{ params.output_location }}",
                    "query": 'SELECT * FROM "covid-19"."world_cases_deaths_testing" order by "date" desc limit 10;\n',
                    "task_id": "query_2",
                    "dependencies": ["wait_for_stack_create"],
                },
                "query_3": {
                    "operator": "AthenaOperator",
                    "database": "default",
                    "output_location": "{{ params.output_location }}",
                    "query": (
                        'SELECT date, positive, negative, pending, hospitalized, death, total,\n'
                        '  deathincrease, hospitalizedincrease, negativeincrease, positiveincrease,\n'
                        '  sta.state AS state_abbreviation, abb.state\n'
                        'FROM "covid-19"."covid_testing_states_daily" sta\n'
                        'JOIN "covid-19"."us_state_abbreviations" abb ON sta.state = abb.abbreviation\n'
                        'limit 500;\n'
                    ),
                    "task_id": "query_3",
                    "dependencies": ["wait_for_stack_create"],
                },
                "delete_stack": {
                    "operator": "CloudFormationDeleteStackOperator",
                    "stack_name": "{{ params.stack_name }}",
                    "task_id": "delete_stack",
                    "trigger_rule": "all_done",
                    "dependencies": ["query_1", "query_3", "query_2"],
                },
                "wait_for_stack_delete": {
                    "operator": "CloudFormationDeleteStackSensor",
                    "stack_name": "{{ params.stack_name }}",
                    "task_id": "wait_for_stack_delete",
                    "trigger_rule": "all_success",
                    "dependencies": ["delete_stack"],
                },
            },
        },
        "bedrock": {
            "default_params": {"model_id": "amazon.nova-lite-v1:0"},
            "tasks": [
                {
                    "task_id": "invoke_model",
                    "operator": "BedrockInvokeModelOperator",
                    "parameters": {
                        "model_id": "{{ params.model_id }}",
                        "input_data": {
                            "messages": [{"role": "user", "content": [{"text": "What is Amazon MWAA?"}]}],
                        },
                    },
                },
            ],
        },
        "lambda": {
            "default_params": {"function_name": "mwaa-test-lambda", "stack_name": "mwaa-test-lambda-stack"},
            "tasks": [
                {
                    "task_id": "create_lambda_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  LambdaRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: lambda.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole\n"
                                "  TestFunction:\n"
                                "    Type: AWS::Lambda::Function\n"
                                "    Properties:\n"
                                "      FunctionName: !Ref FunctionName\n"
                                "      Runtime: python3.12\n"
                                "      Handler: index.handler\n"
                                "      Role: !GetAtt LambdaRole.Arn\n"
                                "      Code:\n"
                                "        ZipFile: |\n"
                                "          def handler(event, context):\n"
                                "              return {'statusCode': 200, 'body': 'Hello from MWAA Serverless test'}\n"
                                "Parameters:\n"
                                "  FunctionName:\n"
                                "    Type: String\n"
                                "    Default: mwaa-test-lambda\n"
                                "Outputs:\n"
                                "  FunctionName:\n"
                                "    Value: !Ref TestFunction\n"
                            ),
                            "Parameters": [{"ParameterKey": "FunctionName", "ParameterValue": "{{ params.function_name }}"}],
                        },
                    },
                },
                {
                    "task_id": "wait_for_lambda_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_lambda_stack"],
                },
                {
                    "task_id": "invoke_function",
                    "operator": "LambdaInvokeFunctionOperator",
                    "parameters": {
                        "function_name": "{{ params.function_name }}",
                        "payload": '{"action": "test"}',
                    },
                    "upstream_tasks": ["wait_for_lambda_stack"],
                },
                {
                    "task_id": "delete_lambda_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["invoke_function"],
                },
                {
                    "task_id": "wait_for_lambda_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_lambda_stack"],
                },
            ],
        },
        "emr_serverless": {
            "default_params": {"application_name": "mwaa-test-emr-app", "stack_name": "mwaa-test-emr-serverless-stack"},
            "tasks": [
                {
                    "task_id": "create_emr_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  EmrServerlessRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: emr-serverless.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess\n"
                                "  ScriptBucket:\n"
                                "    Type: AWS::S3::Bucket\n"
                                "    Properties:\n"
                                "      BucketEncryption:\n"
                                "        ServerSideEncryptionConfiguration:\n"
                                "          - ServerSideEncryptionByDefault:\n"
                                "              SSEAlgorithm: AES256\n"
                                "      PublicAccessBlockConfiguration:\n"
                                "        BlockPublicAcls: true\n"
                                "        BlockPublicPolicy: true\n"
                                "        IgnorePublicAcls: true\n"
                                "        RestrictPublicBuckets: true\n"
                                "Outputs:\n"
                                "  RoleArn:\n"
                                "    Value: !GetAtt EmrServerlessRole.Arn\n"
                                "  ScriptBucketName:\n"
                                "    Value: !Ref ScriptBucket\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_emr_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_emr_stack"],
                },
                {
                    "task_id": "create_application",
                    "operator": "EmrServerlessCreateApplicationOperator",
                    "parameters": {
                        "release_label": "emr-7.0.0",
                        "job_type": "SPARK",
                        # An EMR idempotency token, not a secret. Rendered per run so a
                        # retry does not submit the job twice.
                        "client_request_token": "{{ ds_nodash }}",  # nosec B105
                    },
                    "upstream_tasks": ["wait_for_emr_stack"],
                },
                {
                    "task_id": "stop_application",
                    "operator": "EmrServerlessStopApplicationOperator",
                    "parameters": {
                        "application_id": "{{ ti.xcom_pull(task_ids='create_application') }}",
                    },
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["create_application"],
                },
                {
                    "task_id": "delete_application",
                    "operator": "EmrServerlessDeleteApplicationOperator",
                    "parameters": {
                        "application_id": "{{ ti.xcom_pull(task_ids='create_application') }}",
                    },
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["stop_application"],
                },
                {
                    "task_id": "delete_emr_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["delete_application"],
                },
                {
                    "task_id": "wait_for_emr_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_emr_stack"],
                },
            ],
        },
        "batch": {
            "default_params": {"stack_name": "mwaa-test-batch-stack"},
            "tasks": [
                {
                    "task_id": "create_batch_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  BatchServiceRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: batch.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole\n"
                                "  ComputeEnv:\n"
                                "    Type: AWS::Batch::ComputeEnvironment\n"
                                "    Properties:\n"
                                "      Type: MANAGED\n"
                                "      ComputeResources:\n"
                                "        Type: FARGATE\n"
                                "        MaxvCpus: 4\n"
                                "        Subnets:\n"
                                "          - !Ref PublicSubnet\n"
                                "        SecurityGroupIds:\n"
                                "          - !Ref TaskSecurityGroup\n"
                                "      ServiceRole: !GetAtt BatchServiceRole.Arn\n"
                                "  VPC:\n"
                                "    Type: AWS::EC2::VPC\n"
                                "    Properties:\n"
                                "      CidrBlock: 10.0.0.0/16\n"
                                "      EnableDnsSupport: true\n"
                                "      EnableDnsHostnames: true\n"
                                "  PublicSubnet:\n"
                                "    Type: AWS::EC2::Subnet\n"
                                "    Properties:\n"
                                "      VpcId: !Ref VPC\n"
                                "      CidrBlock: 10.0.1.0/24\n"
                                "      MapPublicIpOnLaunch: true\n"
                                "  TaskSecurityGroup:\n"
                                "    Type: AWS::EC2::SecurityGroup\n"
                                "    Properties:\n"
                                "      GroupDescription: Egress-only group for demo Fargate tasks\n"
                                "      VpcId: !Ref VPC\n"
                                "      SecurityGroupEgress:\n"
                                "        - IpProtocol: -1\n"
                                "          CidrIp: 0.0.0.0/0\n"
                                "          Description: Outbound only; no inbound rules are defined\n"
                                "  IGW:\n"
                                "    Type: AWS::EC2::InternetGateway\n"
                                "  AttachIGW:\n"
                                "    Type: AWS::EC2::VPCGatewayAttachment\n"
                                "    Properties:\n"
                                "      VpcId: !Ref VPC\n"
                                "      InternetGatewayId: !Ref IGW\n"
                                # A public subnet with an attached IGW still has no route
                                # OUT without these three resources. Without them the
                                # Fargate task launches with a public IP, fails to pull its
                                # image from public.ecr.aws, and the demo hangs until the
                                # ECS timeout rather than reporting anything useful.
                                "  PublicRouteTable:\n"
                                "    Type: AWS::EC2::RouteTable\n"
                                "    Properties:\n"
                                "      VpcId: !Ref VPC\n"
                                "  PublicRoute:\n"
                                "    Type: AWS::EC2::Route\n"
                                "    DependsOn: AttachIGW\n"
                                "    Properties:\n"
                                "      RouteTableId: !Ref PublicRouteTable\n"
                                "      DestinationCidrBlock: 0.0.0.0/0\n"
                                "      GatewayId: !Ref IGW\n"
                                "  PublicSubnetRouteAssoc:\n"
                                "    Type: AWS::EC2::SubnetRouteTableAssociation\n"
                                "    Properties:\n"
                                "      SubnetId: !Ref PublicSubnet\n"
                                "      RouteTableId: !Ref PublicRouteTable\n"
                                "  JobQueue:\n"
                                "    Type: AWS::Batch::JobQueue\n"
                                "    Properties:\n"
                                "      Priority: 1\n"
                                "      ComputeEnvironmentOrder:\n"
                                "        - Order: 1\n"
                                "          ComputeEnvironment: !Ref ComputeEnv\n"
                                "  JobDef:\n"
                                "    Type: AWS::Batch::JobDefinition\n"
                                "    Properties:\n"
                                "      Type: container\n"
                                "      PlatformCapabilities:\n"
                                "        - FARGATE\n"
                                "      ContainerProperties:\n"
                                "        Image: public.ecr.aws/amazonlinux/amazonlinux:2023\n"
                                "        Command:\n"
                                "          - echo\n"
                                "          - Hello from MWAA Serverless Batch test\n"
                                "        ResourceRequirements:\n"
                                "          - Type: VCPU\n"
                                "            Value: '0.25'\n"
                                "          - Type: MEMORY\n"
                                "            Value: '512'\n"
                                "        ExecutionRoleArn: !GetAtt EcsTaskRole.Arn\n"
                                "  EcsTaskRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: ecs-tasks.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy\n"
                                "Outputs:\n"
                                "  JobQueueArn:\n"
                                "    Value: !Ref JobQueue\n"
                                "  JobDefArn:\n"
                                "    Value: !Ref JobDef\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_batch_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_batch_stack"],
                },
                {
                    "task_id": "submit_job",
                    "operator": "BatchOperator",
                    "parameters": {
                        "job_name": "batch-job-{{ ds_nodash }}",
                        "job_definition": "{{ ti.xcom_pull(task_ids='create_batch_stack')['CreateStackResponse']['Outputs'][1]['OutputValue'] }}",
                        "job_queue": "{{ ti.xcom_pull(task_ids='create_batch_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                        "wait_for_completion": True,
                    },
                    "upstream_tasks": ["wait_for_batch_stack"],
                },
                {
                    "task_id": "delete_batch_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["submit_job"],
                },
                {
                    "task_id": "wait_for_batch_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_batch_stack"],
                },
            ],
        },
        "step_functions": {
            "default_params": {"state_machine_name": "mwaa-test-sfn", "stack_name": "mwaa-test-sfn-stack"},
            "tasks": [
                {
                    "task_id": "create_sfn_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Parameters:\n"
                                "  StateMachineName:\n"
                                "    Type: String\n"
                                "    Default: mwaa-test-sfn\n"
                                "Resources:\n"
                                "  SfnRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: states.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "  TestStateMachine:\n"
                                "    Type: AWS::StepFunctions::StateMachine\n"
                                "    Properties:\n"
                                "      StateMachineName: !Ref StateMachineName\n"
                                "      RoleArn: !GetAtt SfnRole.Arn\n"
                                "      DefinitionString: |\n"
                                "        {\"Comment\": \"Test\", \"StartAt\": \"Pass\", \"States\": {\"Pass\": {\"Type\": \"Pass\", \"Result\": \"Hello from MWAA\", \"End\": true}}}\n"
                                "Outputs:\n"
                                "  StateMachineArn:\n"
                                "    Value: !Ref TestStateMachine\n"
                            ),
                            "Parameters": [{"ParameterKey": "StateMachineName", "ParameterValue": "{{ params.state_machine_name }}"}],
                        },
                    },
                },
                {
                    "task_id": "wait_for_sfn_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_sfn_stack"],
                },
                {
                    "task_id": "start_execution",
                    "operator": "StepFunctionStartExecutionOperator",
                    "parameters": {
                        "state_machine_arn": "{{ ti.xcom_pull(task_ids='create_sfn_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                        "state_machine_input": '{"key": "value"}',
                    },
                    "upstream_tasks": ["wait_for_sfn_stack"],
                },
                {
                    "task_id": "wait_for_execution",
                    "operator": "StepFunctionExecutionSensor",
                    "parameters": {
                        "execution_arn": "{{ ti.xcom_pull(task_ids='start_execution') }}",
                    },
                    "upstream_tasks": ["start_execution"],
                },
                {
                    "task_id": "delete_sfn_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_execution"],
                },
                {
                    "task_id": "wait_for_sfn_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_sfn_stack"],
                },
            ],
        },
        "redshift": {
            "default_params": {"database": "dev", "stack_name": "mwaa-test-redshift-stack"},
            "tasks": [
                {
                    "task_id": "create_redshift_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  AdminSecret:\n"
                                "    Type: AWS::SecretsManager::Secret\n"
                                "    Properties:\n"
                                "      Description: Generated admin password for the demo namespace\n"
                                "      GenerateSecretString:\n"
                                "        SecretStringTemplate: '{\"username\": \"admin\"}'\n"
                                "        GenerateStringKey: password\n"
                                "        PasswordLength: 32\n"
                                "        ExcludeCharacters: '\"@/\\\\'\n"
                                "  RedshiftNamespace:\n"
                                "    Type: AWS::RedshiftServerless::Namespace\n"
                                "    Properties:\n"
                                "      NamespaceName: mwaa-test-ns\n"
                                "      DbName: dev\n"
                                "      AdminUsername: admin\n"
                                # Generated and stored in Secrets Manager rather than written
                                # in the template. A literal here would otherwise end up in the
                                # DAG YAML and in the immutable workflow snapshot in S3.
                                "      AdminUserPassword: !Sub '{{resolve:secretsmanager:${AdminSecret}::password}}'\n"
                                "  RedshiftWorkgroup:\n"
                                "    Type: AWS::RedshiftServerless::Workgroup\n"
                                "    Properties:\n"
                                "      WorkgroupName: mwaa-test-wg\n"
                                "      NamespaceName: !Ref RedshiftNamespace\n"
                                "      BaseCapacity: 8\n"
                                "      PubliclyAccessible: false\n"
                                "Outputs:\n"
                                "  WorkgroupName:\n"
                                "    Value: mwaa-test-wg\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_redshift_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_redshift_stack"],
                },
                {
                    "task_id": "run_query",
                    "operator": "RedshiftDataOperator",
                    "parameters": {
                        "sql": "SELECT 1;",
                        "database": "{{ params.database }}",
                        "workgroup_name": "mwaa-test-wg",
                        "wait_for_completion": True,
                    },
                    "upstream_tasks": ["wait_for_redshift_stack"],
                },
                {
                    "task_id": "delete_redshift_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["run_query"],
                },
                {
                    "task_id": "wait_for_redshift_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_redshift_stack"],
                },
            ],
        },
        "sns": {
            "default_params": {"topic_name": "mwaa-test-sns-topic", "stack_name": "mwaa-test-sns-stack"},
            "tasks": [
                {
                    "task_id": "create_sns_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Parameters:\n"
                                "  TopicName:\n"
                                "    Type: String\n"
                                "    Default: mwaa-test-sns-topic\n"
                                "Resources:\n"
                                "  TestTopic:\n"
                                "    Type: AWS::SNS::Topic\n"
                                "    Properties:\n"
                                "      TopicName: !Ref TopicName\n"
                                "Outputs:\n"
                                "  TopicArn:\n"
                                "    Value: !Ref TestTopic\n"
                            ),
                            "Parameters": [{"ParameterKey": "TopicName", "ParameterValue": "{{ params.topic_name }}"}],
                        },
                    },
                },
                {
                    "task_id": "wait_for_sns_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_sns_stack"],
                },
                {
                    "task_id": "publish_message",
                    "operator": "SnsPublishOperator",
                    "parameters": {
                        "target_arn": "{{ ti.xcom_pull(task_ids='create_sns_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                        "message": "Hello from MWAA Serverless test",
                    },
                    "upstream_tasks": ["wait_for_sns_stack"],
                },
                {
                    "task_id": "delete_sns_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["publish_message"],
                },
                {
                    "task_id": "wait_for_sns_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_sns_stack"],
                },
            ],
        },
        "ecs": {
            "default_params": {"stack_name": "mwaa-test-ecs-stack"},
            "tasks": [
                {
                    "task_id": "create_ecs_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  VPC:\n"
                                "    Type: AWS::EC2::VPC\n"
                                "    Properties:\n"
                                "      CidrBlock: 10.0.0.0/16\n"
                                "      EnableDnsSupport: true\n"
                                "      EnableDnsHostnames: true\n"
                                "  PublicSubnet:\n"
                                "    Type: AWS::EC2::Subnet\n"
                                "    Properties:\n"
                                "      VpcId: !Ref VPC\n"
                                "      CidrBlock: 10.0.1.0/24\n"
                                "      MapPublicIpOnLaunch: true\n"
                                "  IGW:\n"
                                "    Type: AWS::EC2::InternetGateway\n"
                                "  AttachIGW:\n"
                                "    Type: AWS::EC2::VPCGatewayAttachment\n"
                                "    Properties:\n"
                                "      VpcId: !Ref VPC\n"
                                "      InternetGatewayId: !Ref IGW\n"
                                # A public subnet with an attached IGW still has no route
                                # OUT without these three resources. Without them the
                                # Fargate task launches with a public IP, fails to pull its
                                # image from public.ecr.aws, and the demo hangs until the
                                # ECS timeout rather than reporting anything useful.
                                "  PublicRouteTable:\n"
                                "    Type: AWS::EC2::RouteTable\n"
                                "    Properties:\n"
                                "      VpcId: !Ref VPC\n"
                                "  PublicRoute:\n"
                                "    Type: AWS::EC2::Route\n"
                                "    DependsOn: AttachIGW\n"
                                "    Properties:\n"
                                "      RouteTableId: !Ref PublicRouteTable\n"
                                "      DestinationCidrBlock: 0.0.0.0/0\n"
                                "      GatewayId: !Ref IGW\n"
                                "  PublicSubnetRouteAssoc:\n"
                                "    Type: AWS::EC2::SubnetRouteTableAssociation\n"
                                "    Properties:\n"
                                "      SubnetId: !Ref PublicSubnet\n"
                                "      RouteTableId: !Ref PublicRouteTable\n"
                                "  EcsCluster:\n"
                                "    Type: AWS::ECS::Cluster\n"
                                "    Properties:\n"
                                "      ClusterName: mwaa-test-ecs-cluster\n"
                                "  TaskRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: ecs-tasks.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy\n"
                                "  TaskDef:\n"
                                "    Type: AWS::ECS::TaskDefinition\n"
                                "    Properties:\n"
                                "      Family: mwaa-test-task\n"
                                "      Cpu: '256'\n"
                                "      Memory: '512'\n"
                                "      NetworkMode: awsvpc\n"
                                "      RequiresCompatibilities:\n"
                                "        - FARGATE\n"
                                "      ExecutionRoleArn: !GetAtt TaskRole.Arn\n"
                                "      ContainerDefinitions:\n"
                                "        - Name: test-container\n"
                                "          Image: public.ecr.aws/amazonlinux/amazonlinux:2023\n"
                                "          Command:\n"
                                "            - echo\n"
                                "            - Hello from MWAA Serverless ECS test\n"
                                "          Essential: true\n"
                                "Outputs:\n"
                                "  ClusterName:\n"
                                "    Value: !Ref EcsCluster\n"
                                "  TaskDefArn:\n"
                                "    Value: !Ref TaskDef\n"
                                "  SubnetId:\n"
                                "    Value: !Ref PublicSubnet\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_ecs_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_ecs_stack"],
                },
                {
                    "task_id": "run_task",
                    "operator": "EcsRunTaskOperator",
                    "parameters": {
                        "cluster": "mwaa-test-ecs-cluster",
                        "task_definition": "mwaa-test-task",
                        "launch_type": "FARGATE",
                        "overrides": {},
                        "network_configuration": {
                            "awsvpcConfiguration": {
                                "subnets": ["{{ ti.xcom_pull(task_ids='create_ecs_stack')['CreateStackResponse']['Outputs'][2]['OutputValue'] }}"],
                                "assignPublicIp": "ENABLED",
                            },
                        },
                    },
                    "upstream_tasks": ["wait_for_ecs_stack"],
                },
                {
                    "task_id": "delete_ecs_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["run_task"],
                },
                {
                    "task_id": "wait_for_ecs_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_ecs_stack"],
                },
            ],
        },
        "eks": {
            "default_params": {"cluster_name": "mwaa-test-eks", "stack_name": "mwaa-test-eks-stack"},
            "tasks": [
                {
                    "task_id": "create_eks_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  VPC:\n"
                                "    Type: AWS::EC2::VPC\n"
                                "    Properties:\n"
                                "      CidrBlock: 10.0.0.0/16\n"
                                "      EnableDnsSupport: true\n"
                                "      EnableDnsHostnames: true\n"
                                "  SubnetA:\n"
                                "    Type: AWS::EC2::Subnet\n"
                                "    Properties:\n"
                                "      VpcId: !Ref VPC\n"
                                "      CidrBlock: 10.0.1.0/24\n"
                                "      AvailabilityZone: !Select [0, !GetAZs '']\n"
                                "  SubnetB:\n"
                                "    Type: AWS::EC2::Subnet\n"
                                "    Properties:\n"
                                "      VpcId: !Ref VPC\n"
                                "      CidrBlock: 10.0.2.0/24\n"
                                "      AvailabilityZone: !Select [1, !GetAZs '']\n"
                                "  EksRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: eks.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/AmazonEKSClusterPolicy\n"
                                # A purpose-built group rather than the VPC default, which
                                # allows all traffic between anything attached to it.
                                "  ClusterSecurityGroup:\n"
                                "    Type: AWS::EC2::SecurityGroup\n"
                                "    Properties:\n"
                                "      GroupDescription: Egress-only group for the demo EKS control plane\n"
                                "      VpcId: !Ref VPC\n"
                                "      SecurityGroupEgress:\n"
                                "        - IpProtocol: -1\n"
                                "          CidrIp: 0.0.0.0/0\n"
                                "          Description: Outbound only; no inbound rules are defined\n"
                                "Outputs:\n"
                                "  RoleArn:\n"
                                "    Value: !GetAtt EksRole.Arn\n"
                                "  SubnetAId:\n"
                                "    Value: !Ref SubnetA\n"
                                "  SubnetBId:\n"
                                "    Value: !Ref SubnetB\n"
                                "  SecurityGroupId:\n"
                                "    Value: !Ref ClusterSecurityGroup\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_eks_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_eks_stack"],
                },
                {
                    "task_id": "create_cluster",
                    "operator": "EksCreateClusterOperator",
                    "parameters": {
                        "cluster_name": "{{ params.cluster_name }}",
                        "cluster_role_arn": "{{ ti.xcom_pull(task_ids='create_eks_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                        "resources_vpc_config": {
                            "subnetIds": [
                                "{{ ti.xcom_pull(task_ids='create_eks_stack')['CreateStackResponse']['Outputs'][1]['OutputValue'] }}",
                                "{{ ti.xcom_pull(task_ids='create_eks_stack')['CreateStackResponse']['Outputs'][2]['OutputValue'] }}",
                            ],
                            "securityGroupIds": [
                                "{{ ti.xcom_pull(task_ids='create_eks_stack')['CreateStackResponse']['Outputs'][3]['OutputValue'] }}",
                            ],
                        },
                    },
                    "upstream_tasks": ["wait_for_eks_stack"],
                },
                {
                    "task_id": "wait_for_cluster",
                    "operator": "EksClusterStateSensor",
                    "parameters": {"cluster_name": "{{ params.cluster_name }}", "target_state": "ACTIVE"},
                    "upstream_tasks": ["create_cluster"],
                },
                {
                    "task_id": "delete_cluster",
                    "operator": "EksDeleteClusterOperator",
                    "parameters": {"cluster_name": "{{ params.cluster_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_cluster"],
                },
                {
                    "task_id": "delete_eks_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["delete_cluster"],
                },
                {
                    "task_id": "wait_for_eks_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_eks_stack"],
                },
            ],
        },
        "emr": {
            "default_params": {"log_uri": "s3://amzn-s3-demo-bucket/emr-logs/", "stack_name": "mwaa-test-emr-stack"},
            "tasks": [
                {
                    "task_id": "create_emr_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  LogBucket:\n"
                                "    Type: AWS::S3::Bucket\n"
                                "    Properties:\n"
                                "      BucketEncryption:\n"
                                "        ServerSideEncryptionConfiguration:\n"
                                "          - ServerSideEncryptionByDefault:\n"
                                "              SSEAlgorithm: AES256\n"
                                "      PublicAccessBlockConfiguration:\n"
                                "        BlockPublicAcls: true\n"
                                "        BlockPublicPolicy: true\n"
                                "        IgnorePublicAcls: true\n"
                                "        RestrictPublicBuckets: true\n"
                                "  EmrServiceRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: elasticmapreduce.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2\n"
                                "  EmrEc2Role:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: ec2.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role\n"
                                "  EmrInstanceProfile:\n"
                                "    Type: AWS::IAM::InstanceProfile\n"
                                "    Properties:\n"
                                "      Roles:\n"
                                "        - !Ref EmrEc2Role\n"
                                "Outputs:\n"
                                "  LogBucketUri:\n"
                                "    Value: !Sub 's3://${LogBucket}/emr-logs/'\n"
                                "  ServiceRoleName:\n"
                                "    Value: !Ref EmrServiceRole\n"
                                "  InstanceProfileName:\n"
                                "    Value: !Ref EmrInstanceProfile\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_emr_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_emr_stack"],
                },
                {
                    "task_id": "create_job_flow",
                    "operator": "EmrCreateJobFlowOperator",
                    "parameters": {
                        "emr_conn_id": "aws_default",
                        "job_flow_overrides": {
                            "Name": "emr-cluster-{{ ds_nodash }}",
                            "LogUri": "{{ ti.xcom_pull(task_ids='create_emr_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                            "ReleaseLabel": "emr-7.0.0",
                            "ServiceRole": "{{ ti.xcom_pull(task_ids='create_emr_stack')['CreateStackResponse']['Outputs'][1]['OutputValue'] }}",
                            "JobFlowRole": "{{ ti.xcom_pull(task_ids='create_emr_stack')['CreateStackResponse']['Outputs'][2]['OutputValue'] }}",
                            "Instances": {"InstanceGroups": [{"Name": "Primary", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1}], "KeepJobFlowAliveWhenNoSteps": False, "TerminationProtected": False},
                        },
                    },
                    "upstream_tasks": ["wait_for_emr_stack"],
                },
                {
                    "task_id": "wait_for_job_flow",
                    "operator": "EmrJobFlowSensor",
                    "parameters": {"job_flow_id": "{{ ti.xcom_pull(task_ids='create_job_flow') }}", "target_states": ["TERMINATED"]},
                    "upstream_tasks": ["create_job_flow"],
                },
                {
                    "task_id": "delete_emr_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_job_flow"],
                },
                {
                    "task_id": "wait_for_emr_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_emr_stack"],
                },
            ],
        },
        "cloudformation": {
            "default_params": {"stack_name": "my-cfn-stack"},
            "tasks": [
                {
                    "task_id": "create_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}", "cloudformation_parameters": {"StackName": "{{ params.stack_name }}", "TemplateBody": "AWSTemplateFormatVersion: '2010-09-09'\nDescription: Sample stack\nResources:\n  Placeholder:\n    Type: AWS::CloudFormation::WaitConditionHandle\n"}},
                },
                {
                    "task_id": "wait_for_create",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_stack"],
                },
                {
                    "task_id": "delete_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_create"],
                },
                {
                    "task_id": "wait_for_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_stack"],
                },
            ],
        },
        "sagemaker": {
            "default_params": {
                "instance_type": "ml.m5.large",
                "stack_name": "mwaa-test-sagemaker-stack",
                # SageMaker's built-in image accounts differ PER REGION, and a processing
                # job cannot pull from another Region's ECR. Hardcoding the us-east-1
                # account (683313688378) meant this demo only ever worked in us-east-1 and
                # failed elsewhere with an image-pull error. Look yours up with:
                #   python -c "import sagemaker;
                #     print(sagemaker.image_uris.retrieve('sklearn','<your-region>',version='1.2-1'))"
                # or see docs.aws.amazon.com/sagemaker/latest/dg-ecr-paths/ecr-<region>.html
                "image_uri": "REPLACE_ME_sagemaker_sklearn_image_uri_for_your_region",
            },
            "tasks": [
                {
                    "task_id": "create_sagemaker_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  SageMakerRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: sagemaker.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      ManagedPolicyArns:\n"
                                "        - arn:aws:iam::aws:policy/AmazonSageMakerFullAccess\n"
                                "Outputs:\n"
                                "  RoleArn:\n"
                                "    Value: !GetAtt SageMakerRole.Arn\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_sagemaker_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_sagemaker_stack"],
                },
                {
                    "task_id": "start_processing",
                    "operator": "SageMakerProcessingOperator",
                    "parameters": {
                        "config": {
                            "ProcessingJobName": "processing-job-{{ ds_nodash }}",
                            "RoleArn": "{{ ti.xcom_pull(task_ids='create_sagemaker_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                            "ProcessingResources": {"ClusterConfig": {"InstanceCount": 1, "InstanceType": "{{ params.instance_type }}", "VolumeSizeInGB": 10}},
                            "AppSpecification": {"ImageUri": "{{ params.image_uri }}"},
                        },
                        "wait_for_completion": True,
                    },
                    "upstream_tasks": ["wait_for_sagemaker_stack"],
                },
                {
                    "task_id": "delete_sagemaker_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["start_processing"],
                },
                {
                    "task_id": "wait_for_sagemaker_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_sagemaker_stack"],
                },
            ],
        },
        "rds": {
            "default_params": {"stack_name": "mwaa-test-rds-stack"},
            "tasks": [
                {
                    "task_id": "create_rds_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  MasterSecret:\n"
                                "    Type: AWS::SecretsManager::Secret\n"
                                "    Properties:\n"
                                "      Description: Generated master password for the demo database\n"
                                "      GenerateSecretString:\n"
                                "        SecretStringTemplate: '{\"username\": \"admin\"}'\n"
                                "        GenerateStringKey: password\n"
                                "        PasswordLength: 32\n"
                                "        ExcludeCharacters: '\"@/\\\\'\n"
                                "  TestDB:\n"
                                "    Type: AWS::RDS::DBInstance\n"
                                "    Properties:\n"
                                "      DBInstanceIdentifier: mwaa-test-db\n"
                                "      DBInstanceClass: db.t3.micro\n"
                                "      Engine: mysql\n"
                                "      MasterUsername: admin\n"
                                "      MasterUserPassword: !Sub '{{resolve:secretsmanager:${MasterSecret}::password}}'\n"
                                "      AllocatedStorage: '20'\n"
                                # Encryption at rest is not optional in a sample, even for a
                                # throwaway demo instance.
                                "      StorageEncrypted: true\n"
                                "      PubliclyAccessible: false\n"
                                # 0 / false are deliberate for a demo that tears itself down.
                                # Both MUST be raised for anything real.
                                "      BackupRetentionPeriod: 0\n"
                                "      DeletionProtection: false\n"
                                "Outputs:\n"
                                "  DBInstanceId:\n"
                                "    Value: !Ref TestDB\n"
                                "  SecretArn:\n"
                                "    Value: !Ref MasterSecret\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_rds_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_rds_stack"],
                },
                {
                    "task_id": "create_snapshot",
                    "operator": "RdsCreateDbSnapshotOperator",
                    "parameters": {"db_type": "instance", "db_identifier": "mwaa-test-db", "db_snapshot_identifier": "snapshot-{{ ds_nodash }}"},
                    "upstream_tasks": ["wait_for_rds_stack"],
                },
                {
                    "task_id": "wait_for_snapshot",
                    "operator": "RdsSnapshotExistenceSensor",
                    "parameters": {"db_type": "instance", "db_snapshot_identifier": "snapshot-{{ ds_nodash }}"},
                    "upstream_tasks": ["create_snapshot"],
                },
                {
                    "task_id": "delete_snapshot",
                    "operator": "RdsDeleteDbSnapshotOperator",
                    "parameters": {"db_type": "instance", "db_snapshot_identifier": "snapshot-{{ ds_nodash }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_snapshot"],
                },
                {
                    "task_id": "delete_rds_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["delete_snapshot"],
                },
                {
                    "task_id": "wait_for_rds_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_rds_stack"],
                },
            ],
        },
        "ec2": {
            "default_params": {"stack_name": "mwaa-test-ec2-stack"},
            "tasks": [
                {
                    "task_id": "create_ec2_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  TestInstance:\n"
                                "    Type: AWS::EC2::Instance\n"
                                "    Properties:\n"
                                "      InstanceType: t3.micro\n"
                                "      ImageId: resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64\n"
                                "Outputs:\n"
                                "  InstanceId:\n"
                                "    Value: !Ref TestInstance\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_ec2_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_ec2_stack"],
                },
                {
                    "task_id": "stop_instance",
                    "operator": "EC2StopInstanceOperator",
                    "parameters": {"instance_id": "{{ ti.xcom_pull(task_ids='create_ec2_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}"},
                    "upstream_tasks": ["wait_for_ec2_stack"],
                },
                {
                    "task_id": "wait_for_stopped",
                    "operator": "EC2InstanceStateSensor",
                    "parameters": {"instance_id": "{{ ti.xcom_pull(task_ids='create_ec2_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}", "target_state": "stopped"},
                    "upstream_tasks": ["stop_instance"],
                },
                {
                    "task_id": "start_instance",
                    "operator": "EC2StartInstanceOperator",
                    "parameters": {"instance_id": "{{ ti.xcom_pull(task_ids='create_ec2_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}"},
                    "upstream_tasks": ["wait_for_stopped"],
                },
                {
                    "task_id": "wait_for_running",
                    "operator": "EC2InstanceStateSensor",
                    "parameters": {"instance_id": "{{ ti.xcom_pull(task_ids='create_ec2_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}", "target_state": "running"},
                    "upstream_tasks": ["start_instance"],
                },
                {
                    "task_id": "delete_ec2_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_running"],
                },
                {
                    "task_id": "wait_for_ec2_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_ec2_stack"],
                },
            ],
        },
        "sqs": {
            "default_params": {"queue_name": "mwaa-test-sqs-queue", "stack_name": "mwaa-test-sqs-stack"},
            "tasks": [
                {
                    "task_id": "create_sqs_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Parameters:\n"
                                "  QueueName:\n"
                                "    Type: String\n"
                                "    Default: mwaa-test-sqs-queue\n"
                                "Resources:\n"
                                "  TestQueue:\n"
                                "    Type: AWS::SQS::Queue\n"
                                "    Properties:\n"
                                "      QueueName: !Ref QueueName\n"
                                "Outputs:\n"
                                "  QueueUrl:\n"
                                "    Value: !Ref TestQueue\n"
                            ),
                            "Parameters": [{"ParameterKey": "QueueName", "ParameterValue": "{{ params.queue_name }}"}],
                        },
                    },
                },
                {
                    "task_id": "wait_for_sqs_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_sqs_stack"],
                },
                {
                    "task_id": "publish_message",
                    "operator": "SqsPublishOperator",
                    "parameters": {
                        "sqs_queue": "{{ ti.xcom_pull(task_ids='create_sqs_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                        "message_content": '{"event": "workflow_started"}',
                    },
                    "upstream_tasks": ["wait_for_sqs_stack"],
                },
                {
                    "task_id": "wait_for_message",
                    "operator": "SqsSensor",
                    "parameters": {
                        "sqs_queue": "{{ ti.xcom_pull(task_ids='create_sqs_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                    },
                    "upstream_tasks": ["publish_message"],
                },
                {
                    "task_id": "delete_sqs_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_message"],
                },
                {
                    "task_id": "wait_for_sqs_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_sqs_stack"],
                },
            ],
        },
        "eventbridge": {
            "default_params": {"stack_name": "mwaa-test-eb-stack"},
            "tasks": [
                {
                    "task_id": "create_eb_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  TestEventBus:\n"
                                "    Type: AWS::Events::EventBus\n"
                                "    Properties:\n"
                                "      Name: mwaa-test-event-bus\n"
                                "  TestRule:\n"
                                "    Type: AWS::Events::Rule\n"
                                "    Properties:\n"
                                "      Name: mwaa-test-eb-rule\n"
                                "      EventBusName: !Ref TestEventBus\n"
                                "      EventPattern:\n"
                                "        source:\n"
                                "          - mwaa.serverless\n"
                                "      State: ENABLED\n"
                                "Outputs:\n"
                                "  EventBusName:\n"
                                "    Value: !Ref TestEventBus\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_eb_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_eb_stack"],
                },
                {
                    "task_id": "put_events",
                    "operator": "EventBridgePutEventsOperator",
                    "parameters": {
                        "entries": [{"Source": "mwaa.serverless", "DetailType": "WorkflowEvent", "Detail": '{"status": "started"}', "EventBusName": "mwaa-test-event-bus"}],
                    },
                    "upstream_tasks": ["wait_for_eb_stack"],
                },
                {
                    "task_id": "disable_rule",
                    "operator": "EventBridgeDisableRuleOperator",
                    "parameters": {
                        "name": "mwaa-test-eb-rule",
                        "event_bus_name": "mwaa-test-event-bus",
                    },
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["put_events"],
                },
                {
                    "task_id": "delete_eb_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["disable_rule"],
                },
                {
                    "task_id": "wait_for_eb_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_eb_stack"],
                },
            ],
        },
        "comprehend": {
            "default_params": {"stack_name": "mwaa-test-comprehend-stack"},
            "tasks": [
                {
                    "task_id": "create_comprehend_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  DataBucket:\n"
                                "    Type: AWS::S3::Bucket\n"
                                "    Properties:\n"
                                "      BucketEncryption:\n"
                                "        ServerSideEncryptionConfiguration:\n"
                                "          - ServerSideEncryptionByDefault:\n"
                                "              SSEAlgorithm: AES256\n"
                                "      PublicAccessBlockConfiguration:\n"
                                "        BlockPublicAcls: true\n"
                                "        BlockPublicPolicy: true\n"
                                "        IgnorePublicAcls: true\n"
                                "        RestrictPublicBuckets: true\n"
                                "  ComprehendRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: comprehend.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      Policies:\n"
                                "        - PolicyName: S3Access\n"
                                "          PolicyDocument:\n"
                                "            Version: '2012-10-17'\n"
                                "            Statement:\n"
                                "              - Effect: Allow\n"
                                "                Action:\n"
                                "                  - s3:GetObject\n"
                                "                  - s3:PutObject\n"
                                "                  - s3:ListBucket\n"
                                "                Resource:\n"
                                "                  - !GetAtt DataBucket.Arn\n"
                                "                  - !Sub '${DataBucket.Arn}/*'\n"
                                "Outputs:\n"
                                "  InputS3Uri:\n"
                                "    Value: !Sub 's3://${DataBucket}/input/'\n"
                                "  OutputS3Uri:\n"
                                "    Value: !Sub 's3://${DataBucket}/output/'\n"
                                "  BucketName:\n"
                                "    Value: !Ref DataBucket\n"
                                "  RoleArn:\n"
                                "    Value: !GetAtt ComprehendRole.Arn\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_comprehend_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_comprehend_stack"],
                },
                {
                    "task_id": "upload_test_data",
                    "operator": "S3CreateObjectOperator",
                    "parameters": {
                        "s3_bucket": "{{ ti.xcom_pull(task_ids='create_comprehend_stack')['CreateStackResponse']['Outputs'][2]['OutputValue'] }}",
                        "s3_key": "input/test.txt",
                        "data": "Jane Doe lives at 100 Main St and her email is jane.doe@example.com",
                        "replace": True,
                    },
                    "upstream_tasks": ["wait_for_comprehend_stack"],
                },
                {
                    "task_id": "start_pii_detection",
                    "operator": "ComprehendStartPiiEntitiesDetectionJobOperator",
                    "parameters": {
                        "input_data_config": {
                            "S3Uri": "{{ ti.xcom_pull(task_ids='create_comprehend_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                            "InputFormat": "ONE_DOC_PER_LINE",
                        },
                        "output_data_config": {
                            "S3Uri": "{{ ti.xcom_pull(task_ids='create_comprehend_stack')['CreateStackResponse']['Outputs'][1]['OutputValue'] }}",
                        },
                        "mode": "ONLY_REDACTION",
                        "data_access_role_arn": "{{ ti.xcom_pull(task_ids='create_comprehend_stack')['CreateStackResponse']['Outputs'][3]['OutputValue'] }}",
                        "language_code": "en",
                    },
                    "upstream_tasks": ["upload_test_data"],
                },
                {
                    "task_id": "wait_for_pii_detection",
                    "operator": "ComprehendStartPiiEntitiesDetectionJobCompletedSensor",
                    "parameters": {"job_id": "{{ ti.xcom_pull(task_ids='start_pii_detection') }}"},
                    "upstream_tasks": ["start_pii_detection"],
                },
                {
                    "task_id": "delete_comprehend_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_pii_detection"],
                },
                {
                    "task_id": "wait_for_comprehend_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_comprehend_stack"],
                },
            ],
        },
        "dms": {
            "default_params": {"stack_name": "mwaa-test-dms-stack"},
            "tasks": [
                {
                    "task_id": "create_dms_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Description: DMS replication instance for testing\n"
                                "# NOTE: this stack deliberately does NOT create the\n"
                                "# 'dms-vpc-role' service role. That name is fixed and\n"
                                "# account-global: every DMS replication instance in the\n"
                                "# account depends on it. A demo that created it would fail\n"
                                "# with EntityAlreadyExists in an account that has it, and\n"
                                "# in an account that does not, the teardown would DELETE it\n"
                                "# and break every later DMS workload. Create it once,\n"
                                "# outside this demo:\n"
                                "#   aws iam create-role --role-name dms-vpc-role \\\n"
                                "#     --assume-role-policy-document \\\n"
                                "#     '{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\n"
                                "#       \"Principal\":{\"Service\":\"dms.amazonaws.com\"},\n"
                                "#       \"Action\":\"sts:AssumeRole\"}]}'\n"
                                "#   aws iam attach-role-policy --role-name dms-vpc-role \\\n"
                                "#     --policy-arn arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole\n"
                                "Resources:\n"
                                "  ReplicationInstance:\n"
                                "    Type: AWS::DMS::ReplicationInstance\n"
                                "    Properties:\n"
                                "      ReplicationInstanceClass: dms.t3.micro\n"
                                "      ReplicationInstanceIdentifier: mwaa-test-dms-ri\n"
                                "      PubliclyAccessible: false\n"
                                "Outputs:\n"
                                "  ReplicationInstanceArn:\n"
                                "    Value: !Ref ReplicationInstance\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_dms_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_dms_stack"],
                },
                {
                    "task_id": "delete_dms_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_dms_stack"],
                },
                {
                    "task_id": "wait_for_dms_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_dms_stack"],
                },
            ],
        },
        "kinesis_analytics": {
            "default_params": {"application_name": "mwaa-test-kinesis-app", "stack_name": "mwaa-test-kinesis-stack", "code_bucket": "REPLACE_ME_flink_code_bucket", "code_key": "flink-app.zip"},
            "tasks": [
                {
                    "task_id": "create_kinesis_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  KinesisRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: kinesisanalytics.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "Outputs:\n"
                                "  RoleArn:\n"
                                "    Value: !GetAtt KinesisRole.Arn\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_kinesis_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_kinesis_stack"],
                },
                {
                    "task_id": "create_application",
                    "operator": "KinesisAnalyticsV2CreateApplicationOperator",
                    "parameters": {
                        "application_name": "{{ params.application_name }}",
                        "runtime_environment": "FLINK-1_18",
                        "service_execution_role": "{{ ti.xcom_pull(task_ids='create_kinesis_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                        "create_application_kwargs": {
                            "ApplicationConfiguration": {
                                "FlinkApplicationConfiguration": {
                                    "ParallelismConfiguration": {
                                        "ConfigurationType": "CUSTOM",
                                        "Parallelism": 1,
                                        "ParallelismPerKPU": 1,
                                        "AutoScalingEnabled": False,
                                    }
                                },
                                "ApplicationCodeConfiguration": {
                                    "CodeContent": {
                                        "S3ContentLocation": {
                                            "BucketARN": "arn:aws:s3:::{{ params.code_bucket }}",
                                            "FileKey": "{{ params.code_key }}",
                                        }
                                    },
                                    "CodeContentType": "ZIPFILE",
                                },
                            }
                        },
                    },
                    "upstream_tasks": ["wait_for_kinesis_stack"],
                },
                {
                    "task_id": "start_application",
                    "operator": "KinesisAnalyticsV2StartApplicationOperator",
                    "parameters": {"application_name": "{{ params.application_name }}"},
                    "upstream_tasks": ["create_application"],
                },
                {
                    "task_id": "stop_application",
                    "operator": "KinesisAnalyticsV2StopApplicationOperator",
                    "parameters": {"application_name": "{{ params.application_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["start_application"],
                },
                {
                    "task_id": "delete_kinesis_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["stop_application"],
                },
                {
                    "task_id": "wait_for_kinesis_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_kinesis_stack"],
                },
            ],
        },
        "neptune": {
            "default_params": {"stack_name": "mwaa-test-neptune-stack"},
            "tasks": [
                {
                    "task_id": "create_neptune_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  NeptuneCluster:\n"
                                "    Type: AWS::Neptune::DBCluster\n"
                                "    Properties:\n"
                                "      DBClusterIdentifier: mwaa-test-neptune\n"
                                "      EngineVersion: 1.4.7.0\n"
                                "      DeletionProtection: false\n"
                                "  NeptuneInstance:\n"
                                "    Type: AWS::Neptune::DBInstance\n"
                                "    Properties:\n"
                                "      DBClusterIdentifier: !Ref NeptuneCluster\n"
                                "      DBInstanceClass: db.t3.medium\n"
                                "Outputs:\n"
                                "  ClusterId:\n"
                                "    Value: !Ref NeptuneCluster\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_neptune_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_neptune_stack"],
                },
                {
                    "task_id": "stop_cluster",
                    "operator": "NeptuneStopDbClusterOperator",
                    "parameters": {"db_cluster_id": "mwaa-test-neptune"},
                    "upstream_tasks": ["wait_for_neptune_stack"],
                },
                {
                    "task_id": "start_cluster",
                    "operator": "NeptuneStartDbClusterOperator",
                    "parameters": {"db_cluster_id": "mwaa-test-neptune"},
                    "upstream_tasks": ["stop_cluster"],
                },
                {
                    "task_id": "delete_neptune_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["start_cluster"],
                },
                {
                    "task_id": "wait_for_neptune_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_neptune_stack"],
                },
            ],
        },
        "glacier": {
            "default_params": {"stack_name": "mwaa-test-glacier-stack"},
            "tasks": [
                {
                    "task_id": "create_glacier_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  TestVault:\n"
                                "    Type: AWS::Glacier::Vault\n"
                                "    Properties:\n"
                                "      VaultName: mwaa-test-vault\n"
                                "Outputs:\n"
                                "  VaultName:\n"
                                "    Value: !Ref TestVault\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_glacier_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_glacier_stack"],
                },
                {
                    "task_id": "create_inventory_job",
                    "operator": "GlacierCreateJobOperator",
                    "parameters": {"vault_name": "mwaa-test-vault"},
                    "upstream_tasks": ["wait_for_glacier_stack"],
                },
                {
                    "task_id": "wait_for_job",
                    "operator": "GlacierJobOperationSensor",
                    "parameters": {"vault_name": "mwaa-test-vault", "job_id": "{{ ti.xcom_pull(task_ids='create_inventory_job') }}"},
                    "upstream_tasks": ["create_inventory_job"],
                },
                {
                    "task_id": "delete_glacier_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_job"],
                },
                {
                    "task_id": "wait_for_glacier_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_glacier_stack"],
                },
            ],
        },
        "datasync": {
            "default_params": {"stack_name": "mwaa-test-datasync-stack"},
            "tasks": [
                {
                    "task_id": "create_datasync_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "Capabilities": ["CAPABILITY_IAM"],
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  SourceBucket:\n"
                                "    Type: AWS::S3::Bucket\n"
                                "    Properties:\n"
                                "      BucketEncryption:\n"
                                "        ServerSideEncryptionConfiguration:\n"
                                "          - ServerSideEncryptionByDefault:\n"
                                "              SSEAlgorithm: AES256\n"
                                "      PublicAccessBlockConfiguration:\n"
                                "        BlockPublicAcls: true\n"
                                "        BlockPublicPolicy: true\n"
                                "        IgnorePublicAcls: true\n"
                                "        RestrictPublicBuckets: true\n"
                                "  DestBucket:\n"
                                "    Type: AWS::S3::Bucket\n"
                                "    Properties:\n"
                                "      BucketEncryption:\n"
                                "        ServerSideEncryptionConfiguration:\n"
                                "          - ServerSideEncryptionByDefault:\n"
                                "              SSEAlgorithm: AES256\n"
                                "      PublicAccessBlockConfiguration:\n"
                                "        BlockPublicAcls: true\n"
                                "        BlockPublicPolicy: true\n"
                                "        IgnorePublicAcls: true\n"
                                "        RestrictPublicBuckets: true\n"
                                "  DataSyncRole:\n"
                                "    Type: AWS::IAM::Role\n"
                                "    Properties:\n"
                                "      AssumeRolePolicyDocument:\n"
                                "        Version: '2012-10-17'\n"
                                "        Statement:\n"
                                "          - Effect: Allow\n"
                                "            Principal:\n"
                                "              Service: datasync.amazonaws.com\n"
                                "            Action: sts:AssumeRole\n"
                                "      Policies:\n"
                                "        - PolicyName: S3Access\n"
                                "          PolicyDocument:\n"
                                "            Version: '2012-10-17'\n"
                                "            Statement:\n"
                                "              - Effect: Allow\n"
                                "                Action:\n"
                                "                  - s3:GetObject\n"
                                "                  - s3:PutObject\n"
                                "                  - s3:DeleteObject\n"
                                "                  - s3:GetObjectTagging\n"
                                "                  - s3:PutObjectTagging\n"
                                "                Resource:\n"
                                "                  - !Sub '${SourceBucket.Arn}/*'\n"
                                "                  - !Sub '${DestBucket.Arn}/*'\n"
                                "              - Effect: Allow\n"
                                "                Action:\n"
                                "                  - s3:ListBucket\n"
                                "                  - s3:GetBucketLocation\n"
                                "                  - s3:ListBucketMultipartUploads\n"
                                "                Resource:\n"
                                "                  - !GetAtt SourceBucket.Arn\n"
                                "                  - !GetAtt DestBucket.Arn\n"
                                "  SourceLocation:\n"
                                "    Type: AWS::DataSync::LocationS3\n"
                                "    Properties:\n"
                                "      S3BucketArn: !GetAtt SourceBucket.Arn\n"
                                "      S3Config:\n"
                                "        BucketAccessRoleArn: !GetAtt DataSyncRole.Arn\n"
                                "  DestLocation:\n"
                                "    Type: AWS::DataSync::LocationS3\n"
                                "    Properties:\n"
                                "      S3BucketArn: !GetAtt DestBucket.Arn\n"
                                "      S3Config:\n"
                                "        BucketAccessRoleArn: !GetAtt DataSyncRole.Arn\n"
                                "  DataSyncTask:\n"
                                "    Type: AWS::DataSync::Task\n"
                                "    Properties:\n"
                                "      SourceLocationArn: !Ref SourceLocation\n"
                                "      DestinationLocationArn: !Ref DestLocation\n"
                                "Outputs:\n"
                                "  TaskArn:\n"
                                "    Value: !GetAtt DataSyncTask.TaskArn\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_datasync_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_datasync_stack"],
                },
                {
                    "task_id": "run_datasync",
                    "operator": "DataSyncOperator",
                    "parameters": {
                        "task_arn": "{{ ti.xcom_pull(task_ids='create_datasync_stack')['CreateStackResponse']['Outputs'][0]['OutputValue'] }}",
                        "wait_for_completion": True,
                    },
                    "upstream_tasks": ["wait_for_datasync_stack"],
                },
                {
                    "task_id": "delete_datasync_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["run_datasync"],
                },
                {
                    "task_id": "wait_for_datasync_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_datasync_stack"],
                },
            ],
        },
        "appflow": {
            "default_params": {"stack_name": "mwaa-test-appflow-stack"},
            "tasks": [
                {
                    "task_id": "create_appflow_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  SourceBucket:\n"
                                "    Type: AWS::S3::Bucket\n"
                                "    Properties:\n"
                                "      BucketEncryption:\n"
                                "        ServerSideEncryptionConfiguration:\n"
                                "          - ServerSideEncryptionByDefault:\n"
                                "              SSEAlgorithm: AES256\n"
                                "      PublicAccessBlockConfiguration:\n"
                                "        BlockPublicAcls: true\n"
                                "        BlockPublicPolicy: true\n"
                                "        IgnorePublicAcls: true\n"
                                "        RestrictPublicBuckets: true\n"
                                "  DestBucket:\n"
                                "    Type: AWS::S3::Bucket\n"
                                "    Properties:\n"
                                "      BucketEncryption:\n"
                                "        ServerSideEncryptionConfiguration:\n"
                                "          - ServerSideEncryptionByDefault:\n"
                                "              SSEAlgorithm: AES256\n"
                                "      PublicAccessBlockConfiguration:\n"
                                "        BlockPublicAcls: true\n"
                                "        BlockPublicPolicy: true\n"
                                "        IgnorePublicAcls: true\n"
                                "        RestrictPublicBuckets: true\n"
                                "  TestFlow:\n"
                                "    Type: AWS::AppFlow::Flow\n"
                                "    Properties:\n"
                                "      FlowName: mwaa-test-flow\n"
                                "      TriggerConfig:\n"
                                "        TriggerType: OnDemand\n"
                                "      SourceFlowConfig:\n"
                                "        ConnectorType: S3\n"
                                "        SourceConnectorProperties:\n"
                                "          S3:\n"
                                "            BucketName: !Ref SourceBucket\n"
                                "            BucketPrefix: input\n"
                                "      DestinationFlowConfigList:\n"
                                "        - ConnectorType: S3\n"
                                "          DestinationConnectorProperties:\n"
                                "            S3:\n"
                                "              BucketName: !Ref DestBucket\n"
                                "              BucketPrefix: output\n"
                                "      Tasks:\n"
                                "        - TaskType: Map_all\n"
                                "          SourceFields: []\n"
                                "          TaskProperties:\n"
                                "            - Key: EXCLUDE_SOURCE_FIELDS_LIST\n"
                                "              Value: '[]'\n"
                                "Outputs:\n"
                                "  FlowName:\n"
                                "    Value: mwaa-test-flow\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_appflow_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_appflow_stack"],
                },
                {
                    "task_id": "run_flow",
                    "operator": "AppflowRunOperator",
                    "parameters": {"flow_name": "mwaa-test-flow", "wait_for_completion": True},
                    "upstream_tasks": ["wait_for_appflow_stack"],
                },
                {
                    "task_id": "delete_appflow_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["run_flow"],
                },
                {
                    "task_id": "wait_for_appflow_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_appflow_stack"],
                },
            ],
        },
        "quicksight": {
            "default_params": {"stack_name": "mwaa-test-quicksight-stack"},
            "tasks": [
                {
                    "task_id": "create_quicksight_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  DataSource:\n"
                                "    Type: AWS::QuickSight::DataSource\n"
                                "    Properties:\n"
                                "      AwsAccountId: !Ref AWS::AccountId\n"
                                "      DataSourceId: mwaa-test-datasource\n"
                                "      Name: mwaa-test-datasource\n"
                                "      Type: S3\n"
                                "  DataSet:\n"
                                "    Type: AWS::QuickSight::DataSet\n"
                                "    Properties:\n"
                                "      AwsAccountId: !Ref AWS::AccountId\n"
                                "      DataSetId: mwaa-test-dataset\n"
                                "      Name: mwaa-test-dataset\n"
                                "      ImportMode: SPICE\n"
                                "      PhysicalTableMap:\n"
                                "        PhysicalTable1:\n"
                                "          CustomSql:\n"
                                "            DataSourceArn: !GetAtt DataSource.Arn\n"
                                "            Name: test\n"
                                "            SqlQuery: SELECT 1 as id\n"
                                "            Columns:\n"
                                "              - Name: id\n"
                                "                Type: INTEGER\n"
                                "Outputs:\n"
                                "  DataSetId:\n"
                                "    Value: mwaa-test-dataset\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_quicksight_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_quicksight_stack"],
                },
                {
                    "task_id": "create_ingestion",
                    "operator": "QuickSightCreateIngestionOperator",
                    "parameters": {
                        "data_set_id": "mwaa-test-dataset",
                        "ingestion_id": "ingestion-{{ ds_nodash }}",
                    },
                    "upstream_tasks": ["wait_for_quicksight_stack"],
                },
                {
                    "task_id": "wait_for_ingestion",
                    "operator": "QuickSightSensor",
                    "parameters": {
                        "data_set_id": "mwaa-test-dataset",
                        "ingestion_id": "ingestion-{{ ds_nodash }}",
                    },
                    "upstream_tasks": ["create_ingestion"],
                },
                {
                    "task_id": "delete_quicksight_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_ingestion"],
                },
                {
                    "task_id": "wait_for_quicksight_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_quicksight_stack"],
                },
            ],
        },
        "dynamodb": {
            "default_params": {"stack_name": "mwaa-test-dynamodb-stack"},
            "tasks": [
                {
                    "task_id": "create_dynamodb_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  TestTable:\n"
                                "    Type: AWS::DynamoDB::Table\n"
                                "    Properties:\n"
                                "      TableName: mwaa-test-table\n"
                                "      BillingMode: PAY_PER_REQUEST\n"
                                "      AttributeDefinitions:\n"
                                "        - AttributeName: pk\n"
                                "          AttributeType: S\n"
                                "      KeySchema:\n"
                                "        - AttributeName: pk\n"
                                "          KeyType: HASH\n"
                                "Outputs:\n"
                                "  TableName:\n"
                                "    Value: !Ref TestTable\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_dynamodb_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_dynamodb_stack"],
                },
                {
                    "task_id": "wait_for_value",
                    "operator": "DynamoDBValueSensor",
                    "parameters": {
                        "table_name": "mwaa-test-table",
                        "partition_key_name": "pk",
                        "partition_key_value": "test-key",
                        "attribute_name": "status",
                        "attribute_value": ["complete"],
                    },
                    "upstream_tasks": ["wait_for_dynamodb_stack"],
                },
                {
                    "task_id": "delete_dynamodb_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_value"],
                },
                {
                    "task_id": "wait_for_dynamodb_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_dynamodb_stack"],
                },
            ],
        },
        "opensearch_serverless": {
            "default_params": {"collection_name": "mwaa-test-collection", "stack_name": "mwaa-test-aoss-stack"},
            "tasks": [
                {
                    "task_id": "create_aoss_stack",
                    "operator": "CloudFormationCreateStackOperator",
                    "parameters": {
                        "stack_name": "{{ params.stack_name }}",
                        "cloudformation_parameters": {
                            "StackName": "{{ params.stack_name }}",
                            "TemplateBody": (
                                "AWSTemplateFormatVersion: '2010-09-09'\n"
                                "Resources:\n"
                                "  SecurityPolicy:\n"
                                "    Type: AWS::OpenSearchServerless::SecurityPolicy\n"
                                "    Properties:\n"
                                "      Name: mwaa-test-enc-policy\n"
                                "      Type: encryption\n"
                                "      Policy: '{\"Rules\":[{\"ResourceType\":\"collection\",\"Resource\":[\"collection/mwaa-test-collection\"]}],\"AWSOwnedKey\":true}'\n"
                                "  NetworkPolicy:\n"
                                "    Type: AWS::OpenSearchServerless::SecurityPolicy\n"
                                "    Properties:\n"
                                "      Name: mwaa-test-net-policy\n"
                                "      Type: network\n"
                                "      Policy: '[{\"Rules\":[{\"ResourceType\":\"collection\",\"Resource\":[\"collection/mwaa-test-collection\"]}],\"AllowFromPublic\":false}]'\n"
                                "  Collection:\n"
                                "    Type: AWS::OpenSearchServerless::Collection\n"
                                "    DependsOn:\n"
                                "      - SecurityPolicy\n"
                                "      - NetworkPolicy\n"
                                "    Properties:\n"
                                "      Name: mwaa-test-collection\n"
                                "      Type: SEARCH\n"
                                "Outputs:\n"
                                "  CollectionName:\n"
                                "    Value: mwaa-test-collection\n"
                            ),
                        },
                    },
                },
                {
                    "task_id": "wait_for_aoss_stack",
                    "operator": "CloudFormationCreateStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["create_aoss_stack"],
                },
                {
                    "task_id": "wait_for_collection",
                    "operator": "OpenSearchServerlessCollectionActiveSensor",
                    "parameters": {"collection_name": "{{ params.collection_name }}"},
                    "upstream_tasks": ["wait_for_aoss_stack"],
                },
                {
                    "task_id": "delete_aoss_stack",
                    "operator": "CloudFormationDeleteStackOperator",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "trigger_rule": "all_done",
                    "upstream_tasks": ["wait_for_collection"],
                },
                {
                    "task_id": "wait_for_aoss_delete",
                    "operator": "CloudFormationDeleteStackSensor",
                    "parameters": {"stack_name": "{{ params.stack_name }}"},
                    "upstream_tasks": ["delete_aoss_stack"],
                },
            ],
        },
    }
