# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""build_dag_yaml's correct-by-construction guarantee, and the cross-module
invariants that used to drift between schema.py and constraints.py."""

import pytest
import yaml

import builder
import constraints
import schema
from constraints import QUOTAS

EMPTY = {"task_id": "a", "operator": "EmptyOperator"}


def build(tasks, **kwargs):
    return builder.build_dag_yaml("wf", tasks, **kwargs)


def emitted(result):
    return next(iter(yaml.safe_load(result["dag_yaml"]).values()))


# ── The guarantee the module docstring makes ─────────────────────────────────

def test_short_operator_names_become_fully_qualified():
    body = emitted(build([EMPTY]))
    assert body["tasks"]["a"]["operator"] == schema.SUPPORTED_OPERATORS["EmptyOperator"]


def test_tasks_are_emitted_as_a_mapping_not_a_list():
    body = emitted(build([EMPTY]))
    assert isinstance(body["tasks"], dict)


@pytest.mark.parametrize("alias", list(builder._DEP_ALIASES))
def test_dependency_aliases_never_reach_the_output(alias):
    """`upstream_tasks` was not in the reserved set, so the spec-passthrough loop
    copied it verbatim — breaking the module's central claim."""
    result = build([EMPTY, {"task_id": "b", "operator": "EmptyOperator", alias: ["a"]}])
    assert alias not in result["dag_yaml"]
    assert emitted(result)["tasks"]["b"]["dependencies"] == ["a"]
    assert any(alias in adj for adj in result["values_adjusted"])


@pytest.mark.parametrize("alias", list(builder._REVERSE_DEP_ALIASES))
def test_reverse_dependency_aliases_are_rejected_with_an_explanation(alias):
    result = build([EMPTY, {"task_id": "b", "operator": "EmptyOperator", alias: ["a"]}])
    assert result["valid"] is False
    assert any("dependencies: [b]" in p for p in result["build_problems"])


def test_parameters_wrapper_is_flattened():
    result = build([{"task_id": "g", "operator": "GlueJobOperator",
                     "parameters": {"job_name": "j"}}])
    assert emitted(result)["tasks"]["g"]["job_name"] == "j"
    assert "parameters" not in emitted(result)["tasks"]["g"]


def test_execution_timeout_is_a_timedelta_mapping():
    result = build([{**EMPTY, "execution_timeout_minutes": 10}])
    assert emitted(result)["tasks"]["a"]["execution_timeout"] == {
        "__type__": "datetime.timedelta", "minutes": 10
    }


# ── Nothing changes silently ─────────────────────────────────────────────────

def test_duration_string_is_parsed_and_reported():
    result = build([{**EMPTY, "retry_delay_seconds": "60s"}])
    assert emitted(result)["tasks"]["a"]["retry_delay"] == 60
    assert any("60" in adj for adj in result["values_adjusted"])


def test_capping_is_reported():
    result = build([{**EMPTY, "retry_delay_seconds": 9999,
                     "execution_timeout_minutes": 600}])
    joined = " ".join(result["values_adjusted"])
    assert "capped" in joined
    assert emitted(result)["tasks"]["a"]["retry_delay"] == QUOTAS["max_retry_delay_seconds"]


def test_negative_duration_is_rejected_not_capped():
    result = build([{**EMPTY, "retry_delay_seconds": -5}])
    assert result["valid"] is False
    assert any("negative" in p for p in result["build_problems"])


def test_unparseable_sensor_timeout_does_not_raise():
    """`sensor_timeout_seconds: '1h'` used to raise an uncaught ValueError."""
    ok = build([{"task_id": "w", "operator": "S3KeySensor",
                 "params": {"bucket_key": "s3://b/k"}, "sensor_timeout_seconds": "1h"}])
    assert emitted(ok)["tasks"]["w"]["timeout"] == 3600

    bad = build([{"task_id": "w", "operator": "S3KeySensor",
                  "params": {"bucket_key": "s3://b/k"},
                  "sensor_timeout_seconds": "nope"}])
    assert bad["valid"] is False
    assert any("sensor_timeout_seconds" in p for p in bad["build_problems"])


def test_default_args_duration_is_parsed_not_replaced():
    """Any string retry_delay used to become 300 with nothing in the report."""
    result = build([EMPTY], default_args={"owner": "me", "retry_delay": "60s"})
    assert emitted(result)["default_args"]["retry_delay"] == 60
    assert result["values_adjusted"]


def test_non_list_dependencies_are_rejected():
    """A dict used to be silently coerced to its keys."""
    result = build([EMPTY, {"task_id": "b", "operator": "EmptyOperator",
                            "dependencies": {"a": 1, "zzz": 2}}])
    assert result["valid"] is False
    assert any("dependencies must be a list" in p for p in result["build_problems"])


def test_sensor_cost_defaults_are_applied_and_explained():
    result = build([{"task_id": "w", "operator": "S3KeySensor",
                     "params": {"bucket_key": "s3://b/k"}}])
    task = emitted(result)["tasks"]["w"]
    assert task["mode"] == "reschedule"
    assert task["timeout"] > 0
    assert result["cost_optimizations_applied"]


def test_explicit_poke_mode_wins():
    result = build([{"task_id": "w", "operator": "S3KeySensor",
                     "params": {"bucket_key": "s3://b/k", "mode": "poke"}}])
    assert emitted(result)["tasks"]["w"]["mode"] == "poke"


def test_nested_placeholders_are_detected():
    """Scanning only top-level strings missed placeholders where they actually live."""
    result = build([{"task_id": "g", "operator": "GlueJobOperator",
                     "params": {"job_name": "real",
                                "script_args": {"--input": "s3://REPLACE_ME/in/"}}}])
    assert any("script_args" in w for w in result["placeholders_to_replace"])


# ── STEP_CATALOG must not be handed out by reference ─────────────────────────

def test_plan_pipeline_does_not_alias_the_catalog():
    """In a long-lived stdio server, one caller mutating the response used to poison
    the catalog for the whole process."""
    planned = builder.plan_pipeline(["glue_job"])["planned_tasks"][0]
    assert planned["required_arguments"] is not builder.STEP_CATALOG["glue_job"]["required"]
    planned["required_arguments"]["INJECTED"] = "x"
    assert "INJECTED" not in builder.STEP_CATALOG["glue_job"]["required"]


def test_list_pipeline_steps_does_not_alias_the_catalog():
    step = builder.list_pipeline_steps("glue_job")["steps"]["glue_job"]
    step["required"]["INJECTED"] = "x"
    assert "INJECTED" not in builder.STEP_CATALOG["glue_job"]["required"]


def test_plan_pipeline_reports_what_it_deliberately_left_out():
    plan = builder.plan_pipeline(["glue_job", "athena_query"])
    assert plan["questions_to_ask_the_user"]
    assert plan["deliberately_not_added"]


# ── Cross-module invariants ──────────────────────────────────────────────────

def test_abstract_operators_are_all_in_the_allowlist():
    """An abstract name outside the allowlist can never be matched, so it is dead
    configuration. 'BatchOperatorBase' sat there unnoticed."""
    assert not (schema.ABSTRACT_OPERATORS - set(schema.SUPPORTED_OPERATORS))


@pytest.mark.parametrize("short", ["EmptyOperator", "PythonOperator", "BashOperator"])
def test_standard_operators_use_one_convention(short):
    assert schema.SUPPORTED_OPERATORS[short].startswith(
        "airflow.providers.standard.operators."
    )
    assert short in schema.ALT_OPERATOR_FQNS.values(), \
        "the legacy path must stay recognised on input"


def test_legacy_operator_paths_still_resolve():
    fqn, short, _ = schema.resolve_operator_fqn("airflow.operators.empty.EmptyOperator")
    assert short == "EmptyOperator"


def test_max_active_runs_is_declared_ignored_in_exactly_one_place():
    """Listing it in both sets made the validator warn that the field has no effect
    AND error that its value was too large, for the same input."""
    assert "max_active_runs" in constraints.SILENTLY_IGNORED_DAG_KEYS
    assert "max_active_runs" not in constraints.ACCEPTED_DAG_KEYS


def test_default_args_task_overlap_is_explicit():
    """Six keys are accepted under default_args and ignored on a task. That is a
    real distinction, so it is named rather than left to be inferred."""
    expected = {
        "email", "end_date", "owner", "priority_weight", "start_date",
        "wait_for_downstream",
    }
    assert constraints.DEFAULT_ARGS_ALLOWLIST_ONLY == (
        constraints.DEFAULT_ARGS_ALLOWLIST & constraints.IGNORED_TASK_PARAMS
    )
    assert sorted(constraints.DEFAULT_ARGS_ALLOWLIST_ONLY) == sorted(expected)


def test_reschedule_guidance_does_not_contradict_itself():
    """The dormant 'unsupported' string used to assert the opposite of the verified
    cost_efficiency policy, so flipping the flag started telling users something
    untrue."""
    assert schema.RESCHEDULE_MODE_SUPPORTED is True
    assert "not currently supported end to end" not in schema.RESCHEDULE_MODE_UNSUPPORTED
    assert "RESCHEDULE_MODE_SUPPORTED is False" in schema.RESCHEDULE_MODE_UNSUPPORTED


def test_every_operator_with_required_params_is_in_the_allowlist():
    unknown = set(schema.OPERATOR_REQUIRED_PARAMS) - set(schema.SUPPORTED_OPERATORS)
    assert not unknown


def test_every_xcom_documented_operator_is_in_the_allowlist():
    unknown = set(schema.OPERATOR_XCOM_RETURNS) - set(schema.SUPPORTED_OPERATORS)
    assert not unknown


def test_long_wait_operator_pairs_reference_real_operators():
    for operator, sensor in schema.LONG_WAIT_OPERATOR_PAIRS.items():
        assert operator in schema.SUPPORTED_OPERATORS
        assert sensor in schema.SUPPORTED_OPERATORS
