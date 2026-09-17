# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Validator behaviour, including the untrusted-input bounds.

The DoS tests here are the important ones: they assert on a BOUND, not on an exact
duration, so they stay meaningful on a slow machine.
"""

import time

import pytest

import validator
from constraints import QUOTAS

EMPTY_OP = "airflow.providers.standard.operators.empty.EmptyOperator"


def _chain(n):
    y = f"d:\n  tasks:\n    t0:\n      operator: {EMPTY_OP}\n"
    y += "".join(
        f"    t{i}:\n      operator: {EMPTY_OP}\n      dependencies: [t{i-1}]\n"
        for i in range(1, n)
    )
    return y


def _alias_bomb(depth, width=10):
    """Tiny input that expands to width**depth logical nodes via YAML aliases."""
    s = "a0: &a0 [" + ",".join(["xxxx"] * width) + "]\n"
    s += "".join(
        f"a{i}: &a{i} [" + ",".join([f"*a{i-1}"] * width) + "]\n" for i in range(1, depth)
    )
    s += (f"d:\n  tasks:\n    t:\n      operator: {EMPTY_OP}\n"
          f"      bash_command: 'x'\n      x: *a{depth-1}\n")
    return s


# ── Untrusted input bounds ───────────────────────────────────────────────────

def test_oversize_definition_is_not_parsed():
    """The size guard must short-circuit, not append an error and carry on."""
    big = "d:\n  tasks:\n" + "".join(
        f"    t{i}:\n      operator: {EMPTY_OP}\n" for i in range(20000)
    )
    result = validator.validate(big)
    assert result["valid"] is False
    assert any("was not analysed" in e for e in result["errors"])
    # Proof it did not walk the tasks: no task_count was ever computed.
    assert not result["summary"]


def test_yaml_alias_expansion_is_bounded():
    """447 bytes of nested anchors must not cost seconds of CPU.

    safe_load shares aliased objects, so parsing stays cheap while the logical tree
    explodes. Walking it structurally once per task used to grow 10x per 50 input
    bytes, which on Lambda burns the whole billed timeout.
    """
    started = time.monotonic()
    result = validator.validate(_alias_bomb(9))
    elapsed = time.monotonic() - started
    assert elapsed < 5, f"alias expansion took {elapsed:.1f}s — the node budget is not holding"
    assert any("expands to more than" in e for e in result["errors"]), \
        "a truncated analysis must be reported as an error, never as a clean pass"
    assert result["valid"] is False


def test_long_dependency_chain_does_not_raise():
    """The graph walks must be iterative — a RecursionError escapes validate()."""
    # A definition long enough to blow the recursion limit is also over the size
    # limit, so drive the graph walks directly to prove they are iterative rather
    # than relying on the size guard to hide the problem.
    deep = {f"t{i}": [f"t{i-1}"] for i in range(1, 20000)}
    deep["t0"] = []
    closures = validator._upstream_closures(deep)
    assert len(closures["t19999"]) == 19999

    errors = []
    validator._validate_cycles("d", deep, errors)
    assert errors == []

    # And the largest chain that actually fits inside the size limit must validate.
    biggest = max(
        n for n in range(50, 460, 10)
        if len(_chain(n).encode()) < QUOTAS["max_dag_definition_kb"] * 1024
    )
    assert validator.validate(_chain(biggest))["valid"] is True


def test_cycle_detection_is_iterative_on_a_large_graph():
    deep = {f"t{i}": [f"t{i-1}"] for i in range(1, 20000)}
    deep["t0"] = ["t19999"]  # close the loop
    errors = []
    validator._validate_cycles("d", deep, errors)
    assert any("cycle" in e for e in errors)


def test_deeply_nested_value_is_bounded():
    nested = f"d:\n  tasks:\n    t:\n      operator: {EMPTY_OP}\n      x: " + \
             "[" * 150 + "1" + "]" * 150 + "\n"
    result = validator.validate(nested)
    assert isinstance(result["valid"], bool)  # must not raise


# ── Cycle detection still works after the iterative rewrite ──────────────────

def test_cycle_is_detected():
    y = f"""d:
  tasks:
    a:
      operator: {EMPTY_OP}
      dependencies: [c]
    b:
      operator: {EMPTY_OP}
      dependencies: [a]
    c:
      operator: {EMPTY_OP}
      dependencies: [b]
"""
    errors = validator.validate(y)["errors"]
    assert any("cycle" in e for e in errors)


def test_self_dependency_is_detected():
    y = f"d:\n  tasks:\n    a:\n      operator: {EMPTY_OP}\n      dependencies: [a]\n"
    assert validator.validate(y)["valid"] is False


# ── xcom reachability uses the memoised closure ──────────────────────────────

def test_transitively_upstream_xcom_is_allowed():
    y = """d:
  tasks:
    a:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: echo a
    b:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: echo b
      dependencies: [a]
    c:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: "echo {{ ti.xcom_pull(task_ids='a') }}"
      dependencies: [b]
"""
    assert validator.validate(y)["valid"] is True


def test_non_upstream_xcom_is_an_error():
    y = """d:
  tasks:
    a:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: echo a
    b:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: "echo {{ ti.xcom_pull(task_ids='a') }}"
"""
    errors = validator.validate(y)["errors"]
    assert any("not upstream" in e for e in errors)


# ── Jinja scanning ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("expression,expect_error", [
    # Mid-expression variable — a first-identifier-only regex missed this.
    ("{{ params.x or execution_date }}", True),
    # Statement block — anchoring on {{ skipped these entirely.
    ("{% if dag_run %}y{% endif %}", True),
    ("{{ dag_run.conf }}", True),
    # Legitimate uses that must NOT be flagged.
    ("{{ ds }}", False),
    ("{{ params.q | upper }}", False),
    ("{% for f in params.files %}{{ f }}{% endfor %}", False),
    ("{{ ti.xcom_pull(task_ids='x') }}", False),
    ("literal execution_date in prose, not jinja", False),
])
def test_jinja_variable_detection(expression, expect_error):
    y = (f"d:\n  tasks:\n    x:\n      operator: airflow.providers.standard.operators.bash.BashOperator\n"
         f"      bash_command: echo hi\n"
         f"    t:\n      operator: airflow.providers.standard.operators.bash.BashOperator\n"
         f"      bash_command: {expression!r}\n      dependencies: [x]\n")
    errors = validator.validate(y)["errors"]
    unsupported = [e for e in errors if "not available in MWAA Serverless" in e]
    assert bool(unsupported) is expect_error, errors


# ── DAG-level rules that constraints.py documents ───────────────────────────

@pytest.mark.parametrize("dag_lines,valid", [
    ("  start_date: '2024-01-01'\n", True),
    ("  start_date: not-a-date\n", False),
    ("  start_date: '2024-06-01'\n  end_date: '2024-01-01'\n", False),
    ("  start_date: '2024-01-01'\n  end_date: '2024-06-01'\n", True),
    ("  schedule: '@daily'\n", True),
    ("  schedule: '@nope'\n", False),
    ("  schedule: '0 3 * * *'\n", True),
    ("  schedule: '*/15 * * * 1-5'\n", True),
    ("  schedule: every tuesday-ish\n", False),
    ("  schedule: null\n", True),
])
def test_dag_dates_and_schedule(tasks_yaml, dag_lines, valid):
    assert validator.validate(tasks_yaml(dag_lines))["valid"] is valid


def test_empty_tasks_mapping_is_an_error():
    """An empty mapping used to pass, so a converter that produced no tasks
    reported a deployable workflow that does nothing."""
    result = validator.validate("d:\n  tasks: {}\n")
    assert result["valid"] is False
    assert any("empty" in e for e in result["errors"])


def test_unknown_dag_key_is_an_error(tasks_yaml):
    """The service rejects unknown DAG attributes with 'Unexpected element', so a
    warning meant valid=True for something CreateWorkflow refuses."""
    result = validator.validate(tasks_yaml("  totally_made_up: 1\n"))
    assert result["valid"] is False


def test_max_active_runs_warns_but_does_not_error(tasks_yaml):
    """It is reported by the service as an ignored attribute. Warning AND error for
    the same field was self-contradictory."""
    result = validator.validate(tasks_yaml("  max_active_runs: 99\n"))
    assert result["valid"] is True
    assert any("max_active_runs" in w for w in result["warnings"])


def test_task_id_regex_rejects_trailing_newline():
    """$ matches before a trailing newline in Python; \\Z does not."""
    assert validator._TASK_ID_RE.match("ok-id.1") is not None
    assert validator._TASK_ID_RE.match("a\n") is None


# ── Repair ──────────────────────────────────────────────────────────────────

def test_repair_reports_every_change():
    y = """d:
  tasks:
    - task_id: a
      operator: EmptyOperator
    - task_id: b
      operator: S3KeySensor
      parameters:
        bucket_key: s3://b/k
      upstream_tasks: [a]
      deferrable: true
      retry_delay: 90s
"""
    out = validator.repair(y)
    assert out["repaired_yaml"]
    assert out["changes"], "a repair with no reported changes is a silent rewrite"
    joined = " ".join(out["changes"])
    assert "dependencies" in joined
    assert "deferrable" in joined or "reschedule" in joined



@pytest.mark.parametrize("expression,expected", [
    # Keyword-argument names are NOT variable references. Found by a live run: the
    # broader Jinja scanner flagged `task_ids` as an unrecognised variable, which is a
    # false warning on the exact expression the schema docs tell people to write for
    # passing values between tasks.
    ("{{ ti.xcom_pull(task_ids='up') }}", ["ti.xcom_pull"]),
    ("{{ ti.xcom_pull(task_ids='up')['key_count'] }}", ["ti.xcom_pull"]),
    ("{{ ti.xcom_pull(task_ids='a', key='b') }}", ["ti.xcom_pull"]),
    # '==' is a comparison, not a keyword argument, so both operands are variables.
    ("{% if a == b %}x{% endif %}", ["a", "b"]),
    ("{{ params.x or execution_date }}", ["params.x", "execution_date"]),
    ("{% if dag_run %}x{% endif %}", ["dag_run"]),
])
def test_jinja_identifier_extraction(expression, expected):
    assert validator._jinja_identifiers(expression) == expected


def test_documented_xcom_idiom_produces_no_warning():
    """PARAMETER_PASSING tells users to write exactly this. It must be clean."""
    y = """d:
  tasks:
    up:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: echo hi
    down:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: "echo {{ ti.xcom_pull(task_ids='up')['count'] }}"
      dependencies: [up]
"""
    result = validator.validate(y)
    assert result["valid"] is True
    assert not [w for w in result["warnings"] if "task_ids" in w], result["warnings"]
