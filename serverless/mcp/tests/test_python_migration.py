# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Python DAG -> YAML conversion and compatibility analysis.

The theme of this file: NOTHING IS DISCARDED SILENTLY. Each test either asserts a
construct now converts correctly, or asserts that what cannot be converted appears in
`dropped` with `faithful: False`.
"""

from pathlib import Path

import pytest

import python_analyzer
import python_converter

HEADER = "from airflow import DAG\nfrom airflow.operators.empty import EmptyOperator\n"
EMPTY_OP = "airflow.providers.standard.operators.empty.EmptyOperator"


def convert(body, header=HEADER):
    return python_converter.convert_python_to_yaml(header + body)


def deps_of(result, task_id):
    import yaml
    dag = next(iter(yaml.safe_load(result["yaml"]).values()))
    return dag["tasks"][task_id].get("dependencies", [])


def task_ids(result):
    import yaml
    dag = next(iter(yaml.safe_load(result["yaml"]).values()))
    return sorted(dag["tasks"])


# ── Security: the source is parsed, never executed ──────────────────────────

def test_source_is_never_executed():
    """If the module were imported or exec'd, this would raise SystemExit."""
    result = convert("import sys\nsys.exit(3)\nwith DAG(dag_id='d') as dag:\n"
                     "    a = EmptyOperator(task_id='a')\n")
    assert task_ids(result) == ["a"]


def test_converter_has_no_dynamic_execution():
    """Assert on the AST, not on the text: the module's own docstring names these
    constructs to explain that it does not use them."""
    import ast

    tree = ast.parse(Path(python_converter.__file__).read_text())
    forbidden = {"exec", "eval", "compile", "__import__", "literal_eval"}
    called = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "id", None) or getattr(func, "attr", None)
            if name:
                called.add(name)
        elif isinstance(node, ast.Import):
            called.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            called.add((node.module or "").split(".")[0])
    assert not (forbidden & called), f"converter must not use {forbidden & called}"
    assert "importlib" not in called
    # ast.parse is the only entry point for customer source.
    assert "parse" in called


# ── Dependency extraction ───────────────────────────────────────────────────

def test_rshift_dependency():
    r = convert("with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='a')\n"
                "    b=EmptyOperator(task_id='b')\n    a >> b\n")
    assert deps_of(r, "b") == ["a"]
    assert r["faithful"] is True


def test_lshift_dependency_is_not_dropped():
    """ast.LShift was never handled, so `b << a` produced two parallel tasks."""
    r = convert("with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='a')\n"
                "    b=EmptyOperator(task_id='b')\n    b << a\n")
    assert deps_of(r, "b") == ["a"]
    assert r["faithful"] is True


def test_mixed_chain_direction():
    """`a >> b << c` is `(a >> b) << c`, so it means a->b and c->b."""
    r = convert("with DAG(dag_id='d') as dag:\n"
                "    a=EmptyOperator(task_id='a')\n"
                "    b=EmptyOperator(task_id='b')\n"
                "    c=EmptyOperator(task_id='c')\n"
                "    a >> b << c\n")
    assert sorted(deps_of(r, "b")) == ["a", "c"]


def test_fan_out_and_fan_in():
    r = convert("with DAG(dag_id='d') as dag:\n"
                "    a=EmptyOperator(task_id='a')\n"
                "    b=EmptyOperator(task_id='b')\n"
                "    c=EmptyOperator(task_id='c')\n"
                "    e=EmptyOperator(task_id='e')\n"
                "    a >> [b, c] >> e\n")
    assert deps_of(r, "b") == ["a"]
    assert deps_of(r, "c") == ["a"]
    assert sorted(deps_of(r, "e")) == ["b", "c"]


@pytest.mark.parametrize("call", ["a.set_downstream(b)", "b.set_upstream(a)"])
def test_set_relation_methods(call):
    r = convert(f"with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='a')\n"
                f"    b=EmptyOperator(task_id='b')\n    {call}\n")
    assert deps_of(r, "b") == ["a"]


def test_unresolvable_dependency_endpoint_is_reported():
    """An edge to a task built in a helper used to be dropped by an `if` with no else."""
    r = convert("def make(): return EmptyOperator(task_id='hidden')\n"
                "with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='a')\n"
                "    h=make()\n    a >> h\n")
    assert r["faithful"] is False
    assert any(d["what"] == "dependency" for d in r["dropped"])


# ── Task discovery ──────────────────────────────────────────────────────────

def test_tuple_assignment_tasks_are_found():
    """This produced `tasks: {}` and still reported valid and deployable."""
    r = convert("with DAG(dag_id='d') as dag:\n"
                "    a, b = EmptyOperator(task_id='a'), EmptyOperator(task_id='b')\n"
                "    a >> b\n")
    assert task_ids(r) == ["a", "b"]
    assert deps_of(r, "b") == ["a"]


def test_annotated_assignment_tasks_are_found():
    r = convert("with DAG(dag_id='d') as dag:\n"
                "    a: EmptyOperator = EmptyOperator(task_id='a')\n")
    assert task_ids(r) == ["a"]


def test_duplicate_task_id_is_reported():
    r = convert("with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='same')\n"
                "    b=EmptyOperator(task_id='same')\n")
    assert r["faithful"] is False
    assert any("duplicate" in d["reason"] for d in r["dropped"])


# ── DAG discovery ───────────────────────────────────────────────────────────

def test_second_dag_is_not_merged_into_the_first():
    """Tasks from every DAG used to be pooled into the first dag_id."""
    r = convert("with DAG(dag_id='one') as d1:\n    a=EmptyOperator(task_id='a')\n"
                "with DAG(dag_id='two') as d2:\n    b=EmptyOperator(task_id='b')\n")
    assert task_ids(r) == ["a"], "the second DAG's tasks must not leak into the first"
    assert r["valid"] is False
    assert any("2 DAGs" in e for e in r["errors"])


def test_aliased_dag_import_is_recognised():
    r = python_converter.convert_python_to_yaml(
        "from airflow.models import DAG as Dag\n"
        "from airflow.operators.empty import EmptyOperator\n"
        "with Dag(dag_id='d9', schedule='@daily') as dag:\n"
        "    a=EmptyOperator(task_id='a')\n"
    )
    assert "d9:" in r["yaml"]
    assert "schedule: '@daily'" in r["yaml"], "schedule was silently dropped for aliased DAGs"


# ── Value decoding: no corruption, no silent loss ────────────────────────────

def test_non_literal_argument_is_reported_not_dropped():
    r = convert("BUCKET='b'\nwith DAG(dag_id='d') as dag:\n"
                "    a=EmptyOperator(task_id='a', doc_md=BUCKET)\n")
    assert r["faithful"] is False
    assert any(d["where"].endswith(".doc_md") for d in r["dropped"])


def test_container_with_a_variable_is_not_corrupted_to_null():
    """`tags=[BUCKET, 'x']` used to emit `[null, 'x']` — a value never written."""
    r = convert("BUCKET='b'\nwith DAG(dag_id='d') as dag:\n"
                "    a=EmptyOperator(task_id='a', doc_md=[BUCKET, 'x'])\n")
    assert "null" not in r["yaml"]
    assert r["faithful"] is False


def test_dict_with_a_variable_is_not_corrupted():
    r = convert("BUCKET='b'\nwith DAG(dag_id='d') as dag:\n"
                "    a=EmptyOperator(task_id='a', doc_md={'k': BUCKET, 'lit': 'v'})\n")
    assert "null" not in r["yaml"]
    assert r["faithful"] is False


def test_fstring_is_reported_not_substituted():
    """An f-string became the literal '<f-string: manual conversion needed>', which
    passes validation and then fails at run time."""
    r = convert("B='b'\nwith DAG(dag_id='d') as dag:\n"
                "    a=EmptyOperator(task_id='a', doc_md=f'x-{B}')\n")
    assert "manual conversion needed" not in r["yaml"]
    assert any("f-string" in d["reason"] for d in r["dropped"])


def test_kwargs_expansion_is_reported():
    r = convert("EXTRA={'x':1}\nwith DAG(dag_id='d') as dag:\n"
                "    a=EmptyOperator(task_id='a', **EXTRA)\n")
    assert any("**kwargs" in d["where"] for d in r["dropped"])


def test_timedelta_becomes_seconds():
    r = python_converter.convert_python_to_yaml(
        "from datetime import timedelta\n" + HEADER +
        "with DAG(dag_id='d', default_args={'retry_delay': timedelta(minutes=5)}) as dag:\n"
        "    a=EmptyOperator(task_id='a')\n"
    )
    assert "retry_delay: 300" in r["yaml"]


def test_explicit_none_and_zero_survive():
    """`schedule=None` and `max_active_runs=0` were dropped by truthiness tests."""
    r = convert("with DAG(dag_id='d', max_active_runs=0) as dag:\n"
                "    a=EmptyOperator(task_id='a')\n")
    assert "max_active_runs: 0" in r["yaml"]


def test_unhonoured_default_args_are_reported():
    r = python_converter.convert_python_to_yaml(
        "from datetime import timedelta\n" + HEADER +
        "with DAG(dag_id='d', default_args={'owner':'me','depends_on_past':True,"
        "'sla':timedelta(hours=1),'email':['a@b.c']}) as dag:\n"
        "    a=EmptyOperator(task_id='a')\n"
    )
    dropped_keys = {d["where"] for d in r["dropped"]}
    assert {"depends_on_past", "sla", "email"} <= dropped_keys
    assert "owner: me" in r["yaml"]


def test_python_callable_keeps_the_real_function_name():
    r = python_converter.convert_python_to_yaml(
        "from airflow import DAG\nfrom airflow.operators.python import PythonOperator\n"
        "def my_transform(**c): pass\n"
        "with DAG(dag_id='d') as dag:\n"
        "    a=PythonOperator(task_id='p1', python_callable=my_transform)\n"
    )
    assert "REPLACE_MODULE.my_transform" in r["yaml"], \
        "the callable name was available in the AST and used to be replaced by the task_id"


def test_long_chain_does_not_raise():
    n = 1500
    body = "with DAG(dag_id='big') as dag:\n"
    body += "".join(f"    t{i}=EmptyOperator(task_id='t{i}')\n" for i in range(n))
    body += "    " + " >> ".join(f"t{i}" for i in range(n)) + "\n"
    r = convert(body)  # must not raise RecursionError
    assert isinstance(r["valid"], bool)


# ── Analyzer ────────────────────────────────────────────────────────────────

def analyse(body, header=HEADER):
    return python_analyzer.analyze_python_dag(header + body)


@pytest.mark.parametrize("body", [
    # "eval" inside "evaluate_model_quality", "open" inside "open_window_start"
    "score = evaluate_model_quality('x')\nopened = open_window_start()\n"
    "with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='a')\n",
    # ordinary .map on a dataframe, not task mapping
    "def h(df): return df.map(str)\n"
    "with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='a')\n",
])
def test_analyzer_no_longer_reports_false_blockers(body):
    result = analyse(body)
    assert result["compatible"] is True, result["errors"]


def test_analyzer_import_os_is_a_warning_not_a_blocker():
    result = analyse("with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='a')\n",
                     header="import os\n" + HEADER)
    assert result["compatible"] is True
    assert any("os" in w for w in result["warnings"])


def test_analyzer_still_blocks_real_taskflow():
    result = analyse("@task\ndef t(): pass\n"
                     "with DAG(dag_id='d') as dag:\n    a=EmptyOperator(task_id='a')\n",
                     header="from airflow.decorators import task\n" + HEADER)
    assert result["compatible"] is False


def test_analyzer_still_blocks_dynamic_mapping():
    result = analyse("with DAG(dag_id='d') as dag:\n"
                     "    a=EmptyOperator.partial(task_id='a').expand(x=[1,2])\n")
    assert result["compatible"] is False


def test_analyzer_reports_what_the_conversion_would_drop():
    """The whole point: a DAG can be 'compatible' and still lose data."""
    result = analyse("BUCKET='b'\nwith DAG(dag_id='d') as dag:\n"
                     "    a=EmptyOperator(task_id='a', doc_md=BUCKET)\n")
    assert result["conversion_faithful"] is False
    assert result["conversion_would_drop"]
    assert "conversion_warning" in result


def test_analyzer_accepts_aliased_dag_import():
    result = python_analyzer.analyze_python_dag(
        "from airflow.models import DAG as Dag\n"
        "from airflow.operators.empty import EmptyOperator\n"
        "with Dag(dag_id='d', schedule='@daily') as dag:\n"
        "    a=EmptyOperator(task_id='a')\n"
    )
    assert result["compatible"] is True, result["errors"]
