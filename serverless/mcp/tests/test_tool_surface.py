# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""The MCP tool surface itself: both transports must expose the same tools, every
tool must be callable, and the documented count must match reality."""

import asyncio
import inspect
import json

import pytest

import app
import local_server


@pytest.fixture(scope="module")
def registered_tools():
    return asyncio.run(local_server.build_server().list_tools())


def test_both_transports_expose_the_same_tools(registered_tools):
    """local_server reads app.py's registry rather than redefining tools, so the two
    transports cannot drift."""
    assert {t.name for t in registered_tools} == set(app.mcp_server.tools)


def test_every_tool_has_a_description(registered_tools):
    missing = [t.name for t in registered_tools if not (t.description or "").strip()]
    assert not missing


def test_every_tool_has_an_input_schema(registered_tools):
    for tool in registered_tools:
        assert isinstance(tool.input_schema, dict)
        assert tool.input_schema.get("type") == "object"


def test_readme_tool_count_matches_reality(registered_tools):
    """The README used to claim 37 while the tables listed 38."""
    from pathlib import Path

    readme = (Path(__file__).resolve().parent.parent / "README.md").read_text()
    count = len(registered_tools)
    assert f"{count} tools" in readme or f"of the {count}" in readme, \
        f"README does not mention the real tool count ({count})"


def test_every_tool_returns_json():
    """Every tool returns a JSON string; the local wrapper depends on that, and an
    agent parses it."""
    offline_tools = {
        "get_server_config": {},
        "get_serverless_overview": {},
        "get_dag_yaml_spec": {},
        "get_serverless_constraints": {},
        "list_supported_operators": {},
        "describe_operator": {"operator": "GlueJobOperator"},
        "suggest_operator": {"intent": "run a glue job"},
        "list_pipeline_steps": {},
        "plan_pipeline": {"steps": ["glue_job"]},
        "build_dag_yaml": {"dag_id": "d",
                           "tasks": [{"task_id": "a", "operator": "EmptyOperator"}]},
        "validate_dag_yaml": {"yaml_content": "d:\n  tasks: {}\n"},
        "repair_dag_yaml": {"yaml_content": "d:\n  tasks: {}\n"},
        "get_code_bundle_guidance": {},
        "build_code_bundle": {"files": {"m.py": "def f(**c): pass\n"}},
        "check_dag_code_consistency": {"yaml_content": "d:\n  tasks: {}\n"},
        "generate_dag_yaml": {"dag_id": "d", "service": "s3"},
        "get_service_tasks_tool": {"service": "s3"},
        "compose_dag_yaml_tool": {"dag_id": "d", "services_config": [{"service": "s3"}]},
        "analyze_python_dag_tool": {"python_source": "x = 1\n"},
        "convert_python_to_yaml_tool": {"python_source": "x = 1\n"},
        "generate_execution_role": {
            "yaml_content": "d:\n  tasks:\n    t:\n"
                            "      operator: airflow.providers.standard.operators.empty.EmptyOperator\n"
        },
    }
    for name, kwargs in offline_tools.items():
        fn = app.mcp_server.tool_implementations[name]
        out = fn(**kwargs)
        assert isinstance(out, str), f"{name} did not return a string"
        json.loads(out)  # raises if it is not JSON


def test_tool_signatures_are_annotated():
    """The client's input schema is derived from the annotations, so an unannotated
    parameter becomes an untyped field."""
    for name, fn in app.mcp_server.tool_implementations.items():
        sig = inspect.signature(fn)
        for param in sig.parameters.values():
            assert param.annotation is not inspect.Parameter.empty, \
                f"{name}.{param.name} has no type annotation"


def test_destructive_tool_docstrings_say_so():
    """An agent reads these docstrings to decide whether to ask first."""
    expectations = {
        "mwaa_delete_workflows": ("EVERY workflow", "irreversible"),
        "mwaa_redeploy": ("DESTRUCTIVE", "OVERWRITES"),
        "mwaa_deploy_and_run": ("MUTATING", "UPDATED"),
        "mwaa_get_failed_runs": ("Bedrock", "DEFAULTS TO TRUE"),
        "preflight_dag_yaml": ("throwaway", "quota", "log_group_residue"),
    }
    for name, phrases in expectations.items():
        doc = inspect.getdoc(app.mcp_server.tool_implementations[name]) or ""
        for phrase in phrases:
            assert phrase in doc, f"{name} docstring should mention {phrase!r}"


def test_local_server_fails_loudly_if_handler_internals_change(monkeypatch):
    """local_server reads undocumented attributes of the Lambda handler. If a version
    bump removes them it must raise, not register zero tools and look healthy."""
    class Bare:
        tools = {"x": {}}

    monkeypatch.setattr(app, "mcp_server", Bare())
    with pytest.raises(RuntimeError, match="tool_implementations"):
        local_server.build_server()
