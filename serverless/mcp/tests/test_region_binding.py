# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""The server works in one AWS Region per process, and must say so rather than let a
caller infer it.

A wrong Region is not an error: an empty workflow list from the wrong Region looks
exactly like an empty workflow list from the right one. These tests pin the two things
that make that detectable — Region reporting, and rejecting arguments that do not exist
(notably `region`, which a caller might reasonably assume works).

Nothing here talks to AWS; the Region is read from a stubbed boto3 session.
"""

import asyncio
import json

import pytest

import app
import local_server
import operations


@pytest.fixture
def region_env(monkeypatch):
    """Control both Region variables and what boto3 resolves, independently."""
    def _set(resolved, aws_region=None, default_region=None, profile=None):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("AWS_PROFILE", raising=False)
        if aws_region is not None:
            monkeypatch.setenv("AWS_REGION", aws_region)
        if default_region is not None:
            monkeypatch.setenv("AWS_DEFAULT_REGION", default_region)
        if profile is not None:
            monkeypatch.setenv("AWS_PROFILE", profile)

        class _Session:
            region_name = resolved

        monkeypatch.setattr(operations.boto3, "Session", lambda *a, **k: _Session())
        return operations.describe_region()
    return _set


def test_reports_the_effective_region_and_that_it_is_fixed(region_env):
    d = region_env(resolved="eu-west-1", default_region="eu-west-1")
    assert d["effective_region"] == "eu-west-1"
    assert "AWS_DEFAULT_REGION" in d["source"]
    assert d["fixed_for_process_lifetime"] is True


def test_attributes_the_region_to_the_profile_when_no_env_var_is_set(region_env):
    d = region_env(resolved="us-east-1", profile="myprofile")
    assert d["effective_region"] == "us-east-1"
    assert "myprofile" in d["source"]


def test_aws_region_alone_is_reported_as_ignored(region_env):
    """botocore reads AWS_DEFAULT_REGION, not AWS_REGION. Setting only AWS_REGION leaves
    the Region coming from the profile, which is the silent-wrong-Region trap."""
    d = region_env(resolved="us-east-1", aws_region="eu-west-1")
    assert "ignored_aws_region" in d
    assert "eu-west-1" in d["ignored_aws_region"]
    assert "AWS_DEFAULT_REGION" in d["ignored_aws_region"]
    # and it must not claim AWS_REGION as the source
    assert "AWS_REGION" not in d["source"]


def test_no_ignored_warning_when_both_variables_agree(region_env):
    d = region_env(resolved="eu-west-1", aws_region="eu-west-1", default_region="eu-west-1")
    assert "ignored_aws_region" not in d


def test_missing_region_is_reported_as_a_problem(region_env):
    d = region_env(resolved=None)
    assert d["effective_region"] is None
    assert "problem" in d
    assert "AWS_DEFAULT_REGION" in d["problem"]


def test_get_server_config_surfaces_the_region(region_env):
    region_env(resolved="ap-south-1", default_region="ap-south-1")
    cfg = json.loads(app.get_server_config())
    assert cfg["aws_region"]["effective_region"] == "ap-south-1"


# ── unknown arguments must be rejected, not silently dropped ──────────────

def _call(server, name, arguments):
    return asyncio.run(server._tool_manager.call_tool(name, arguments, None))


@pytest.fixture(scope="module")
def server():
    return local_server.build_server()


def test_unknown_argument_is_rejected(server):
    """The Lambda handler calls tool_func(**arguments), so an unsupported argument
    raises. The stdio library would silently drop it, so the two transports disagreed:
    mwaa_list_workflows(region=...) returned another Region's data with no error."""
    with pytest.raises(Exception) as excinfo:
        _call(server, "mwaa_list_workflows", {"region": "us-west-2"})
    msg = str(excinfo.value)
    assert "region" in msg
    assert "name_contains" in msg          # names what is accepted
    assert "get_server_config" in msg      # points at how to see the real Region


def test_rejection_message_explains_there_is_no_region_argument(server):
    with pytest.raises(Exception) as excinfo:
        _call(server, "mwaa_get_workflow", {"workflow_name": "x", "region": "us-west-2"})
    assert "no tool takes a Region argument" in str(excinfo.value)


def test_misspelled_argument_is_rejected(server):
    with pytest.raises(Exception) as excinfo:
        _call(server, "mwaa_list_workflows", {"name_contain": "x"})
    assert "name_contain" in str(excinfo.value)


def test_valid_arguments_are_still_accepted(server):
    """The check must not reject legitimate calls. This tool needs no AWS access."""
    out = _call(server, "build_dag_yaml", {
        "dag_id": "region_test",
        "tasks": [{"task_id": "w", "operator": "GlueJobSensor",
                   "params": {"job_name": "j", "run_id": "r"}}],
    })
    payload = out if isinstance(out, str) else out[0].text
    assert json.loads(payload)["valid"] is True


def test_no_argument_tools_still_work(server):
    out = _call(server, "get_serverless_overview", {})
    payload = out if isinstance(out, str) else out[0].text
    assert "authoring_policy" in payload
