# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test configuration.

`src/` is on sys.path rather than being a package, because that is how the Lambda
runtime sees it: the handler is `app.handler` with CodeUri: src/, so the modules
import each other by bare name. The tests exercise the same import shape.

NOTHING here talks to AWS. Every AWS-facing test uses a stub client.
"""

import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

ALL_DEMO_SERVICES = [
    "s3", "glue", "athena", "bedrock", "lambda", "emr_serverless", "emr", "batch",
    "step_functions", "redshift", "sns", "sqs", "ecs", "eks", "cloudformation",
    "sagemaker", "rds", "ec2", "eventbridge", "comprehend", "dms",
    "kinesis_analytics", "neptune", "glacier", "datasync", "appflow", "quicksight",
    "dynamodb", "opensearch_serverless",
]


@pytest.fixture
def tasks_yaml():
    """A minimal valid definition, parameterised by extra DAG-level lines."""
    def _build(dag_lines="", task_body=None):
        body = task_body or "      operator: airflow.providers.standard.operators.empty.EmptyOperator\n"
        return f"d:\n{dag_lines}  tasks:\n    t:\n{body}"
    return _build


@pytest.fixture
def stub_mwaa_client():
    """A two-page mwaa-serverless client stub. Records every destructive call."""
    class Stub:
        def __init__(self, names=("prod-etl-nightly", "prod-billing", "page2-only")):
            self.names = list(names)
            self.deleted = []
            self.stopped = []
            self.started = []
            self.updated = []

        def _wf(self, name):
            return {"Name": name, "WorkflowStatus": "READY",
                    "WorkflowArn": f"arn:aws:airflow-serverless:us-east-1:111122223333:workflow/{name}"}

        # Deliberately has NO get_paginator, to exercise the NextToken fallback.
        def get_paginator(self, name):
            raise ValueError(f"no paginator for {name}")

        def list_workflows(self, **kwargs):
            if not kwargs.get("NextToken"):
                return {"Workflows": [self._wf(n) for n in self.names[:-1]], "NextToken": "p2"}
            return {"Workflows": [self._wf(self.names[-1])]}

        def list_workflow_runs(self, **kwargs):
            return {"WorkflowRuns": []}

        def delete_workflow(self, WorkflowArn, **kwargs):
            self.deleted.append(WorkflowArn)
            return {}

        def stop_workflow_run(self, **kwargs):
            self.stopped.append(kwargs)
            return {}

        def start_workflow_run(self, **kwargs):
            self.started.append(kwargs)
            return {"RunId": "run-1"}

        def update_workflow(self, **kwargs):
            self.updated.append(kwargs)
            return {}

    return Stub
