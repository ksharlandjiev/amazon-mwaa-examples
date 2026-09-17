#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Check every generated IAM policy against live IAM, without creating anything.

    python verify_generated_policies.py            # all 29 demo templates
    python verify_generated_policies.py ec2 s3     # just these

Why this exists: `_WILDCARD_ONLY_ACTIONS` in tools.py is a claim about IAM, and a wrong
entry fails in a way nothing local can see. A resource-scoped action that IAM refuses to
scope evaluates to implicitDeny at run time while the policy stays syntactically valid,
cfn-lint passes and the unit tests pass. This sample shipped exactly that bug in its own
Lambda policy once already.

So the set is verified the only way it can be: ask IAM. `iam:SimulateCustomPolicy`
evaluates a policy document that is never attached to anything, against the real
authorisation engine. It is a read-only call — it creates no role, policy or resource.

Requires credentials with iam:SimulateCustomPolicy. Exit code 1 means the generated
policies and IAM disagree; the report names each action and which direction to fix.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import boto3  # noqa: E402
import tools  # noqa: E402

ACCOUNT = boto3.client("sts").get_caller_identity()["Account"]
REGION = boto3.session.Session().region_name or "us-east-1"
CALLER = boto3.client("sts").get_caller_identity()["Arn"]
iam = boto3.client("iam")


def _simulate(policy, action, resource):
    """True if `action` on `resource` is allowed by `policy` alone."""
    import json

    result = iam.simulate_custom_policy(
        PolicyInputList=[json.dumps(policy)],
        ActionNames=[action],
        ResourceArns=[resource] if resource != "*" else ["*"],
    )
    return result["EvaluationResults"][0]["EvalDecision"]


def check_service(service):
    """Simulate every action in one demo's generated policy. Returns a list of problems."""
    dag_yaml = tools.generate_yaml(f"demo_{service}", service)["dag_yaml"]
    generated = tools.generate_execution_role_policy(
        dag_yaml, account_id=ACCOUNT, region=REGION
    )
    policy = generated["permissions_policy"]
    problems = []

    for statement in policy["Statement"]:
        actions = statement["Action"]
        actions = actions if isinstance(actions, list) else [actions]
        resource = statement["Resource"]
        resources = resource if isinstance(resource, list) else [resource]
        scoped = resources != ["*"]

        for action in actions:
            # Simulating against the statement's own resource answers the only question
            # that matters: does this grant actually authorise this call?
            decision = _simulate(policy, action, resources[0])
            allowed = decision == "allowed"

            if scoped and not allowed:
                problems.append(
                    f"DENIED WHILE SCOPED  {action}\n"
                    f"    statement {statement['Sid']} scopes it to {resources[0]}\n"
                    f"    IAM says {decision}. If IAM defines no resource type for this\n"
                    f"    action, add it to _WILDCARD_ONLY_ACTIONS."
                )
            elif not scoped and allowed:
                # Confirm the wildcard is actually required: if an ARN also works, the
                # action does support resource-level permissions and belongs in the
                # scoped statement instead.
                probe = {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Action": action,
                        "Resource": f"arn:aws:{action.split(':')[0]}:{REGION}:{ACCOUNT}:*",
                    }],
                }
                arn = f"arn:aws:{action.split(':')[0]}:{REGION}:{ACCOUNT}:*"
                if _simulate(probe, action, arn) == "allowed":
                    problems.append(
                        f"WILDCARD NOT NEEDED  {action}\n"
                        f"    it is in _WILDCARD_ONLY_ACTIONS, but an ARN authorises it too.\n"
                        f"    Remove it from the set so it gets scoped."
                    )
    return problems


def main():
    from tests.conftest import ALL_DEMO_SERVICES  # noqa: PLC0415

    services = sys.argv[1:] or ALL_DEMO_SERVICES
    print(f"account {ACCOUNT}  region {REGION}")
    print(f"caller  {CALLER}")
    print(f"simulating {len(services)} demo policies (read-only; creates nothing)\n")

    all_problems = {}
    for service in services:
        problems = check_service(service)
        status = "ok" if not problems else f"{len(problems)} problem(s)"
        print(f"  {service:24} {status}")
        if problems:
            all_problems[service] = problems

    if not all_problems:
        print("\nAll generated policies agree with live IAM.")
        print(f"_WILDCARD_ONLY_ACTIONS is correct for these {len(services)} templates.")
        return 0

    print("\n" + "=" * 70)
    for service, problems in all_problems.items():
        print(f"\n{service}:")
        for problem in problems:
            print(f"  {problem}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
