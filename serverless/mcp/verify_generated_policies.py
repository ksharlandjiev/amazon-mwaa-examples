#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Check `_WILDCARD_ONLY_ACTIONS` against live IAM, without creating anything.

    python verify_generated_policies.py            # every action in the operator map
    python verify_generated_policies.py ec2 s3     # only these services

Why this exists: `_WILDCARD_ONLY_ACTIONS` in tools.py is a claim about IAM, and a wrong
entry fails in a way nothing local can see. An action that IAM refuses to resource-scope
evaluates to implicitDeny at run time while the policy stays syntactically valid, cfn-lint
passes and the unit tests pass. This sample shipped exactly that bug twice: once in its own
Lambda policy, and once in the generated execution-role policy, where a unit test asserting
"no Resource '*' anywhere" actively required the broken form.

HOW THE CHECK WORKS. For each action it builds a one-statement policy granting that action
on the SCOPED ARN the generator would use, and asks iam:SimulateCustomPolicy to evaluate
it. The policy is never attached to anything and no resource is created — it is a read-only
call against the real authorisation engine.

    allowed      -> the action supports resource-level permissions, so it must NOT be in
                    _WILDCARD_ONLY_ACTIONS (having it there over-grants).
    implicitDeny -> IAM will not scope this action, so it MUST be in the set or the
                    generated policy denies it at run time.

Both directions come from the same probe, which matters: an earlier version of this script
used a second, differently-shaped probe for the reverse direction and produced false
positives on every RDS and Comprehend action.

Statements carrying a Condition are simulated with matching context entries, since without
them the condition fails and every result is a meaningless deny.

Requires credentials with iam:SimulateCustomPolicy. Exit code 1 means IAM and the set
disagree; the report names each action and which way to fix it.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import boto3  # noqa: E402

import tools  # noqa: E402

# A bucket name to stand in for the ${BUCKET_NAME} placeholder. Simulating against the
# literal placeholder tests an ARN no caller would ever use.
PROBE_BUCKET = "mwaa-serverless-mcp-probe-bucket"

sts = boto3.client("sts")
ACCOUNT = sts.get_caller_identity()["Account"]
CALLER = sts.get_caller_identity()["Arn"]
REGION = boto3.session.Session().region_name or "us-east-1"
iam = boto3.client("iam")

def _context_entries(condition):
    """Context that satisfies a statement's own condition.

    The values are taken FROM the condition rather than hardcoded. The point of the probe
    is to test whether IAM will scope the action to a resource, so the condition has to
    pass — otherwise every conditioned statement reports a meaningless deny. An earlier
    version hardcoded airflow-serverless.amazonaws.com and reported iam:PassRole as
    unscopable in every demo that passes a role to CloudFormation instead.
    """
    entries = []
    for operator, keys in (condition or {}).items():
        if not operator.startswith("StringEquals"):
            # Only exact-match conditions can be satisfied by echoing the policy's value.
            continue
        for key, value in keys.items():
            values = value if isinstance(value, list) else [value]
            # One value, typed "string". The type must match the CONDITION KEY's type,
            # not the number of alternatives the policy accepts — iam:PassedToService is
            # a String key, and sending "stringList" because the policy listed two
            # principals made every multi-principal PassRole statement report a deny.
            entries.append({
                "ContextKeyName": key,
                "ContextKeyValues": [str(values[0])],
                "ContextKeyType": "string",
            })
    return entries


def _concrete(arn):
    """Substitute placeholders so the ARN is one that could really exist."""
    return (arn.replace(tools._BUCKET_PLACEHOLDER, PROBE_BUCKET)
               .replace(tools._ACCOUNT_PLACEHOLDER, ACCOUNT)
               .replace(tools._REGION_PLACEHOLDER, REGION))


def _is_allowed(action, resources, condition=None):
    """Whether a policy granting `action` on `resources` authorises `action`.

    Tested against every resource in the statement, allowed if any one of them works. S3
    statements carry both the bucket and the bucket/* form, and object actions only match
    the second — checking just the first reports a working policy as broken.
    """
    statement = {"Effect": "Allow", "Action": action, "Resource": resources}
    if condition:
        statement["Condition"] = condition
    policy = json.dumps({"Version": "2012-10-17", "Statement": [statement]})
    entries = _context_entries(condition)

    decisions = []
    for resource in resources:
        kwargs = {
            "PolicyInputList": [policy],
            "ActionNames": [action],
            "ResourceArns": [resource],
        }
        if entries:
            kwargs["ContextEntries"] = entries
        decision = iam.simulate_custom_policy(**kwargs)["EvaluationResults"][0]["EvalDecision"]
        decisions.append(decision)
        if decision == "allowed":
            return True, decisions
    return False, decisions


def _actions_for(services):
    """Every action the operator map can emit, for the given demo services."""
    actions = {}
    for service in services:
        dag_yaml = tools.generate_yaml(f"demo_{service}", service)["dag_yaml"]
        generated = tools.generate_execution_role_policy(
            dag_yaml, account_id=ACCOUNT, region=REGION,
            passable_role_arns=[f"arn:aws:iam::{ACCOUNT}:role/mwaa-serverless-probe-role"],
            include_destructive_actions=True,
        )
        for statement in generated["permissions_policy"]["Statement"]:
            resource = statement["Resource"]
            resources = [_concrete(r) for r in
                         (resource if isinstance(resource, list) else [resource])]
            names = statement["Action"]
            for action in (names if isinstance(names, list) else [names]):
                # Keep the first statement each action appears in; they are identical
                # across demos by construction.
                actions.setdefault(action, (resources, statement.get("Condition")))
    return actions


def main():
    from tests.conftest import ALL_DEMO_SERVICES  # noqa: PLC0415

    services = sys.argv[1:] or ALL_DEMO_SERVICES
    print(f"account {ACCOUNT}  region {REGION}")
    print(f"caller  {CALLER}")
    print("simulating (read-only; creates nothing)\n")

    actions = _actions_for(services)
    # Resource "*" statements tell us nothing — that is the answer, not the question.
    scoped = {a: v for a, v in actions.items() if v[0] != ["*"]}
    unscoped = {a for a, v in actions.items() if v[0] == ["*"]}

    print(f"{len(actions)} distinct actions across {len(services)} demo(s)")
    print(f"  {len(unscoped)} granted on \"*\" (already in _WILDCARD_ONLY_ACTIONS)")
    print(f"  {len(scoped)} granted on an ARN — simulating each\n")

    should_be_wildcard = []   # scoped but IAM denies -> add to the set
    should_be_scoped = []     # in the set but an ARN works -> remove from the set

    for action in sorted(scoped):
        resources, condition = scoped[action]
        allowed, decisions = _is_allowed(action, resources, condition)
        if not allowed:
            should_be_wildcard.append((action, resources, decisions))
            print(f"  DENIED  {action}")

    for action in sorted(unscoped):
        # Ask the same question of the actions already excused: would an ARN have worked?
        service = action.split(":", 1)[0]
        probe = [f"arn:aws:{service}:{REGION}:{ACCOUNT}:*"]
        allowed, _ = _is_allowed(action, probe)
        if allowed:
            should_be_scoped.append(action)
            print(f"  SCOPABLE  {action}")

    if not should_be_wildcard and not should_be_scoped:
        print("\nIAM agrees with _WILDCARD_ONLY_ACTIONS for every action checked.")
        return 0

    print("\n" + "=" * 72)
    if should_be_wildcard:
        print("\nIAM WILL NOT SCOPE THESE — the generated policy denies them at run time.")
        print("Add to _WILDCARD_ONLY_ACTIONS in src/tools.py:\n")
        for action, _resources, _decisions in should_be_wildcard:
            print(f'    "{action}",')
        print("\n  (evidence)")
        for action, resources, decisions in should_be_wildcard:
            print(f"    {action}: {decisions} against {resources}")
    if should_be_scoped:
        print("\nTHESE ARE SCOPABLE — leaving them in the set grants more than needed.")
        print("Remove from _WILDCARD_ONLY_ACTIONS in src/tools.py:\n")
        for action in should_be_scoped:
            print(f'    "{action}",')
    return 1


if __name__ == "__main__":
    sys.exit(main())
