# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Static analysis of Python DAG files for MWAA Serverless compatibility.

Two things this module deliberately does NOT do any more:

1. Guess. It runs the real converter and reports exactly what the conversion would
   discard, in `conversion_would_drop`. Before, the analyser only checked operator
   and import admissibility, so it reported `compatible: True` for a DAG whose `<<`
   dependencies, tuple-assigned tasks or default_args keys would all silently
   vanish. A compatibility report that misses that is worse than none.

2. Match on substrings. `"eval" in "evaluate_model_quality"` and
   `"open" in "open_window_start"` both used to produce blockers, and any decorator
   whose name merely contained "task" was reported as a TaskFlow task.

The source is only ever parsed with ast.parse — never executed.
"""

import ast

from schema import SUPPORTED_OPERATORS

_SHORT_NAMES = set(SUPPORTED_OPERATORS.keys())
_FQNS = set(SUPPORTED_OPERATORS.values())

# PythonOperator and BashOperator used to be blockers. They are now supported: the
# YAML task references a callable or command, and the code itself is uploaded
# separately as a code bundle via the CreateWorkflow `Code` parameter.
_CODE_BUNDLE_MODULES = {
    "airflow.operators.python": (
        "PythonOperator is now SUPPORTED. Convert it to a YAML task with "
        "python_callable: '<module>.<function>' and move the callable into a code "
        "bundle module. Call get_code_bundle_guidance for the packaging rules."
    ),
    "airflow.providers.standard.operators.python": (
        "PythonOperator is supported. Provide the callable in a code bundle."
    ),
    "airflow.operators.bash": (
        "BashOperator is now SUPPORTED. Convert it to a YAML task with bash_command. "
        "Scripts run with /usr/local/airflow/dags as the working directory."
    ),
    "airflow.providers.standard.operators.bash": (
        "BashOperator is supported. Provide any script in a code bundle."
    ),
}

# TaskFlow decorators. Matched EXACTLY: a helper called @my_task_helper is not a task.
_TASKFLOW_DECORATORS = {"task", "task_group", "setup", "teardown"}

# Dynamic task mapping. .expand/.expand_kwargs are unambiguous Airflow APIs; .map is
# only treated as task mapping when its receiver is a task, because df.map(str) is
# ordinary Python.
_ALWAYS_MAPPING_METHODS = {"expand", "expand_kwargs"}
_TASK_ONLY_MAPPING_METHODS = {"map"}


def analyze_python_dag(source: str) -> dict:
    """Analyze Python DAG source for MWAA Serverless incompatibilities."""
    errors = []
    warnings = []
    info = []

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return {"compatible": False, "errors": [f"Syntax error: {e}"], "warnings": [],
                "info": [], "conversion_faithful": False, "conversion_would_drop": []}

    imports = _collect_imports(tree)
    task_vars = _collect_task_variables(tree, imports)

    _check_imports(imports, errors, warnings)
    _check_decorators(tree, errors, warnings)
    _check_operator_usage(tree, imports, warnings, info)
    _check_dynamic_mapping(tree, task_vars, errors)
    _check_deferrable(tree, errors)
    _check_callbacks(tree, warnings)
    _check_python_callables(tree, warnings)
    _check_python_logic(tree, warnings)
    _check_dag_construction(tree, imports, info, warnings)

    if not _defines_a_dag(tree, imports):
        errors.append(
            "No DAG definition found — this file does not appear to be an Airflow DAG. "
            "Expected `with DAG(...)`, `dag = DAG(...)` or an @dag-decorated function "
            "(aliased imports such as `from airflow import DAG as Dag` are recognised)."
        )

    # The authoritative compatibility signal: run the real conversion and report what
    # it would lose. Static checks alone cannot know this.
    dropped, conversion_errors = _conversion_findings(source)
    errors.extend(conversion_errors)

    result = {
        "compatible": not errors,
        "errors": errors,
        "warnings": warnings,
        "info": info,
        "conversion_faithful": not dropped,
        "conversion_would_drop": dropped,
    }
    if dropped:
        result["conversion_warning"] = (
            f"The conversion would DISCARD {len(dropped)} item(s) listed in "
            f"conversion_would_drop. The DAG may be 'compatible' in the sense that it "
            f"produces valid YAML while still not being the pipeline you wrote — check "
            f"every entry."
        )
    return result


def _conversion_findings(source):
    """(dropped, errors) from a trial conversion. Imported lazily to keep the
    dependency one-way and avoid a cycle if the converter ever imports this module."""
    try:
        from python_converter import convert_python_to_yaml
        converted = convert_python_to_yaml(source)
    except Exception as e:  # noqa: BLE001 - reported, never swallowed
        return [], [f"Trial conversion could not run ({type(e).__name__}: {e}); "
                    f"compatibility could not be confirmed."]
    return converted.get("dropped") or [], list(converted.get("errors") or [])


def _collect_imports(tree):
    """Collect all imports as {alias: full_module_path}."""
    imports = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports[alias.asname or alias.name] = alias.name
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            for alias in node.names:
                full = f"{mod}.{alias.name}" if mod else alias.name
                imports[alias.asname or alias.name] = full
    return imports


def _is_operator_name(name, imports):
    """Whether a called name refers to an Airflow operator or sensor."""
    if not name:
        return False
    resolved = imports.get(name, name)
    class_name = resolved.rsplit(".", 1)[-1]
    return (class_name in _SHORT_NAMES or resolved in _FQNS
            or class_name.endswith(("Operator", "Sensor")))


def _collect_task_variables(tree, imports):
    """Variable names bound to an operator instance.

    Used so `.map()` is only reported as dynamic task mapping when it is called on a
    task, rather than on any object that happens to have a map method.
    """
    task_vars = set()
    for node in ast.walk(tree):
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        if not targets:
            continue
        values = node.value.elts if isinstance(node.value, ast.Tuple) else [node.value]
        flat_targets = []
        for t in targets:
            flat_targets.extend(t.elts if isinstance(t, ast.Tuple) else [t])
        for target, value in zip(flat_targets, values * len(flat_targets), strict=False):
            if (isinstance(target, ast.Name) and isinstance(value, ast.Call)
                    and _is_operator_name(_get_call_name(value), imports)):
                task_vars.add(target.id)
    return task_vars


def _defines_a_dag(tree, imports):
    """Whether the module constructs a DAG, honouring import aliases."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _get_call_name(node)
            if not name:
                continue
            # `DAG(...)` directly, or any alias that resolves to airflow's DAG.
            if name == "DAG":
                return True
            resolved = imports.get(name, "")
            if resolved.rsplit(".", 1)[-1] == "DAG" and "airflow" in resolved:
                return True
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if _get_decorator_name(dec) == "dag":
                    return True
    return False


def _check_imports(imports, errors, warnings):
    for alias, full in imports.items():
        for mod, note in _CODE_BUNDLE_MODULES.items():
            if full.startswith(mod):
                warnings.append(f"Import '{full}': {note}")

        # @task imports from airflow.decorators are errors
        if full.startswith("airflow.decorators"):
            class_name = full.rsplit(".", 1)[-1]
            if class_name == "task" or (class_name == "decorators" and alias == "task"):
                errors.append(
                    f"Import '{full}': the @task decorator (TaskFlow API) is not supported. "
                    f"Rewrite the function as a plain callable and reference it from a "
                    f"PythonOperator task with python_callable."
                )

        # Check if importing an operator not in the allowlist
        if ".operators." in full or ".sensors." in full:
            class_name = full.rsplit(".", 1)[-1]
            if class_name not in _SHORT_NAMES and full not in _FQNS and class_name[:1].isupper():
                if class_name == "DummyOperator":
                    warnings.append(
                        "DummyOperator will be converted to EmptyOperator, which does nothing."
                    )
                elif "amazon.aws" not in full:
                    errors.append(
                        f"Operator '{class_name}' ({full}) is not an Amazon or standard "
                        f"provider operator — not supported in MWAA Serverless"
                    )
                else:
                    errors.append(
                        f"Operator '{class_name}' ({full}) not in MWAA Serverless "
                        f"operator allowlist"
                    )


def _check_decorators(tree, errors, warnings):
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            dec_name = _get_decorator_name(dec)
            if not dec_name:
                continue
            # Exact match. A substring test reported @my_task_helper as a TaskFlow task.
            if dec_name in _TASKFLOW_DECORATORS:
                errors.append(
                    f"@{dec_name} decorator on '{node.name}': the TaskFlow API is not "
                    f"supported. Rewrite it as a plain callable used by a PythonOperator task."
                )
            elif dec_name == "dag":
                warnings.append(
                    f"@dag decorator on '{node.name}': will be converted to a DAG object in YAML"
                )


def _get_decorator_name(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Call):
        return _get_decorator_name(node.func)
    return None


def _check_operator_usage(tree, imports, warnings, info):
    """Check operator instantiations."""
    operator_count = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _get_call_name(node)
        if not name:
            continue

        resolved = imports.get(name, name)
        class_name = resolved.rsplit(".", 1)[-1]

        if class_name in _SHORT_NAMES or resolved in _FQNS:
            operator_count += 1
        elif class_name.endswith(("Operator", "Sensor")):
            warnings.append(f"Operator '{class_name}' not in MWAA Serverless allowlist")

    if operator_count > 0:
        info.append(f"Found {operator_count} supported operator instantiation(s)")


def _get_call_name(node):
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _check_dynamic_mapping(tree, task_vars, errors):
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        method = node.func.attr
        if method in _ALWAYS_MAPPING_METHODS:
            errors.append(f".{method}() call: dynamic task mapping not supported")
        elif method in _TASK_ONLY_MAPPING_METHODS:
            receiver = node.func.value
            # Only a task can be task-mapped. `df.map(str)` is ordinary Python and used
            # to be reported as a blocker.
            is_task = (isinstance(receiver, ast.Name) and receiver.id in task_vars) or \
                      isinstance(receiver, ast.Call)
            if is_task:
                errors.append(
                    f".{method}() on a task: dynamic task mapping not supported"
                )


def _check_deferrable(tree, errors):
    for node in ast.walk(tree):
        if (isinstance(node, ast.keyword) and node.arg == "deferrable"
                and isinstance(node.value, ast.Constant) and node.value.value is True):
            errors.append("deferrable=True: deferrable operators not supported")


def _check_callbacks(tree, warnings):
    callback_params = {
        "on_failure_callback", "on_success_callback", "on_retry_callback",
        "on_execute_callback", "on_skipped_callback", "sla_miss_callback",
    }
    seen = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg in callback_params and node.arg not in seen:
            seen.add(node.arg)
            warnings.append(f"'{node.arg}' is ignored by MWAA Serverless")


def _check_python_callables(tree, warnings):
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "python_callable":
            warnings.append(
                "python_callable: PythonOperator is supported, but the callable must live in a "
                "code bundle module and be referenced as 'module_name.function_name' in the YAML "
                "(not as a Python reference). Extract the function into its own .py file."
            )
            return


# Module-level Python whose RESULT cannot be carried into YAML. These are warnings,
# not blockers: `import os` to read an environment variable is near-universal in a DAG
# file and does not by itself prevent conversion. What actually matters is whether a
# value the converter cannot resolve ends up in a task argument, and that is reported
# authoritatively in conversion_would_drop.
_RUNTIME_VALUE_MODULES = {
    "zipfile", "io", "os", "sys", "subprocess", "tempfile", "pathlib",
    "json", "csv", "boto3", "botocore", "requests", "urllib",
}

# Matched as exact dotted names or dotted prefixes, never as substrings.
_RUNTIME_VALUE_CALLS = {
    "os.environ.get", "os.environ", "os.getenv", "os.path",
    "open", "exec", "eval", "compile",
}


def _matches_runtime_call(call_str: str) -> str | None:
    """The _RUNTIME_VALUE_CALLS entry this call name matches, or None.

    Exact or dotted-prefix only: `evaluate_model_quality` must not match `eval`, and
    `open_window_start` must not match `open`.
    """
    if not call_str:
        return None
    for candidate in _RUNTIME_VALUE_CALLS:
        if call_str == candidate or call_str.startswith(candidate + "."):
            return candidate
    return None


def _check_python_logic(tree, warnings):
    """Flag module-level Python whose value YAML cannot hold."""
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mod_root = alias.name.split(".")[0]
                if mod_root in _RUNTIME_VALUE_MODULES:
                    warnings.append(
                        f"Module-level import '{alias.name}': anything computed with "
                        f"{mod_root} is only known at run time, so a task argument derived "
                        f"from it cannot be carried into YAML. Pass it as a DAG param instead."
                    )
        elif isinstance(node, ast.ImportFrom):
            mod_root = (node.module or "").split(".")[0]
            if mod_root in _RUNTIME_VALUE_MODULES:
                warnings.append(
                    f"Module-level import from '{node.module}': anything computed with "
                    f"{mod_root} is only known at run time. Pass it as a DAG param instead."
                )

        if isinstance(node, ast.Assign):
            for call_node in ast.walk(node.value):
                if not isinstance(call_node, ast.Call):
                    continue
                call_str = _get_full_call_name(call_node)
                matched = _matches_runtime_call(call_str)
                if matched:
                    warnings.append(
                        f"Module-level '{call_str}(...)': the value is computed at run time, "
                        f"so any task argument using it will be reported in "
                        f"conversion_would_drop. Pass it as a DAG param instead."
                    )
                    break


def _get_full_call_name(node):
    """Get dotted call name like 'os.environ.get'."""
    if isinstance(node, ast.Call):
        return _get_full_call_name(node.func)
    if isinstance(node, ast.Attribute):
        parent = _get_full_call_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _check_dag_construction(tree, imports, info, warnings):
    ignored_dag_kwargs = ("catchup", "tags", "access_control", "user_defined_macros",
                          "user_defined_filters", "render_template_as_native_obj")
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _get_call_name(node)
        if not name:
            continue
        resolved = imports.get(name, name)
        if name != "DAG" and resolved.rsplit(".", 1)[-1] != "DAG":
            continue
        info.append("Found DAG() constructor — will need conversion to YAML format")
        for kw in node.keywords:
            if kw.arg in ignored_dag_kwargs:
                warnings.append(f"DAG param '{kw.arg}' is ignored by MWAA Serverless")

    _check_dynamic_task_ids(tree, warnings)


def _check_dynamic_task_ids(tree, warnings):
    """Detect f-string task_id values and for-loop task generation."""
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "task_id" \
                and isinstance(node.value, ast.JoinedStr):
            warnings.append(
                "f-string task_id detected: the id is only known at run time, so every "
                "task built in that loop collapses onto one YAML key. Write the tasks out."
            )
        if not isinstance(node, ast.For):
            continue
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                call_name = _get_call_name(child)
                if call_name and ("Operator" in call_name or "Sensor" in call_name):
                    warnings.append(
                        f"For-loop creating '{call_name}' tasks: dynamic task generation "
                        f"is not supported in YAML — write each task out explicitly."
                    )
                    break
