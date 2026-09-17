# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Convert Python DAG source to MWAA Serverless YAML via AST extraction.

The customer's Python is PARSED, never executed: ast.parse is the only entry point,
and every value is decoded by the _eval_const allowlist below. There is no exec,
eval, compile, importlib or ast.literal_eval anywhere in this module.

The design rule that matters most here: **nothing is discarded silently.** A
converter that quietly drops a dependency edge or an operator argument produces a
DAG that deploys cleanly and then does the wrong thing, which is far worse than one
that refuses. Every value this module cannot faithfully represent is recorded in the
returned `dropped` list, and `faithful` is False whenever that list is non-empty.
"""

import ast

import yaml

import validator
from schema import SUPPORTED_OPERATORS

_SHORT_NAMES = set(SUPPORTED_OPERATORS.keys())
_FQN_TO_SHORT = {v: k for k, v in SUPPORTED_OPERATORS.items()}

# Operators with no MWAA Serverless equivalent. PythonOperator and BashOperator are
# NOT in this set any more — they are supported and convert to real tasks.
_REPLACEABLE_WITH_EMPTY = {
    "DummyOperator", "ShortCircuitOperator", "BranchPythonOperator",
    "TriggerDagRunOperator", "ExternalTaskSensor", "LatestOnlyOperator",
}

# Operators whose code has to be supplied in a code bundle.
_CODE_OPERATORS = {"PythonOperator", "BashOperator"}

# default_args keys that survive conversion. Anything else is reported as dropped —
# see DEFAULT_ARGS_ALLOWLIST in constraints.py for what the service accepts.
_DEFAULT_ARGS_KEPT = ("owner", "retries", "retry_delay", "execution_timeout")

# Argument names consumed by the conversion itself rather than forwarded.
_NON_OPERATOR_KWARGS = ("task_id", "dag", "deferrable")

_TIMEDELTA_MULTIPLIERS = {
    "weeks": 604800, "days": 86400, "hours": 3600,
    "minutes": 60, "seconds": 1, "milliseconds": 0.001,
}


class _Unresolved:
    """A value the converter read but cannot represent in YAML.

    Returned instead of None so an argument that was explicitly `None` in the source
    stays distinguishable from one that could not be parsed. The previous code used
    None for both and dropped them with the same `elif val is not None` guard.
    """

    __slots__ = ("expr", "why")

    def __init__(self, node, why):
        self.expr = _unparse(node)
        self.why = why

    def __repr__(self):  # pragma: no cover - diagnostics only
        return f"<Unresolved {self.expr!r}: {self.why}>"


def _unparse(node):
    """Source text for an AST node, for use in a human-readable report."""
    try:
        text = ast.unparse(node)
    except Exception:  # noqa: BLE001 - unparse is best-effort diagnostics
        return f"<{type(node).__name__}>"
    return text if len(text) <= 120 else text[:117] + "..."


class _Report:
    """Accumulates everything the conversion could not carry over."""

    def __init__(self):
        self.dropped = []
        self.errors = []
        self.replacements = []
        self.code_actions = []

    def drop(self, what, where, expr, reason, action):
        self.dropped.append({
            "what": what,
            "where": where,
            "source": expr,
            "reason": reason,
            "action": action,
        })


def convert_python_to_yaml(source: str) -> dict:
    """Extract DAG structure from Python and produce MWAA Serverless YAML."""
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return {"yaml": None, "valid": False, "errors": [f"Syntax error: {e}"],
                "warnings": [], "hints": [], "replacements": [], "dropped": [],
                "faithful": False, "needs_code_bundle": False,
                "normalisations_applied": []}

    report = _Report()
    imports = _collect_imports(tree)
    dag_defs = _find_dag_definitions(tree, imports)

    if not dag_defs:
        report.errors.append(
            "No DAG definition found. Expected `with DAG(...)`, `dag = DAG(...)` or an "
            "@dag-decorated function. Aliased imports (`from airflow import DAG as Dag`) "
            "are resolved, so this means no DAG constructor is present at all."
        )
        return _empty_result(report)

    chosen = dag_defs[0]
    if len(dag_defs) > 1:
        # Previously every DAG's tasks were pooled into the FIRST dag_id and the other
        # DAGs vanished. One file, one workflow: convert the first and say so.
        others = ", ".join(d["dag_id"] or "<unnamed>" for d in dag_defs[1:])
        report.errors.append(
            f"This file defines {len(dag_defs)} DAGs ({', '.join(d['dag_id'] or '<unnamed>' for d in dag_defs)}). "
            f"MWAA Serverless accepts one DAG per definition. Only '{chosen['dag_id']}' was "
            f"converted; split {others} into separate files and convert each one."
        )
        for d in dag_defs[1:]:
            report.drop("dag", d["dag_id"] or "<unnamed>", d["source"],
                        "a definition may contain only one DAG",
                        "convert this DAG from its own file")

    dag_id = chosen["dag_id"] or "converted_dag"
    dag_kwargs = chosen["kwargs"]
    tasks, dep_edges = _extract_tasks(chosen["scope"], imports, report)

    yaml_tasks = []
    seen_ids = {}
    needs_code_bundle = False

    for t in tasks:
        tid = t["task_id"]
        if tid in seen_ids:
            # An unguarded dict assignment used to let the second task overwrite the
            # first with no report at all.
            report.drop("task", tid, t["source"],
                        f"duplicate task_id '{tid}' — the earlier task would be overwritten",
                        "give each task a unique task_id")
            continue
        seen_ids[tid] = True

        op = t["operator"]
        resolved = imports.get(op, op)
        short = _resolve_short_name(op, resolved)

        if short and short in _SHORT_NAMES:
            yaml_task = {"task_id": tid, "operator": short}
        elif op in _REPLACEABLE_WITH_EMPTY or resolved.rsplit(".", 1)[-1] in _REPLACEABLE_WITH_EMPTY:
            orig = resolved.rsplit(".", 1)[-1] if "." in resolved else op
            report.replacements.append(
                f"'{tid}': replaced {orig} with EmptyOperator. {orig} has no MWAA Serverless "
                f"equivalent, so the task now does NOTHING — reimplement its behaviour or "
                f"remove it."
            )
            yaml_task = {"task_id": tid, "operator": "EmptyOperator"}
        else:
            class_name = resolved.rsplit(".", 1)[-1] if "." in resolved else op
            report.errors.append(
                f"'{tid}': operator '{class_name}' has no supported equivalent"
            )
            yaml_task = {"task_id": tid, "operator": f"UNSUPPORTED:{class_name}"}

        params = {k: v for k, v in t["kwargs"].items() if k not in _NON_OPERATOR_KWARGS}

        if short in _CODE_OPERATORS:
            needs_code_bundle = True
            _handle_code_operator(short, tid, t, params, report)

        if params:
            yaml_task["parameters"] = params
        for key in ("retries", "retry_delay", "execution_timeout", "trigger_rule"):
            if t.get(key) is not None:
                yaml_task[key] = t[key]

        yaml_task["_source"] = t["source"]
        yaml_tasks.append(yaml_task)

    _wire_dependencies(yaml_tasks, dep_edges, report)

    dag_def = _build_dag_level(dag_kwargs, report)
    dag_def["tasks"] = _normalise_tasks(yaml_tasks)

    for tcfg in dag_def["tasks"].values():
        op = tcfg.get("operator", "")
        if op in SUPPORTED_OPERATORS:
            tcfg["operator"] = SUPPORTED_OPERATORS[op]

    result_yaml = yaml.dump({dag_id: dag_def}, default_flow_style=False,
                            sort_keys=False, width=4096, allow_unicode=True)

    # Run the result through repair + validation so the caller gets a definition that
    # is already in the shape the service accepts, and knows what still needs work.
    fixed = validator.repair(result_yaml)
    final_yaml = fixed["repaired_yaml"]
    check = fixed["validation"] or validator.validate(final_yaml)

    return _result(report, final_yaml, check, fixed["changes"], needs_code_bundle)


def _empty_result(report):
    return {
        "yaml": None,
        "valid": False,
        "faithful": False,
        "errors": report.errors,
        "warnings": [],
        "hints": [],
        "replacements": report.replacements,
        "dropped": report.dropped,
        "needs_code_bundle": False,
        "normalisations_applied": [],
        "next_step": "Resolve the errors above; nothing was converted.",
    }


def _result(report, final_yaml, check, changes, needs_code_bundle):
    faithful = not report.dropped
    errors = report.errors + check["errors"]
    out = {
        "yaml": final_yaml,
        # `valid` covers BOTH the schema check and the conversion itself: a file with
        # two DAGs produces schema-valid YAML for one of them, which must not report
        # as a clean conversion.
        "valid": not errors,
        # Distinct from `valid` on purpose: a definition can be schema-valid and still
        # not be the DAG the customer wrote.
        "faithful": faithful,
        "errors": errors,
        "warnings": check["warnings"],
        "hints": check["hints"],
        "replacements": report.replacements,
        "dropped": report.dropped,
        "normalisations_applied": changes,
        "needs_code_bundle": needs_code_bundle,
    }
    if report.dropped:
        out["dropped_summary"] = (
            f"{len(report.dropped)} item(s) from the source could NOT be represented in YAML "
            f"and are listed in `dropped`. The emitted definition is not equivalent to the "
            f"original DAG. Review every entry before deploying."
        )
    if report.code_actions:
        out["code_bundle_actions"] = report.code_actions
        out["code_bundle_next_step"] = (
            "Call get_code_bundle_guidance, put the callables in modules, then build_code_bundle "
            "and pass the result to mwaa_deploy_and_run as code_zip_base64."
        )
    if not check["valid"]:
        out["next_step"] = "Resolve the errors above; the definition is not deployable yet."
    elif errors:
        out["next_step"] = (
            "The YAML is schema-valid but the conversion reported errors — resolve them first."
        )
    elif report.dropped:
        out["next_step"] = (
            "The YAML is schema-valid but INCOMPLETE — work through `dropped` first, then "
            "preflight_dag_yaml before deploying."
        )
    else:
        out["next_step"] = (
            "Review replacements, then preflight_dag_yaml before deploying."
        )
    return out


def _handle_code_operator(short, tid, task, params, report):
    """PythonOperator/BashOperator carry code that YAML cannot hold."""
    if short == "PythonOperator":
        existing = params.get("python_callable")
        if not isinstance(existing, str) or "." not in existing:
            guess = task.get("callable_name") or tid
            params["python_callable"] = f"REPLACE_MODULE.{guess}"
            report.code_actions.append(
                f"'{tid}': set python_callable to '<module>.{guess}' and put '{guess}' in a "
                f"code-bundle module. It is currently the placeholder 'REPLACE_MODULE.{guess}'."
            )
    elif short == "BashOperator" and "bash_command" not in params:
        params["bash_command"] = "REPLACE_WITH_COMMAND"
        report.code_actions.append(
            f"'{tid}': bash_command could not be extracted — set it explicitly."
        )


def _wire_dependencies(yaml_tasks, dep_edges, report):
    """Attach dependency edges, reporting any endpoint that does not resolve."""
    task_map = {t["task_id"]: t for t in yaml_tasks}
    for upstream, downstream, source in dep_edges:
        missing = [n for n in (upstream, downstream) if n not in task_map]
        if missing:
            # Previously this edge was dropped by an `if` with no else branch, so a
            # task defined in a helper function or a loop lost its edges silently.
            report.drop(
                "dependency", f"{upstream} >> {downstream}", source,
                f"{' and '.join(repr(m) for m in missing)} is not a task defined in this DAG "
                f"(defined in a helper function, imported, or built in a loop)",
                "define the task inline in the DAG body, or add the dependency by hand",
            )
            continue
        deps = task_map[downstream].setdefault("upstream_tasks", [])
        if upstream not in deps:
            deps.append(upstream)


def _build_dag_level(dag_kwargs, report):
    """DAG-level settings, reporting every key that does not survive."""
    dag_def = {}
    # `is not None` rather than truthiness: schedule=None and max_active_runs=0 are
    # meaningful values that a truthiness test silently discarded.
    for key in ("schedule", "description", "max_active_runs"):
        val = dag_kwargs.get(key)
        if isinstance(val, _Unresolved):
            report.drop("dag setting", key, val.expr, val.why,
                        f"set {key} explicitly in the YAML")
        elif val is not None:
            dag_def[key] = val

    default_args = dag_kwargs.get("default_args")
    if isinstance(default_args, _Unresolved):
        report.drop("default_args", "default_args", default_args.expr, default_args.why,
                    "write default_args explicitly in the YAML")
    elif isinstance(default_args, dict) and default_args:
        clean = {}
        for k, v in default_args.items():
            if isinstance(v, _Unresolved):
                report.drop("default_args key", k, v.expr, v.why,
                            f"set default_args.{k} to a literal value")
            elif k not in _DEFAULT_ARGS_KEPT:
                report.drop(
                    "default_args key", k, f"{k}={v!r}",
                    f"MWAA Serverless does not honour '{k}' in default_args",
                    "remove it, or reproduce the behaviour another way "
                    "(depends_on_past and sla have no equivalent; use "
                    "SnsPublishOperator for email-style alerting)",
                )
            elif v is not None:
                clean[k] = v
        if clean:
            dag_def["default_args"] = clean
    return dag_def


def _normalise_tasks(yaml_tasks):
    """List-of-tasks to the mapping shape, flattening `parameters`."""
    normalised = {}
    for t in yaml_tasks:
        tid = t.pop("task_id", "unknown")
        t.pop("_source", None)
        params = t.pop("parameters", None)
        if isinstance(params, dict):
            for k, v in params.items():
                if k not in t:
                    t[k] = v
        if "upstream_tasks" in t:
            t["dependencies"] = t.pop("upstream_tasks")
        normalised[tid] = t
    return normalised


# ══════════════════════════════════════════════════════════════════════════
#  DAG DISCOVERY
# ══════════════════════════════════════════════════════════════════════════

def _collect_imports(tree):
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


def _is_dag_call(node, imports):
    """Whether a Call node constructs an Airflow DAG, honouring import aliases.

    Matching the literal name "DAG" meant `from airflow import DAG as Dag` was not
    recognised, and the file was reported as "not an Airflow DAG".
    """
    if not isinstance(node, ast.Call):
        return False
    name = _get_call_name(node)
    if not name:
        return False
    if name == "DAG":
        return True
    resolved = imports.get(name, "")
    return resolved.split(".")[-1] == "DAG" and "airflow" in resolved


def _find_dag_definitions(tree, imports):
    """Every DAG in the module, each with the AST scope its tasks live in.

    Order is source order. The scope is the `with` block body for a context-manager
    DAG (so one DAG's tasks cannot leak into another's) and the whole module for a
    bare `dag = DAG(...)` assignment, which has no syntactic boundary.
    """
    found = []

    for node in ast.walk(tree):
        # @dag-decorated function
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if _decorator_name(dec) != "dag":
                    continue
                dag_id, kwargs = node.name, {}
                if isinstance(dec, ast.Call):
                    dag_id, kwargs = _dag_call_kwargs(dec, default_id=node.name)
                found.append({"dag_id": dag_id, "kwargs": kwargs, "scope": node,
                              "lineno": node.lineno, "source": f"@dag {node.name}"})

        # with DAG(...) as dag:
        elif isinstance(node, ast.With):
            for item in node.items:
                if _is_dag_call(item.context_expr, imports):
                    dag_id, kwargs = _dag_call_kwargs(item.context_expr)
                    found.append({"dag_id": dag_id, "kwargs": kwargs, "scope": node,
                                  "lineno": node.lineno,
                                  "source": _unparse(item.context_expr)})

        # dag = DAG(...)
        elif isinstance(node, ast.Assign) and _is_dag_call(node.value, imports):
            dag_id, kwargs = _dag_call_kwargs(node.value)
            found.append({"dag_id": dag_id, "kwargs": kwargs, "scope": tree,
                          "lineno": node.lineno, "source": _unparse(node.value)})

    found.sort(key=lambda d: d["lineno"])
    # A `with DAG(...)` also contains no nested duplicate; de-duplicate by line.
    seen_lines = set()
    unique = []
    for d in found:
        if d["lineno"] in seen_lines:
            continue
        seen_lines.add(d["lineno"])
        unique.append(d)
    return unique


def _dag_call_kwargs(call_node, default_id=None):
    """(dag_id, kwargs) from a DAG(...) or @dag(...) call."""
    dag_id = default_id
    kwargs = {}
    if call_node.args:
        first = _eval_const(call_node.args[0])
        if isinstance(first, str):
            dag_id = first
    for kw in call_node.keywords:
        val = _eval_const(kw.value)
        if kw.arg == "dag_id":
            if isinstance(val, str):
                dag_id = val
        elif kw.arg in ("schedule", "schedule_interval"):
            kwargs["schedule"] = val
        elif kw.arg == "description":
            kwargs["description"] = val
        elif kw.arg == "max_active_runs":
            kwargs["max_active_runs"] = val
        elif kw.arg == "default_args":
            kwargs["default_args"] = val
    return dag_id, kwargs


def _decorator_name(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Call):
        return _decorator_name(node.func)
    return None


def _resolve_short_name(local_name, fqn):
    """Resolve to a short operator name from the allowlist."""
    if local_name in _SHORT_NAMES:
        return local_name
    if fqn in _FQN_TO_SHORT:
        return _FQN_TO_SHORT[fqn]
    class_name = fqn.rsplit(".", 1)[-1] if "." in fqn else fqn
    if class_name in _SHORT_NAMES:
        return class_name
    return None


# ══════════════════════════════════════════════════════════════════════════
#  TASK AND DEPENDENCY EXTRACTION
# ══════════════════════════════════════════════════════════════════════════

def _extract_tasks(scope, imports, report):
    """Extract task instantiations and dependency expressions within one DAG scope."""
    tasks = []
    task_var_map = {}
    dep_edges = []

    for node in ast.walk(scope):
        if isinstance(node, ast.Assign):
            _extract_from_assign(node, node.targets, imports, tasks, task_var_map, report)
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            # x: EmptyOperator = EmptyOperator(task_id="x")
            _extract_from_assign(node, [node.target], imports, tasks, task_var_map, report)
        elif isinstance(node, ast.Expr):
            if isinstance(node.value, ast.Call):
                task = _parse_operator_call(node.value, imports, report)
                if task and task["task_id"]:
                    tasks.append(task)
                else:
                    _extract_set_relation(node.value, task_var_map, dep_edges)
            elif isinstance(node.value, ast.BinOp):
                _extract_deps(node.value, task_var_map, dep_edges)

    return tasks, dep_edges


def _extract_from_assign(node, targets, imports, tasks, task_var_map, report):
    """Handle `a = Op(...)` and `a, b = Op(...), Op(...)`.

    The tuple form used to be invisible: only a single ast.Name target with a direct
    ast.Call value was recognised, so `a, b = Op(), Op()` produced `tasks: {}` and the
    result was still reported as valid and deployable.
    """
    if len(targets) == 1 and isinstance(targets[0], ast.Tuple) and isinstance(node.value, ast.Tuple):
        pairs = list(zip(targets[0].elts, node.value.elts, strict=False))
    elif len(targets) == 1:
        pairs = [(targets[0], node.value)]
    else:
        # a = b = Op(...)
        pairs = [(t, node.value) for t in targets]

    for target, value in pairs:
        if not isinstance(value, ast.Call):
            continue
        task = _parse_operator_call(value, imports, report)
        if not task:
            continue
        var_name = _get_assign_target(target)
        if var_name and task["task_id"]:
            task_var_map[var_name] = task["task_id"]
        tasks.append(task)


def _parse_operator_call(call_node, imports, report):
    """Parse an operator instantiation call."""
    name = _get_call_name(call_node)
    if not name:
        return None

    resolved = imports.get(name, name)
    class_name = resolved.rsplit(".", 1)[-1] if "." in resolved else name

    if not (class_name.endswith(("Operator", "Sensor"))
            or class_name in _SHORT_NAMES or class_name in _REPLACEABLE_WITH_EMPTY):
        return None

    source = _unparse(call_node)
    task_id = None
    kwargs = {}
    callable_name = None
    pending = []

    for kw in call_node.keywords:
        if kw.arg is None:
            # **extra_kwargs — the keys are not knowable without executing the source.
            pending.append(("**kwargs", _unparse(kw.value),
                            "**kwargs expansion cannot be resolved without running the code"))
            continue
        val = _eval_const(kw.value)
        if kw.arg == "task_id":
            task_id = val if isinstance(val, str) else None
            if task_id is None:
                pending.append((kw.arg, _unparse(kw.value),
                                "task_id is not a literal string"))
            continue
        if kw.arg == "python_callable":
            # Keep the real function name; the placeholder used to fall back to the
            # task_id even though the AST had the name right there.
            if isinstance(kw.value, ast.Name):
                callable_name = kw.value.id
            elif isinstance(kw.value, ast.Attribute):
                callable_name = kw.value.attr
        if isinstance(val, _Unresolved):
            pending.append((kw.arg, val.expr, val.why))
            continue
        kwargs[kw.arg] = val

    if task_id is None and call_node.args:
        first = _eval_const(call_node.args[0])
        if isinstance(first, str):
            task_id = first

    resolved_task_id = task_id or "unknown_task"
    for arg, expr, why in pending:
        report.drop("operator argument", f"{resolved_task_id}.{arg}", expr, why,
                    f"set {arg} to a literal value in the YAML, or pass it as a DAG param "
                    f"and reference it with {{{{ params.x }}}}")

    return {
        "task_id": resolved_task_id,
        "operator": name,
        "source": source,
        "callable_name": callable_name,
        "kwargs": kwargs,
        "retries": kwargs.pop("retries", None),
        "retry_delay": kwargs.pop("retry_delay", None),
        "execution_timeout": kwargs.pop("execution_timeout", None),
        "trigger_rule": kwargs.pop("trigger_rule", None),
    }


def _flatten_chain(node):
    """Split a >>/<< chain into its leftmost operand and the (op, operand) sequence.

    Iterative: the previous recursive form raised an uncaught RecursionError on a
    chain of ~1000 tasks, and it only looked at ast.RShift, so `<<` was ignored
    outright — `b << a` produced two tasks and no dependency at all.
    """
    spine = []
    cur = node
    while isinstance(cur, ast.BinOp) and isinstance(cur.op, (ast.RShift, ast.LShift)):
        spine.append((cur.op, cur.right))
        cur = cur.left
    spine.reverse()
    return cur, spine


def _extract_deps(node, task_var_map, dep_edges):
    """Turn a >>/<< chain into (upstream, downstream, source) edges.

    Chains are left-associative, so `a >> b >> c` is `(a >> b) >> c` and the value of
    each step is its right operand. `a >> b << c` therefore means a->b and c->b.
    """
    if not isinstance(node, ast.BinOp) or not isinstance(node.op, (ast.RShift, ast.LShift)):
        return
    source = _unparse(node)
    leftmost, spine = _flatten_chain(node)
    if not spine:
        return

    prev = _resolve_task_refs(leftmost, task_var_map)
    for op, right in spine:
        current = _resolve_task_refs(right, task_var_map)
        for p in prev:
            for c in current:
                if isinstance(op, ast.RShift):
                    dep_edges.append((p, c, source))
                else:
                    dep_edges.append((c, p, source))
        prev = current or prev


def _extract_set_relation(call_node, task_var_map, dep_edges):
    """Handle a.set_downstream(b) / a.set_upstream(b), which were unhandled entirely."""
    func = call_node.func
    if not isinstance(func, ast.Attribute) or func.attr not in ("set_downstream", "set_upstream"):
        return
    owners = _resolve_task_refs(func.value, task_var_map)
    if not owners or not call_node.args:
        return
    source = _unparse(call_node)
    for arg in call_node.args:
        for other in _resolve_task_refs(arg, task_var_map):
            for owner in owners:
                if func.attr == "set_downstream":
                    dep_edges.append((owner, other, source))
                else:
                    dep_edges.append((other, owner, source))


def _resolve_task_refs(node, task_var_map):
    """Resolve a node to task ids, iteratively for chains and lists."""
    if isinstance(node, ast.Name):
        # An unmapped name is returned as-is so _wire_dependencies can report it by
        # name rather than dropping the edge without a trace.
        return [task_var_map.get(node.id, node.id)]
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        out = []
        for elt in node.elts:
            out.extend(_resolve_task_refs(elt, task_var_map))
        return out
    if isinstance(node, ast.Call):
        for kw in node.keywords:
            if kw.arg == "task_id":
                val = _eval_const(kw.value)
                if isinstance(val, str):
                    return [val]
        return []
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
        _, spine = _flatten_chain(node)
        return _resolve_task_refs(spine[-1][1], task_var_map) if spine else []
    return []


def _get_call_name(node):
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _get_assign_target(target):
    return target.id if isinstance(target, ast.Name) else None


# ══════════════════════════════════════════════════════════════════════════
#  VALUE DECODING (parse only — nothing is executed)
# ══════════════════════════════════════════════════════════════════════════

def _eval_const(node):
    """Decode a literal AST node, or return _Unresolved describing why not.

    Containers propagate unresolvedness rather than substituting None for the
    elements they could not read: `tags=[BUCKET, "other"]` used to become
    `[null, "other"]`, emitting a value the user never wrote.
    """
    if node is None:
        return None
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        out = []
        for elt in node.elts:
            val = _eval_const(elt)
            if isinstance(val, _Unresolved):
                return _Unresolved(node, f"list element {val.expr!r} is not a literal ({val.why})")
            out.append(val)
        return out
    if isinstance(node, ast.Dict):
        return _eval_dict(node)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        val = _eval_const(node.operand)
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            return -val
        return _Unresolved(node, "unary minus on a non-numeric value")
    if isinstance(node, ast.Call):
        return _eval_call(node)
    if isinstance(node, ast.JoinedStr):
        # An f-string used to be replaced with the literal text
        # "<f-string: manual conversion needed>", which passes validation and then
        # fails at run time.
        return _Unresolved(node, "f-string: interpolation happens at run time, so the "
                                 "value is not knowable from the source")
    if isinstance(node, ast.Name):
        return _Unresolved(node, "reference to a variable defined elsewhere")
    if isinstance(node, ast.Attribute):
        return _Unresolved(node, "attribute lookup on a Python object")
    if isinstance(node, (ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp)):
        return _Unresolved(node, "comprehension: evaluated at run time")
    if isinstance(node, ast.BinOp):
        return _Unresolved(node, "computed expression")
    if isinstance(node, ast.BoolOp):
        return _Unresolved(node, "boolean expression")
    if isinstance(node, ast.IfExp):
        return _Unresolved(node, "conditional expression")
    return _Unresolved(node, f"unsupported expression ({type(node).__name__})")


def _eval_call(node):
    """timedelta(...) becomes integer seconds; every other call is unresolvable."""
    fname = _get_call_name(node)
    if fname != "timedelta":
        return _Unresolved(node, "function call: the return value is only known at run time")

    total = 0
    matched = False
    for kw in node.keywords:
        if kw.arg in _TIMEDELTA_MULTIPLIERS:
            v = _eval_const(kw.value)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                total += v * _TIMEDELTA_MULTIPLIERS[kw.arg]
                matched = True
    order = ["days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks"]
    for i, arg in enumerate(node.args):
        if i < len(order) and order[i] in _TIMEDELTA_MULTIPLIERS:
            v = _eval_const(arg)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                total += v * _TIMEDELTA_MULTIPLIERS[order[i]]
                matched = True
    if matched:
        return int(total)
    return _Unresolved(node, "timedelta() arguments are not literals")


def _eval_dict(node):
    if not isinstance(node, ast.Dict):
        return _Unresolved(node, "not a dict literal")
    result = {}
    for key_node, value_node in zip(node.keys, node.values, strict=True):
        if key_node is None:
            # {**other} — the keys come from a value only known at run time.
            return _Unresolved(node, "dict uses ** expansion, so its keys are not knowable")
        key = _eval_const(key_node)
        if isinstance(key, _Unresolved):
            return _Unresolved(node, f"dict key {key.expr!r} is not a literal")
        value = _eval_const(value_node)
        if isinstance(value, _Unresolved):
            # Report at the dict level; substituting None here is what produced
            # {"k": null} for {"k": BUCKET}.
            return _Unresolved(node, f"value for key {key!r} is not a literal ({value.why})")
        result[key] = value
    return result
