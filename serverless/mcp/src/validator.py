"""
Validation and auto-repair for MWAA Serverless DAG YAML.

Two entry points:

  validate(yaml_content)  -> findings, split into errors / warnings / hints
  repair(yaml_content)     -> corrected YAML plus a list of what changed

The rules encoded here were confirmed against the live mwaa-serverless
CreateWorkflow API; see constraints.YAML_SCHEMA for the evidence behind each one.
Anything reported as an `error` will either be rejected by CreateWorkflow or
will deploy and then fail at run time.
"""

import copy
import re

import yaml

from constraints import (
    ACCEPTED_DAG_KEYS,
    AWS_BASE_OPERATOR_ATTRS,
    DAG_FACTORY_RESERVED_KEYS,
    DEFAULT_ARGS_ALLOWLIST,
    IGNORED_DAG_PARAMS,
    IGNORED_TASK_PARAMS,
    QUOTAS,
    SILENTLY_IGNORED_DAG_KEYS,
    SUPPORTED_JINJA_VARIABLES,
    SUPPORTED_MACROS,
    UNSUPPORTED_JINJA_REPLACEMENTS,
)
from schema import (
    ABSTRACT_OPERATORS,
    CODE_OPERATORS,
    LONG_WAIT_OPERATOR_PAIRS,
    OPERATOR_REQUIRED_PARAMS,
    RESCHEDULE_MODE_UNSUPPORTED,
    SENSOR_MODES,
    SUPPORTED_OPERATORS,
    is_sensor,
    resolve_operator_fqn,
)

_TASK_ID_RE = re.compile(r"^[a-zA-Z0-9_.-]+$")
_DAG_ID_RE = re.compile(r"^[a-zA-Z0-9_.-]+$")
_JINJA_VAR_RE = re.compile(r"\{\{[-\s]*([a-zA-Z_][a-zA-Z0-9_.]*)")
_XCOM_PULL_RE = re.compile(r"xcom_pull\s*\(([^)]*)\)")
_XCOM_TASKIDS_RE = re.compile(r"task_ids\s*=\s*[\"']([^\"']+)[\"']")
_DURATION_RE = re.compile(r"^(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$", re.I)

_DURATION_UNITS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}

_TIMEDELTA_FIELDS = {"weeks", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds"}

# Operators that push nothing useful to XCom. Verified against the live service:
# a downstream task rendering {{ ti.xcom_pull(task_ids='<cfn task>') }} received
# the literal string "None", so indexing it raises TypeError at run time.
_NO_XCOM_OPERATORS = {
    "CloudFormationCreateStackOperator":
        "it returns None — CloudFormation stack Outputs are NOT exposed through XCom",
    "CloudFormationDeleteStackOperator": "it returns None",
    "CloudFormationCreateStackSensor": "sensors of this type return None",
    "CloudFormationDeleteStackSensor": "sensors of this type return None",
    "EmptyOperator": "it does nothing and returns None",
}

# Task keys that dag-factory consumes itself instead of forwarding to the operator.
_STRUCTURAL_TASK_KEYS = {
    "operator", "dependencies", "task_id", "retries", "retry_delay",
    "execution_timeout", "trigger_rule",
}

# Alternative dependency spellings that people (and LLMs) reach for. All of them
# are forwarded to the operator constructor by dag-factory and blow up.
_DEP_ALIASES = ("upstream_tasks", "depends_on", "upstream", "depends", "needs")
_REVERSE_DEP_ALIASES = ("downstream_tasks", "downstream")


def _duration_to_seconds(value):
    """Parse an int or a human duration string into seconds, or None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        m = _DURATION_RE.match(value.strip())
        if m:
            return int(m.group(1)) * _DURATION_UNITS[m.group(2).lower()]
    return None


def _timedelta_mapping(seconds):
    """Build the timedelta mapping shape the service requires."""
    if seconds % 60 == 0 and seconds >= 60:
        return {"__type__": "datetime.timedelta", "minutes": seconds // 60}
    return {"__type__": "datetime.timedelta", "seconds": seconds}


def _timedelta_seconds(value):
    """Total seconds for a `__type__: datetime.timedelta` mapping, else None."""
    if not isinstance(value, dict):
        return None
    fields = {k: v for k, v in value.items() if k in _TIMEDELTA_FIELDS}
    if not fields:
        return None
    mult = {"weeks": 604800, "days": 86400, "hours": 3600, "minutes": 60,
            "seconds": 1, "milliseconds": 0.001, "microseconds": 0.000001}
    total = 0
    for k, v in fields.items():
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            return None
        total += v * mult[k]
    return total


def _iter_strings(obj, path=""):
    """Yield (path, string) for every string in a nested structure."""
    if isinstance(obj, str):
        yield path, obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield from _iter_strings(v, f"{path}.{k}" if path else str(k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _iter_strings(v, f"{path}[{i}]")


# ══════════════════════════════════════════════════════════════════════════
#  VALIDATE
# ══════════════════════════════════════════════════════════════════════════

def validate(yaml_content: str) -> dict:
    """Validate DAG YAML against the real MWAA Serverless schema.

    Returns {valid, errors, warnings, hints, summary}. `errors` are things that
    will break; `warnings` are things the service silently ignores; `hints` are
    best-practice observations.
    """
    errors, warnings, hints = [], [], []

    size_kb = len(yaml_content.encode("utf-8")) / 1024
    if size_kb > QUOTAS["max_dag_definition_kb"]:
        errors.append(
            f"Definition is {size_kb:.1f} KB, over the {QUOTAS['max_dag_definition_kb']} KB limit. "
            f"Move inline scripts and templates into S3 objects and reference them."
        )

    try:
        data = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return {"valid": False, "errors": [f"YAML parse error: {e}"],
                "warnings": [], "hints": [], "summary": {}}

    if not isinstance(data, dict):
        return {"valid": False, "errors": ["Root of the definition must be a YAML mapping whose single key is the dag_id."],
                "warnings": [], "hints": [], "summary": {}}

    if not data:
        return {"valid": False, "errors": ["Definition is empty."],
                "warnings": [], "hints": [], "summary": {}}

    if len(data) > 1:
        errors.append(
            f"Found {len(data)} top-level keys ({', '.join(list(data)[:5])}). MWAA Serverless rejects "
            f"this with 'DAG definition should contain a single DAG'. Split into one file per DAG. "
            f"(If you meant these as DAG-level settings, they belong nested under the dag_id key.)"
        )

    summary = {"dag_id": None, "task_count": 0, "operators": [], "needs_code_bundle": False}

    for dag_id, dag_cfg in data.items():
        summary["dag_id"] = summary["dag_id"] or dag_id
        if not _DAG_ID_RE.match(str(dag_id)):
            errors.append(f"dag_id '{dag_id}' must match ^[a-zA-Z0-9_.-]+$ (no spaces or slashes).")

        if not isinstance(dag_cfg, dict):
            errors.append(f"'{dag_id}' must be a mapping of DAG settings, got {type(dag_cfg).__name__}.")
            continue

        _validate_dag_level(dag_id, dag_cfg, errors, warnings, hints)

        tasks = dag_cfg.get("tasks")
        if tasks is None:
            errors.append(f"DAG '{dag_id}' has no 'tasks' key.")
            continue

        if isinstance(tasks, list):
            errors.append(
                f"DAG '{dag_id}': 'tasks' is a LIST. MWAA Serverless requires a MAPPING keyed by "
                f"task_id and rejects a list with 'Invalid tasks configuration.'  "
                f"Change  tasks:\\n  - task_id: a\\n    operator: X   into   tasks:\\n  a:\\n    operator: X.  "
                f"Call repair_dag_yaml to convert this automatically."
            )
            continue

        if not isinstance(tasks, dict):
            errors.append(f"DAG '{dag_id}': 'tasks' must be a mapping, got {type(tasks).__name__}.")
            continue

        summary["task_count"] = len(tasks)
        _validate_tasks(dag_id, tasks, errors, warnings, hints, summary)

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "hints": hints,
        "summary": summary,
    }


def _validate_dag_level(dag_id, dag_cfg, errors, warnings, hints):
    for key in dag_cfg:
        if key in SILENTLY_IGNORED_DAG_KEYS:
            warnings.append(
                f"DAG '{dag_id}': '{key}' — {SILENTLY_IGNORED_DAG_KEYS[key]}"
            )
            continue
        if key in ACCEPTED_DAG_KEYS:
            continue
        if key == "task_groups":
            errors.append(
                f"DAG '{dag_id}': 'task_groups' is not supported "
                f"(ValidationException: 'Unexpected element'). Flatten the group into plain tasks."
            )
        elif key == "schedule_interval":
            errors.append(f"DAG '{dag_id}': use 'schedule', not the deprecated 'schedule_interval'.")
        elif key in IGNORED_DAG_PARAMS:
            warnings.append(f"DAG '{dag_id}': '{key}' is ignored by MWAA Serverless — remove it.")
        else:
            warnings.append(f"DAG '{dag_id}': unrecognised DAG-level key '{key}'.")

    mar = dag_cfg.get("max_active_runs")
    if mar is not None:
        if not isinstance(mar, int) or isinstance(mar, bool) or mar < 1:
            errors.append(f"DAG '{dag_id}': max_active_runs must be an integer >= 1.")
        elif mar > QUOTAS["max_concurrent_runs_per_workflow"]:
            errors.append(
                f"DAG '{dag_id}': max_active_runs={mar} exceeds the per-workflow limit of "
                f"{QUOTAS['max_concurrent_runs_per_workflow']}."
            )

    mat = dag_cfg.get("max_active_tasks")
    if mat is not None and (not isinstance(mat, int) or isinstance(mat, bool) or mat < 1):
        errors.append(f"DAG '{dag_id}': max_active_tasks must be an integer >= 1.")

    da = dag_cfg.get("default_args")
    if da is not None:
        if not isinstance(da, dict):
            errors.append(f"DAG '{dag_id}': default_args must be a mapping.")
        else:
            for key in da:
                if key not in DEFAULT_ARGS_ALLOWLIST:
                    extra = ""
                    if key == "mode":
                        extra = (" 'mode' has to be set on each sensor task individually — "
                                 "reschedule mode cannot be applied DAG-wide.")
                    errors.append(
                        f"DAG '{dag_id}': default_args key '{key}' is not accepted. MWAA Serverless "
                        f"allows only: {', '.join(sorted(DEFAULT_ARGS_ALLOWLIST))}.{extra}"
                    )
            _validate_retry_delay(f"DAG '{dag_id}' default_args", da.get("retry_delay"), errors)
            _validate_retries(f"DAG '{dag_id}' default_args", da.get("retries"), errors)
            _validate_execution_timeout(f"DAG '{dag_id}' default_args", da.get("execution_timeout"), errors)

    if "schedule" not in dag_cfg:
        hints.append(
            f"DAG '{dag_id}' has no 'schedule'. It will only run on demand. Set a cron expression "
            f"or '@daily' if it should run automatically, or 'schedule: null' to be explicit."
        )

    params = dag_cfg.get("params")
    if params is not None and not isinstance(params, dict):
        errors.append(f"DAG '{dag_id}': params must be a mapping of name -> default value.")


def _validate_retries(where, retries, errors):
    if retries is None:
        return
    if not isinstance(retries, int) or isinstance(retries, bool):
        errors.append(f"{where}: retries must be an integer.")
    elif retries < 0 or retries > QUOTAS["max_retries_per_task"]:
        errors.append(f"{where}: retries must be 0-{QUOTAS['max_retries_per_task']} (got {retries}).")


def _validate_retry_delay(where, rd, errors):
    if rd is None:
        return
    if isinstance(rd, dict):
        secs = _timedelta_seconds(rd)
        if secs is None:
            errors.append(f"{where}: retry_delay mapping is not a valid timedelta.")
            return
    elif isinstance(rd, str):
        errors.append(
            f"{where}: retry_delay must be an INTEGER number of seconds, not the string '{rd}'. "
            f"MWAA Serverless rejects duration strings with 'unsupported type for timedelta "
            f"seconds component: str'."
        )
        return
    elif isinstance(rd, (int, float)) and not isinstance(rd, bool):
        secs = int(rd)
    else:
        errors.append(f"{where}: retry_delay must be an integer number of seconds.")
        return

    if secs < 0 or secs > QUOTAS["max_retry_delay_seconds"]:
        errors.append(
            f"{where}: retry_delay must be 0-{QUOTAS['max_retry_delay_seconds']} seconds (got {secs})."
        )


def _validate_execution_timeout(where, et, errors):
    if et is None:
        return
    if not isinstance(et, dict):
        errors.append(
            f"{where}: execution_timeout must be a mapping "
            f"{{__type__: datetime.timedelta, minutes: N}}, not {type(et).__name__} ({et!r}). "
            f"MWAA Serverless rejects ints and strings with 'execution_timeout must be timedelta object'."
        )
        return
    if et.get("__type__") != "datetime.timedelta":
        errors.append(
            f"{where}: execution_timeout mapping must include '__type__: datetime.timedelta'. "
            f"Without it the service reports 'execution_timeout must be timedelta object but "
            f"passed as type: <class \\'dict\\'>'."
        )
    secs = _timedelta_seconds(et)
    if secs is None:
        errors.append(
            f"{where}: execution_timeout needs at least one of "
            f"{', '.join(sorted(_TIMEDELTA_FIELDS))} with a numeric value."
        )
    else:
        cap = QUOTAS["max_task_execution_timeout_minutes"] * 60
        if secs > cap:
            errors.append(
                f"{where}: execution_timeout is {secs / 60:.0f} minutes, over the "
                f"{QUOTAS['max_task_execution_timeout_minutes']} minute maximum."
            )


def _validate_tasks(dag_id, tasks, errors, warnings, hints, summary):
    task_ids = set(tasks.keys())
    dep_graph = {}

    for tid, tcfg in tasks.items():
        if not _TASK_ID_RE.match(str(tid)):
            errors.append(f"Task id '{tid}' must match ^[a-zA-Z0-9_.-]+$.")

        if not isinstance(tcfg, dict):
            errors.append(f"Task '{tid}' must be a mapping of operator + arguments.")
            dep_graph[tid] = []
            continue

        if "task_id" in tcfg and tcfg["task_id"] != tid:
            warnings.append(
                f"Task '{tid}': the mapping key is the task_id. The inner 'task_id: "
                f"{tcfg['task_id']}' disagrees with it and is redundant — remove it."
            )

        short = _validate_operator(dag_id, tid, tcfg, errors, warnings, summary)
        _validate_flat_params(tid, tcfg, errors)
        deps = _validate_dependencies(tid, tcfg, task_ids, errors)
        dep_graph[tid] = deps

        _validate_retries(f"Task '{tid}'", tcfg.get("retries"), errors)
        _validate_retry_delay(f"Task '{tid}'", tcfg.get("retry_delay"), errors)
        _validate_execution_timeout(f"Task '{tid}'", tcfg.get("execution_timeout"), errors)
        _validate_task_extras(tid, tcfg, warnings, hints)

        if short:
            _validate_required_params(tid, short, tcfg, errors, hints)
            _validate_code_operator(tid, short, tcfg, errors, warnings, summary)
            _validate_sensor_cost(tid, short, tcfg, errors, warnings, hints)
            _validate_blocking_wait_cost(tid, short, tcfg, hints)

    _validate_cycles(dag_id, dep_graph, errors)
    _validate_jinja(tasks, dep_graph, task_ids, errors, warnings)
    _validate_graph_shape(dag_id, tasks, dep_graph, hints)


def _validate_operator(dag_id, tid, tcfg, errors, warnings, summary):
    op = tcfg.get("operator")
    if not op:
        errors.append(f"Task '{tid}' has no 'operator'.")
        return None
    if not isinstance(op, str):
        errors.append(f"Task '{tid}': operator must be a string.")
        return None

    fqn, short, was_short = resolve_operator_fqn(op)

    if fqn is None:
        bare = op.rsplit(".", 1)[-1]
        if bare in SUPPORTED_OPERATORS:
            errors.append(
                f"Task '{tid}': operator '{op}' is not a recognised path. Use "
                f"'{SUPPORTED_OPERATORS[bare]}'."
            )
        else:
            close = [k for k in SUPPORTED_OPERATORS if bare.lower()[:8] in k.lower()][:4]
            suffix = f" Closest allowlisted names: {', '.join(close)}." if close else ""
            errors.append(
                f"Task '{tid}': operator '{op}' is not in the MWAA Serverless allowlist. "
                f"Only Amazon provider operators plus PythonOperator, BashOperator and "
                f"EmptyOperator are available.{suffix}"
            )
        return None

    if was_short:
        errors.append(
            f"Task '{tid}': operator '{op}' is a short name. MWAA Serverless requires the fully "
            f"qualified path and rejects short names with \"operator '{op}' is not supported\". "
            f"Use '{fqn}'."
        )

    if short in ABSTRACT_OPERATORS:
        errors.append(
            f"Task '{tid}': '{short}' is an abstract base class, not a usable operator. "
            f"Pick a concrete operator from the same module."
        )
        return None

    summary["operators"].append(short)
    return short


def _validate_flat_params(tid, tcfg, errors):
    if "parameters" in tcfg:
        inner = tcfg["parameters"]
        names = ", ".join(list(inner)[:6]) if isinstance(inner, dict) else "..."
        errors.append(
            f"Task '{tid}': operator arguments are nested under 'parameters'. MWAA Serverless has "
            f"no 'parameters' wrapper — dag-factory forwards it to the operator as an unknown "
            f"kwarg and the required arguments come out missing "
            f"(\"missing keyword arguments\"). Move {names} up to the task level and delete "
            f"'parameters'. Call repair_dag_yaml to do this automatically."
        )
    for reserved in DAG_FACTORY_RESERVED_KEYS:
        if reserved in tcfg and reserved != "__type__":
            errors.append(
                f"Task '{tid}': '{reserved}' is reserved by dag-factory and must not be used as "
                f"an operator argument."
            )


def _validate_dependencies(tid, tcfg, task_ids, errors):
    deps = []
    for alias in _DEP_ALIASES:
        if alias in tcfg:
            errors.append(
                f"Task '{tid}': '{alias}' is not a recognised key. Use 'dependencies'. "
                f"dag-factory passes unknown keys to the operator, which fails with "
                f"\"Invalid arguments were passed ... {{'{alias}': ...}}\"."
            )
    for alias in _REVERSE_DEP_ALIASES:
        if alias in tcfg:
            errors.append(
                f"Task '{tid}': '{alias}' is not supported. Dependencies are declared on the "
                f"DOWNSTREAM task with 'dependencies: [{tid}]'."
            )

    raw = tcfg.get("dependencies")
    if raw is None:
        return deps
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        errors.append(f"Task '{tid}': dependencies must be a list of upstream task ids.")
        return deps

    for dep in raw:
        if not isinstance(dep, str):
            errors.append(f"Task '{tid}': dependency entries must be task id strings.")
            continue
        if dep == tid:
            errors.append(f"Task '{tid}' depends on itself.")
            continue
        if dep not in task_ids:
            near = [t for t in task_ids if dep.lower() in t.lower() or t.lower() in dep.lower()][:3]
            suffix = f" Did you mean: {', '.join(near)}?" if near else ""
            errors.append(f"Task '{tid}': dependency '{dep}' is not a task in this DAG.{suffix}")
            continue
        deps.append(dep)
    return deps


def _validate_task_extras(tid, tcfg, warnings, hints):
    for attr, note in AWS_BASE_OPERATOR_ATTRS.items():
        if attr in tcfg:
            warnings.append(f"Task '{tid}': '{attr}' — {note} Remove it to keep the definition clean.")

    for key in tcfg:
        if key in IGNORED_TASK_PARAMS:
            warnings.append(f"Task '{tid}': '{key}' is ignored by MWAA Serverless — remove it.")

    if tcfg.get("deferrable") is True:
        warnings.append(
            f"Task '{tid}': deferrable=True has no effect — MWAA Serverless has no triggerer "
            f"(CreateWorkflow returns Warnings: ['ignored attributes: deferrable']). The task runs "
            f"in normal blocking mode, so remove it. mode: reschedule is not a substitute — it is "
            f"not supported end to end either. Bound the wait with a timeout instead."
        )

    if "expand" in tcfg or "expand_kwargs" in tcfg:
        warnings.append(f"Task '{tid}': dynamic task mapping is not supported and will not expand.")


def _validate_sensor_cost(tid, short, tcfg, errors, warnings, hints):
    """Check the sensor arguments that determine how long a wait holds a worker.

    MWAA Serverless bills for worker occupancy, and neither Airflow mechanism for
    releasing the worker applies here: `deferrable` is ignored and `mode: reschedule` is
    not supported end to end. So the checks below steer toward a bounded poke-mode wait.
    """
    mode = tcfg.get("mode")

    if mode is not None:
        if is_sensor(short):
            if not isinstance(mode, str) or mode not in SENSOR_MODES:
                errors.append(
                    f"Task '{tid}': mode={mode!r} is invalid. The service rejects this with "
                    f"\"The mode must be one of {list(SENSOR_MODES)}\"."
                )
            elif mode == "reschedule":
                errors.append(f"Task '{tid}': {RESCHEDULE_MODE_UNSUPPORTED}")
        elif isinstance(mode, str) and mode in SENSOR_MODES:
            # Only flag a sensor-scheduling value. Some operators (e.g.
            # ComprehendStartPiiEntitiesDetectionJobOperator) have their own unrelated
            # `mode` argument, which must be left alone.
            warnings.append(
                f"Task '{tid}': mode='{mode}' is a sensor scheduling mode and has no effect on "
                f"{short}, which is not a sensor. Remove it."
            )

    if not is_sensor(short):
        return

    if tcfg.get("timeout") is None:
        hints.append(
            f"COST: sensor '{tid}' has no timeout, so it defaults to Airflow's 7 days. A sensor "
            f"holds a worker slot for its entire wait on MWAA Serverless, so an unbounded wait is "
            f"an unbounded bill. Set timeout to the longest wait that is still useful."
        )

    pi = tcfg.get("poke_interval")
    if pi is not None and (not isinstance(pi, (int, float)) or isinstance(pi, bool) or pi <= 0):
        errors.append(f"Task '{tid}': poke_interval must be a positive number of seconds.")

    if tcfg.get("exponential_backoff") and tcfg.get("max_wait") is None:
        hints.append(
            f"Task '{tid}': exponential_backoff is on without max_wait, so the interval grows "
            f"unbounded. Set max_wait to cap it."
        )


def _validate_blocking_wait_cost(tid, short, tcfg, hints):
    """Flag an operator+sensor split that costs more than it saves.

    Splitting a job into fire-and-forget plus a sensor is the usual way to avoid holding
    a worker during a long job, but it only pays off when the waiting task is cheap,
    which needs reschedule mode. Without it the sensor holds a worker exactly as the
    operator would have, and the split just adds a task.
    """
    if tcfg.get("wait_for_completion") is not False:
        return
    sensor = LONG_WAIT_OPERATOR_PAIRS.get(short)
    if not sensor:
        return
    hints.append(
        f"COST: task '{tid}' ({short}) uses wait_for_completion: false, which normally pairs with "
        f"a {sensor}. Be aware that on MWAA Serverless the sensor holds a worker slot for the whole "
        f"wait just as the operator would, because reschedule mode is unavailable, so the split "
        f"costs one extra task start without reducing the billed wait. Prefer "
        f"wait_for_completion: true unless you need the two steps to be separately retryable, or "
        f"need the DAG to do something else in parallel."
    )


def _validate_required_params(tid, short, tcfg, errors, hints):
    required = OPERATOR_REQUIRED_PARAMS.get(short)
    if not required:
        return
    missing = [p for p in required if p not in tcfg or tcfg[p] in (None, "")]
    if missing:
        errors.append(
            f"Task '{tid}' ({short}): missing required argument(s) {', '.join(missing)}. "
            f"CreateWorkflow may accept this because the operator defaults them to None, but the "
            f"task WILL fail at run time. Supply them, or reference them via {{{{ params.x }}}}."
        )


def _validate_code_operator(tid, short, tcfg, errors, warnings, summary):
    if short not in CODE_OPERATORS:
        return
    summary["needs_code_bundle"] = True

    if short == "PythonOperator":
        pc = tcfg.get("python_callable")
        if isinstance(pc, str) and pc:
            if "." not in pc:
                errors.append(
                    f"Task '{tid}': python_callable '{pc}' must be 'module_name.function_name' "
                    f"where module_name is a .py file at the root of your code bundle."
                )
            elif pc.startswith(".") or pc.endswith("."):
                errors.append(f"Task '{tid}': python_callable '{pc}' is malformed.")
        if "op_kwargs" in tcfg and not isinstance(tcfg["op_kwargs"], dict):
            errors.append(f"Task '{tid}': op_kwargs must be a mapping.")

    if short == "BashOperator":
        cmd = tcfg.get("bash_command")
        if isinstance(cmd, str) and cmd.strip().endswith(".sh") and not cmd.strip().startswith(("./", "/", "bash ", "sh ")):
            warnings.append(
                f"Task '{tid}': bash_command '{cmd}' looks like a script path. Scripts run with "
                f"/usr/local/airflow/dags as the working directory — use './{cmd.strip()}'."
            )


def _validate_cycles(dag_id, dep_graph, errors):
    """Report dependency cycles (Airflow rejects them; the DAG will not load)."""
    WHITE, GREY, BLACK = 0, 1, 2
    colour = {t: WHITE for t in dep_graph}
    reported = set()

    def visit(node, stack):
        colour[node] = GREY
        for dep in dep_graph.get(node, []):
            if dep not in colour:
                continue
            if colour[dep] == GREY:
                cyc = stack[stack.index(dep):] + [dep] if dep in stack else [dep, node]
                key = frozenset(cyc)
                if key not in reported:
                    reported.add(key)
                    errors.append(
                        f"DAG '{dag_id}': dependency cycle {' -> '.join(reversed(cyc))}. "
                        f"Airflow cannot load a cyclic graph."
                    )
            elif colour[dep] == WHITE:
                visit(dep, stack + [dep])
        colour[node] = BLACK

    for t in list(dep_graph):
        if colour[t] == WHITE:
            visit(t, [t])


def _upstream_closure(task, dep_graph, seen=None):
    """All transitive upstream task ids of `task`."""
    if seen is None:
        seen = set()
    for dep in dep_graph.get(task, []):
        if dep not in seen:
            seen.add(dep)
            _upstream_closure(dep, dep_graph, seen)
    return seen


def _validate_jinja(tasks, dep_graph, task_ids, errors, warnings):
    """Check Jinja variables and, critically, that every xcom_pull is reachable."""
    seen_unsupported = set()

    for tid, tcfg in tasks.items():
        if not isinstance(tcfg, dict):
            continue
        upstream = _upstream_closure(tid, dep_graph)

        for path, s in _iter_strings(tcfg):
            if "{{" not in s:
                continue

            for m in _JINJA_VAR_RE.finditer(s):
                var = m.group(1)
                root = var.split(".")[0]
                if root in SUPPORTED_JINJA_VARIABLES or var in SUPPORTED_MACROS:
                    continue
                if root in UNSUPPORTED_JINJA_REPLACEMENTS:
                    key = (tid, root)
                    if key not in seen_unsupported:
                        seen_unsupported.add(key)
                        errors.append(
                            f"Task '{tid}' ({path}): Jinja variable '{{{{ {root} }}}}' is not "
                            f"available in MWAA Serverless. {UNSUPPORTED_JINJA_REPLACEMENTS[root]}"
                        )
                    continue
                key = (tid, root)
                if key not in seen_unsupported:
                    seen_unsupported.add(key)
                    warnings.append(
                        f"Task '{tid}' ({path}): unrecognised Jinja variable '{{{{ {var} }}}}'. "
                        f"Supported: {', '.join(sorted(SUPPORTED_JINJA_VARIABLES))}."
                    )

            # xcom_pull reachability — the top cause of silent None values.
            for call in _XCOM_PULL_RE.finditer(s):
                args = call.group(1)
                ids = _XCOM_TASKIDS_RE.findall(args)
                if not ids and "task_ids" not in args:
                    warnings.append(
                        f"Task '{tid}' ({path}): xcom_pull() without task_ids pulls from the "
                        f"current task. Pass task_ids='<upstream_task_id>'."
                    )
                for ref in ids:
                    if ref not in task_ids:
                        near = [t for t in task_ids if ref.lower() in t.lower()][:3]
                        suffix = f" Did you mean: {', '.join(near)}?" if near else ""
                        errors.append(
                            f"Task '{tid}' ({path}): xcom_pull(task_ids='{ref}') references a task "
                            f"that does not exist in this DAG.{suffix}"
                        )
                        continue
                    if ref not in upstream:
                        errors.append(
                            f"Task '{tid}' ({path}): xcom_pull(task_ids='{ref}') reads from '{ref}', "
                            f"but '{ref}' is not upstream of '{tid}'. The pull will return None "
                            f"because the task may not have run. Add '{ref}' to "
                            f"'{tid}'.dependencies (directly or transitively)."
                        )
                        continue

                    # Pulling from an operator that pushes nothing is a guaranteed
                    # runtime TypeError as soon as the result is indexed.
                    ref_cfg = tasks.get(ref)
                    if isinstance(ref_cfg, dict):
                        _, ref_short, _ = resolve_operator_fqn(ref_cfg.get("operator", "") or "")
                        if ref_short in _NO_XCOM_OPERATORS:
                            indexed = bool(re.search(
                                re.escape(call.group(0)) + r"\s*(\[|\.)", s))
                            detail = _NO_XCOM_OPERATORS[ref_short]
                            msg = (
                                f"Task '{tid}' ({path}): xcom_pull(task_ids='{ref}') pulls from a "
                                f"{ref_short} task, which pushes nothing to XCom ({detail}). "
                            )
                            if indexed:
                                errors.append(
                                    msg + "Indexing that None raises "
                                    "\"TypeError: 'NoneType' object is not subscriptable\" at run "
                                    "time. Pass the value in as a DAG param, or use a "
                                    "PythonOperator that reads it with boto3 and returns it."
                                )
                            else:
                                warnings.append(msg + "The rendered value will be 'None'.")


def _validate_graph_shape(dag_id, tasks, dep_graph, hints):
    """Best-practice observations about the shape of the graph."""
    if len(tasks) < 2:
        return
    has_downstream = {d for deps in dep_graph.values() for d in deps}
    isolated = [t for t in tasks if not dep_graph.get(t) and t not in has_downstream]
    if isolated and len(isolated) != len(tasks):
        hints.append(
            f"DAG '{dag_id}': task(s) {', '.join(isolated)} have no dependencies in either "
            f"direction, so they run in parallel with everything else. If they were meant to be "
            f"part of the chain, add 'dependencies'."
        )


# ══════════════════════════════════════════════════════════════════════════
#  REPAIR
# ══════════════════════════════════════════════════════════════════════════

def repair(yaml_content: str) -> dict:
    """Rewrite common mistakes into the schema MWAA Serverless actually accepts.

    Returns {repaired_yaml, changes, unfixable, validation}. `changes` lists
    every transformation applied. Anything that cannot be fixed mechanically
    (a missing required argument, an unknown operator) is listed in `unfixable`
    and must be resolved by the author.
    """
    changes, unfixable = [], []

    try:
        data = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return {"repaired_yaml": yaml_content, "changes": [],
                "unfixable": [f"YAML parse error, cannot repair: {e}"], "validation": None}

    if not isinstance(data, dict) or not data:
        return {"repaired_yaml": yaml_content, "changes": [],
                "unfixable": ["Root must be a non-empty mapping keyed by dag_id."], "validation": None}

    data = copy.deepcopy(data)

    # A definition that is a bare DAG body (dag_id as a sibling of tasks) rather
    # than nested under the dag_id key.
    if "tasks" in data:
        dag_id = data.pop("dag_id", None) or "workflow"
        data = {dag_id: data}
        changes.append(f"Wrapped the DAG body under its dag_id key ('{dag_id}') — the root key must be the dag_id.")

    if len(data) > 1:
        unfixable.append(
            f"{len(data)} DAGs in one definition ({', '.join(list(data)[:5])}). MWAA Serverless "
            f"allows one DAG per file — split them manually."
        )

    for dag_id, dag_cfg in data.items():
        if not isinstance(dag_cfg, dict):
            continue

        if "schedule_interval" in dag_cfg:
            dag_cfg["schedule"] = dag_cfg.pop("schedule_interval")
            changes.append(f"DAG '{dag_id}': renamed 'schedule_interval' to 'schedule'.")

        for key in list(dag_cfg):
            if key in IGNORED_DAG_PARAMS and key not in ACCEPTED_DAG_KEYS:
                dag_cfg.pop(key)
                changes.append(f"DAG '{dag_id}': removed '{key}' (ignored by MWAA Serverless).")

        da = dag_cfg.get("default_args")
        if isinstance(da, dict):
            # `mode` is rejected in default_args, but the author's intent is clear —
            # push it down onto the sensor tasks rather than silently dropping it.
            pending_mode = da.get("mode") if da.get("mode") in SENSOR_MODES else None
            for key in list(da):
                if key not in DEFAULT_ARGS_ALLOWLIST:
                    da.pop(key)
                    if key == "mode" and pending_mode:
                        changes.append(
                            f"DAG '{dag_id}': moved default_args.mode='{pending_mode}' onto the "
                            f"individual sensor tasks — 'mode' is not accepted in default_args."
                        )
                    else:
                        changes.append(
                            f"DAG '{dag_id}': removed default_args.'{key}' — not in the accepted "
                            f"default_args allowlist (it would fail validation)."
                        )
            _repair_durations(da, f"DAG '{dag_id}' default_args", changes)
            if not da:
                dag_cfg.pop("default_args")
        else:
            pending_mode = None

        tasks = dag_cfg.get("tasks")

        # tasks as a list -> mapping keyed by task_id
        if isinstance(tasks, list):
            converted, dropped = {}, 0
            for i, t in enumerate(tasks):
                if not isinstance(t, dict):
                    dropped += 1
                    continue
                tid = t.pop("task_id", None) or f"task_{i + 1}"
                converted[str(tid)] = t
            dag_cfg["tasks"] = converted
            tasks = converted
            changes.append(
                f"DAG '{dag_id}': converted 'tasks' from a list to a mapping keyed by task_id "
                f"({len(converted)} tasks) — a list is rejected with 'Invalid tasks configuration.'"
            )
            if dropped:
                unfixable.append(f"DAG '{dag_id}': dropped {dropped} task entries that were not mappings.")

        if not isinstance(tasks, dict):
            continue

        # downstream_tasks -> dependencies on the target task (needs a second pass)
        reverse_edges = {}
        for tid, tcfg in tasks.items():
            if not isinstance(tcfg, dict):
                continue
            for alias in _REVERSE_DEP_ALIASES:
                if alias in tcfg:
                    targets = tcfg.pop(alias)
                    if isinstance(targets, str):
                        targets = [targets]
                    if isinstance(targets, list):
                        for tgt in targets:
                            reverse_edges.setdefault(str(tgt), []).append(tid)
                        changes.append(
                            f"Task '{tid}': converted '{alias}' into 'dependencies' entries on "
                            f"{', '.join(str(t) for t in targets)}."
                        )

        for tid, tcfg in tasks.items():
            if not isinstance(tcfg, dict):
                continue
            _repair_task(tid, tcfg, changes, unfixable)
            if pending_mode and is_sensor(tcfg.get("operator")) and "mode" not in tcfg:
                tcfg["mode"] = pending_mode

        for tgt, ups in reverse_edges.items():
            if tgt in tasks and isinstance(tasks[tgt], dict):
                deps = tasks[tgt].setdefault("dependencies", [])
                if isinstance(deps, list):
                    for u in ups:
                        if u not in deps:
                            deps.append(u)
            else:
                unfixable.append(f"downstream target '{tgt}' is not a task in DAG '{dag_id}'.")

    repaired = yaml.dump(data, default_flow_style=False, sort_keys=False, width=4096, allow_unicode=True)
    result = validate(repaired)
    return {
        "repaired_yaml": repaired,
        "changes": changes,
        "unfixable": unfixable + result["errors"],
        "validation": result,
    }


def _repair_task(tid, tcfg, changes, unfixable):
    # Redundant inner task_id
    if "task_id" in tcfg:
        tcfg.pop("task_id")
        changes.append(f"Task '{tid}': removed the redundant inner 'task_id' (the mapping key is the task_id).")

    # `parameters:` wrapper -> flat
    if isinstance(tcfg.get("parameters"), dict):
        inner = tcfg.pop("parameters")
        clashes = [k for k in inner if k in tcfg]
        for k, v in inner.items():
            tcfg.setdefault(k, v)
        changes.append(
            f"Task '{tid}': flattened {len(inner)} argument(s) out of the 'parameters' wrapper onto "
            f"the task — MWAA Serverless has no 'parameters' key."
        )
        if clashes:
            unfixable.append(
                f"Task '{tid}': {', '.join(clashes)} appeared both inside and outside 'parameters'; "
                f"kept the outer value. Confirm which is correct."
            )

    # dependency aliases -> dependencies
    for alias in _DEP_ALIASES:
        if alias in tcfg:
            val = tcfg.pop(alias)
            if isinstance(val, str):
                val = [val]
            if isinstance(val, list):
                deps = tcfg.setdefault("dependencies", [])
                if isinstance(deps, list):
                    for d in val:
                        if d not in deps:
                            deps.append(d)
                changes.append(f"Task '{tid}': renamed '{alias}' to 'dependencies'.")

    # short operator name -> FQN
    op = tcfg.get("operator")
    if isinstance(op, str) and op:
        fqn, short, was_short = resolve_operator_fqn(op)
        if was_short and fqn:
            tcfg["operator"] = fqn
            changes.append(f"Task '{tid}': expanded operator '{op}' to '{fqn}' (short names are rejected).")
        elif fqn is None:
            bare = op.rsplit(".", 1)[-1]
            if bare in SUPPORTED_OPERATORS:
                tcfg["operator"] = SUPPORTED_OPERATORS[bare]
                changes.append(f"Task '{tid}': corrected operator path '{op}' to '{SUPPORTED_OPERATORS[bare]}'.")
            else:
                unfixable.append(f"Task '{tid}': operator '{op}' is not in the allowlist — replace it manually.")

    # drop attributes the service ignores
    for attr in AWS_BASE_OPERATOR_ATTRS:
        if attr in tcfg:
            tcfg.pop(attr)
            changes.append(f"Task '{tid}': removed '{attr}' (ignored by the service; it returns an 'ignored attributes' warning).")

    for key in list(tcfg):
        if key in IGNORED_TASK_PARAMS:
            tcfg.pop(key)
            changes.append(f"Task '{tid}': removed '{key}' (ignored by MWAA Serverless).")

    # deferrable is accepted but silently ignored (no triggerer). On a sensor the
    # author clearly wanted a non-blocking wait, so translate it into the thing
    # that actually achieves that.
    if tcfg.pop("deferrable", None) is not None:
        changes.append(
            f"Task '{tid}': removed 'deferrable' — MWAA Serverless ignores it and returns an "
            f"'ignored attributes' warning. There is no working deferral mechanism to swap in."
        )

    # An invalid sensor mode is a hard rejection; drop it back to the default. Only
    # touch `mode` when it is clearly the sensor scheduling argument — several
    # operators have their own unrelated required `mode` parameter.
    if "mode" in tcfg:
        if is_sensor(tcfg.get("operator")):
            if tcfg["mode"] not in SENSOR_MODES:
                bad = tcfg.pop("mode")
                changes.append(
                    f"Task '{tid}': removed invalid mode={bad!r} (must be one of {list(SENSOR_MODES)})."
                )
            elif tcfg["mode"] == "reschedule":
                tcfg["mode"] = "poke"
                changes.append(
                    f"Task '{tid}': changed mode from 'reschedule' to 'poke' — reschedule mode is "
                    f"accepted at create time but is not supported end to end, so the wait never "
                    f"completes."
                )
        elif tcfg["mode"] in SENSOR_MODES:
            tcfg.pop("mode")
            changes.append(
                f"Task '{tid}': removed the sensor scheduling mode — this task is not a sensor."
            )

    if tcfg.pop("deferrable", None) is not None:
        changes.append(f"Task '{tid}': removed 'deferrable' (no triggerer in MWAA Serverless).")

    _repair_durations(tcfg, f"Task '{tid}'", changes)


def _repair_durations(cfg, where, changes):
    """Coerce retry_delay to int seconds and execution_timeout to a timedelta mapping."""
    rd = cfg.get("retry_delay")
    if rd is not None and not isinstance(rd, dict):
        secs = _duration_to_seconds(rd)
        if secs is None:
            pass
        elif not isinstance(rd, int) or isinstance(rd, bool):
            capped = min(secs, QUOTAS["max_retry_delay_seconds"])
            cfg["retry_delay"] = capped
            note = f" (capped at {capped}s)" if capped != secs else ""
            changes.append(f"{where}: converted retry_delay {rd!r} to {capped} seconds{note}.")
        elif secs > QUOTAS["max_retry_delay_seconds"]:
            cfg["retry_delay"] = QUOTAS["max_retry_delay_seconds"]
            changes.append(
                f"{where}: capped retry_delay from {secs}s to "
                f"{QUOTAS['max_retry_delay_seconds']}s (service maximum)."
            )

    et = cfg.get("execution_timeout")
    if et is None:
        return
    cap = QUOTAS["max_task_execution_timeout_minutes"] * 60
    if isinstance(et, dict):
        if et.get("__type__") != "datetime.timedelta" and _timedelta_seconds(et) is not None:
            et["__type__"] = "datetime.timedelta"
            changes.append(f"{where}: added '__type__: datetime.timedelta' to execution_timeout.")
        secs = _timedelta_seconds(et)
        if secs is not None and secs > cap:
            cfg["execution_timeout"] = _timedelta_mapping(cap)
            changes.append(
                f"{where}: capped execution_timeout from {secs / 60:.0f} to "
                f"{QUOTAS['max_task_execution_timeout_minutes']} minutes (service maximum)."
            )
        return

    secs = _duration_to_seconds(et)
    if secs is None:
        return
    if secs > cap:
        changes.append(
            f"{where}: capped execution_timeout from {secs / 60:.0f} to "
            f"{QUOTAS['max_task_execution_timeout_minutes']} minutes (service maximum)."
        )
        secs = cap
    cfg["execution_timeout"] = _timedelta_mapping(secs)
    changes.append(
        f"{where}: converted execution_timeout {et!r} to the required timedelta mapping "
        f"{cfg['execution_timeout']}."
    )
