"""
Code bundles for PythonOperator and BashOperator tasks.

MWAA Serverless keeps the workflow definition and the code that Python/Bash tasks
run in two separate S3 objects. The definition goes in `DefinitionS3Location`;
the code goes in `Code.S3Location` and is extracted to /usr/local/airflow/dags on
the worker, so every module must sit at the ROOT of the archive.

This module builds that archive, and cross-checks it against the DAG so a
`python_callable: transform.clean` that has no matching `clean` in `transform.py`
is caught before deployment rather than at run time.
"""

import ast
import base64
import io
import re
import zipfile

import yaml

from constraints import CODE_SUPPORT, QUOTAS
from schema import resolve_operator_fqn

_MAX_BUNDLE_BYTES = 250 * 1024 * 1024
_PREINSTALLED = set(CODE_SUPPORT["preinstalled_packages"])

# Imports that will fail or hang at run time because tasks have no internet access
# and only a subset of AWS endpoints is reachable without a VPC.
_NETWORK_MODULES = {
    "requests": "HTTP calls to third-party endpoints are unreachable without a VPC. Stage the data in S3 first.",
    "urllib3": "Outbound HTTP is unreachable without a VPC.",
    "httpx": "Outbound HTTP is unreachable without a VPC.",
    "aiohttp": "Outbound HTTP is unreachable without a VPC.",
    "socket": "Raw network access is unavailable without a VPC.",
    "smtplib": "SMTP is unreachable. Use SnsPublishOperator or Amazon SES via boto3 instead.",
    "psycopg2": "Database endpoints are only reachable with a VPC attached (NetworkConfiguration).",
    "pymysql": "Database endpoints are only reachable with a VPC attached (NetworkConfiguration).",
    "snowflake": "Third-party endpoints are unreachable without a VPC.",
}


def build_code_bundle(files, bundle_name: str = "code.zip") -> dict:
    """Zip a set of Python modules and shell scripts into a deployable bundle.

    `files` maps filename -> file contents, e.g.
        {"transform.py": "def clean(...): ...", "run.sh": "#!/bin/bash\\n..."}

    Filenames must be flat (no directories) because the worker imports modules
    from the archive root. Returns base64 for upload plus per-file analysis.
    """
    if isinstance(files, str):
        return {"error": "files must be a mapping of filename -> contents, not a string."}
    if not isinstance(files, dict) or not files:
        return {"error": "Provide at least one file as {filename: contents}."}

    errors, warnings, modules = [], [], {}

    for name, content in files.items():
        if not isinstance(name, str) or not isinstance(content, str):
            errors.append(f"'{name}': filename and contents must both be strings.")
            continue
        if "/" in name or "\\" in name:
            errors.append(
                f"'{name}': the bundle must be flat. MWAA Serverless extracts the archive to "
                f"/usr/local/airflow/dags and imports modules from the root, so nested "
                f"directories are not importable. Rename it to '{name.split('/')[-1]}'."
            )
            continue
        if not name.endswith((".py", ".sh", ".sql", ".json", ".txt", ".yaml", ".yml", ".csv")):
            warnings.append(f"'{name}': unusual extension for a code bundle.")

        if name.endswith(".py"):
            info = _analyse_python(name, content)
            errors.extend(info["errors"])
            warnings.extend(info["warnings"])
            modules[name[:-3]] = info["functions"]

    if errors:
        return {"error": "Bundle has problems that must be fixed first.",
                "errors": errors, "warnings": warnings}

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    raw = buf.getvalue()

    if len(raw) > _MAX_BUNDLE_BYTES:
        return {"error": f"Bundle is {len(raw) / 1e6:.1f} MB, over the 250 MB limit."}

    callables = sorted(f"{mod}.{fn}" for mod, fns in modules.items() for fn in fns)

    return {
        "bundle_name": bundle_name,
        "zip_base64": base64.b64encode(raw).decode("ascii"),
        "size_bytes": len(raw),
        "files": sorted(files),
        "available_python_callables": callables,
        "warnings": warnings,
        "upload_note": (
            "Pass this to mwaa_deploy_and_run as code_zip_base64 (with code_s3_key), or write it "
            "to disk and upload manually, then reference it in the CreateWorkflow `Code` parameter."
        ),
        "worker_environment": CODE_SUPPORT["worker_environment"],
        "reminder": CODE_SUPPORT["no_internet_by_default"],
    }


def _analyse_python(name: str, source: str) -> dict:
    """Parse a module, list its top-level functions, and flag runtime hazards."""
    errors, warnings, functions = [], [], []
    try:
        tree = ast.parse(source, filename=name)
    except SyntaxError as e:
        return {"errors": [f"'{name}': syntax error on line {e.lineno}: {e.msg}"],
                "warnings": [], "functions": []}

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(node.name)
            if isinstance(node, ast.AsyncFunctionDef):
                warnings.append(
                    f"'{name}.{node.name}' is async. PythonOperator calls it synchronously; "
                    f"it will return a coroutine instead of running."
                )
            has_kwargs = node.args.kwarg is not None
            argnames = {a.arg for a in node.args.args}
            if not has_kwargs and not (argnames & {"context", "ti", "task_instance", "kwargs"}):
                if node.args.args:
                    warnings.append(
                        f"'{name}.{node.name}' takes positional arguments and no **kwargs. "
                        f"PythonOperator passes the Airflow context as keyword arguments — add "
                        f"**context, or supply the values via op_kwargs."
                    )

    for node in ast.walk(tree):
        mod = None
        if isinstance(node, ast.Import):
            for alias in node.names:
                mod = alias.name.split(".")[0]
                if mod in _NETWORK_MODULES:
                    warnings.append(f"'{name}' imports {mod}: {_NETWORK_MODULES[mod]}")
        elif isinstance(node, ast.ImportFrom) and node.module:
            mod = node.module.split(".")[0]
            if mod in _NETWORK_MODULES:
                warnings.append(f"'{name}' imports from {mod}: {_NETWORK_MODULES[mod]}")

    if re.search(r"(aws_secret_access_key|AKIA[0-9A-Z]{16}|aws_access_key_id)", source):
        errors.append(
            f"'{name}' appears to contain hard-coded AWS credentials. Remove them — the workflow "
            f"execution role is resolved automatically by boto3 on the worker."
        )

    return {"errors": errors, "warnings": warnings, "functions": functions}


def check_dag_code_consistency(yaml_content: str, files=None) -> dict:
    """Cross-check Python/Bash tasks in a DAG against the code bundle.

    Catches the failure that dominates first Python/Bash deployments: a
    `python_callable` naming a module or function that is not in the bundle.
    """
    try:
        data = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return {"error": f"YAML parse error: {e}"}
    if not isinstance(data, dict) or not data:
        return {"error": "Definition must be a mapping keyed by dag_id."}

    files = files if isinstance(files, dict) else {}
    available = {}
    for fname, content in files.items():
        if isinstance(fname, str) and fname.endswith(".py") and isinstance(content, str):
            available[fname[:-3]] = set(_analyse_python(fname, content)["functions"])
    script_files = {f for f in files if isinstance(f, str) and f.endswith(".sh")}

    errors, warnings, required_callables, bash_tasks = [], [], [], []

    for dag_id, dag_cfg in data.items():
        tasks = (dag_cfg or {}).get("tasks")
        if not isinstance(tasks, dict):
            continue
        for tid, tcfg in tasks.items():
            if not isinstance(tcfg, dict):
                continue
            _, short, _ = resolve_operator_fqn(tcfg.get("operator", ""))

            if short == "PythonOperator":
                pc = tcfg.get("python_callable")
                if not pc or not isinstance(pc, str):
                    errors.append(f"Task '{tid}': PythonOperator needs python_callable.")
                    continue
                required_callables.append(pc)
                if "." not in pc:
                    errors.append(
                        f"Task '{tid}': python_callable '{pc}' must be 'module.function'."
                    )
                    continue
                mod, fn = pc.rsplit(".", 1)
                if not files:
                    continue
                if mod not in available:
                    errors.append(
                        f"Task '{tid}': python_callable '{pc}' needs module '{mod}.py' at the root "
                        f"of the code bundle. Bundle contains: "
                        f"{', '.join(sorted(m + '.py' for m in available)) or 'no Python modules'}."
                    )
                elif fn not in available[mod]:
                    errors.append(
                        f"Task '{tid}': '{mod}.py' has no top-level function '{fn}'. "
                        f"It defines: {', '.join(sorted(available[mod])) or 'nothing'}."
                    )

            elif short == "BashOperator":
                cmd = tcfg.get("bash_command")
                bash_tasks.append(tid)
                if not cmd or not isinstance(cmd, str):
                    errors.append(f"Task '{tid}': BashOperator needs bash_command.")
                    continue
                m = re.match(r"^\s*(?:bash\s+|sh\s+|\./)?([\w.\-]+\.sh)\b", cmd)
                if m and files and m.group(1) not in script_files:
                    errors.append(
                        f"Task '{tid}': bash_command references '{m.group(1)}' which is not in the "
                        f"code bundle. Bundle scripts: {', '.join(sorted(script_files)) or 'none'}."
                    )

    unused = sorted(
        f"{m}.{f}" for m, fns in available.items() for f in fns
        if f"{m}.{f}" not in required_callables and not f.startswith("_")
    )
    if unused and required_callables:
        warnings.append(f"Bundle exports unused callables: {', '.join(unused[:8])}.")

    needs_bundle = bool(required_callables or bash_tasks)
    if needs_bundle and not files:
        warnings.append(
            "This DAG has Python/Bash tasks, so it needs a code bundle passed via the "
            "CreateWorkflow `Code` parameter. Pass the bundle files to verify the callables resolve."
        )

    return {
        "consistent": not errors,
        "needs_code_bundle": needs_bundle,
        "errors": errors,
        "warnings": warnings,
        "python_callables_referenced": sorted(set(required_callables)),
        "bash_tasks": bash_tasks,
    }


def code_bundle_guidance() -> dict:
    """Everything needed to author and package Python/Bash task code."""
    return {
        "operators": CODE_SUPPORT["operators"],
        "how_code_is_delivered": CODE_SUPPORT["how_code_is_delivered"],
        "accepted_files": CODE_SUPPORT["accepted_code_files"],
        "worker_environment": CODE_SUPPORT["worker_environment"],
        "preinstalled_packages": CODE_SUPPORT["preinstalled_packages"],
        "do_not_bundle": sorted(_PREINSTALLED),
        "packaging_with_dependencies": CODE_SUPPORT["packaging_dependencies"],
        "limits": {**CODE_SUPPORT["limits"], "max_xcom_kb": QUOTAS["max_xcom_kb"]},
        "network": CODE_SUPPORT["no_internet_by_default"],
        "when_to_use": CODE_SUPPORT["when_to_use"],
        "callable_signature_example": (
            "def summarise(**context):\n"
            "    ti = context['ti']\n"
            "    keys = ti.xcom_pull(task_ids='list_input')\n"
            "    return {'count': len(keys)}   # returned value is pushed to XCom\n"
        ),
        "yaml_example": (
            "tasks:\n"
            "  summarise:\n"
            "    operator: airflow.providers.standard.operators.python.PythonOperator\n"
            "    python_callable: transform.summarise\n"
            "    dependencies: [list_input]\n"
            "  say_hello:\n"
            "    operator: airflow.providers.standard.operators.bash.BashOperator\n"
            "    bash_command: \"echo processed {{ ti.xcom_pull(task_ids='summarise')['count'] }}\"\n"
            "    dependencies: [summarise]\n"
        ),
    }
