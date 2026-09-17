# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Run this MCP server locally over stdio, with no network exposure.

The Lambda + HTTP API deployment puts a public endpoint on the internet. Running
locally instead means access control is your OS user and your own AWS credentials:
there is no URL to leak and no authorizer to configure. This is the recommended
way to use the server for individual development.

Tools are not redefined here. They are read from the same registry `app.py`
populates, so the Lambda and local paths always expose identical tools with
identical descriptions.

    pip install -r requirements-local.txt
    python local_server.py

Then point your MCP client at it as a stdio server — see README.md.
"""

import functools
import inspect
import json
import logging
import os
import re
import sys

# Tools are registered as an import side effect of app.py.
import app  # noqa: F401  (imported for its registration side effects)

try:
    # mcp 2.x renamed FastMCP to MCPServer; the surface used here (add_tool, run)
    # is identical, so support whichever version happens to be installed.
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:
    try:
        from mcp.server.fastmcp import FastMCP as _Server
    except ImportError as exc:  # pragma: no cover
        sys.stderr.write(
            "The 'mcp' package is required for local stdio mode.\n"
            "Install it with:  pip install -r requirements-local.txt\n"
        )
        raise SystemExit(1) from exc

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    # stdout carries the JSON-RPC stream, so logs must go to stderr or the
    # client's parser will choke on them.
    stream=sys.stderr,
    format="%(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("mwaa-serverless-mcp.local")


# Hints for failures that are environmental rather than a mistake in the request.
_ERROR_HINTS = (
    ("EndpointConnectionError",
     "Could not reach the MWAA Serverless endpoint. This is usually transient network "
     "or DNS trouble, or a region with no MWAA Serverless endpoint. Check AWS_REGION and retry."),
    ("ExpiredToken",
     "AWS credentials have expired. Refresh them and retry."),
    ("UnrecognizedClientException",
     "AWS credentials were rejected. Check which profile/role the server is using."),
    ("AccessDenied",
     "The caller is missing an airflow-serverless:* permission for this call."),
    ("NoRegionError",
     "No AWS region is configured. Set AWS_REGION or AWS_DEFAULT_REGION."),
    ("UnknownServiceError",
     "This boto3 is too old to know the mwaa-serverless client. Upgrade to boto3 >= 1.40."),
)


def _wrap_tool_errors(fn, name):
    """Return the tool's JSON error envelope instead of raising.

    Every tool in app.py returns a JSON string, and on Lambda a failure comes back
    as JSON the agent can read. Locally, an uncaught exception surfaces as an opaque
    `UnexpectedToolError: Error executing tool <name>` with the real cause buried in
    stderr, which an agent cannot act on. This keeps both transports consistent.

    functools.wraps is used deliberately: `inspect.signature` follows `__wrapped__`,
    so the schema the client sees is still derived from the real annotated signature.
    """
    def _envelope(exc):
        detail = f"{type(exc).__name__}: {exc}"
        hint = next((h for key, h in _ERROR_HINTS if key in detail), None)
        out = {"error": detail, "tool": name}
        if hint:
            out["hint"] = hint
        return json.dumps(out, indent=2, default=str)

    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def awrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:  # noqa: BLE001 — reported to the caller as JSON
                log.exception("Tool %s failed", name)
                return _envelope(e)
        return awrapper

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 — reported to the caller as JSON
            log.exception("Tool %s failed", name)
            return _envelope(e)
    return wrapper


def _tool_registry():
    """The (spec, implementation) registries app.py's handler populates.

    These are INTERNAL attributes of awslabs.mcp_lambda_handler, not a documented API.
    Reading them is what keeps the Lambda and stdio transports exposing identical
    tools from one definition — but it means a minor version bump can remove them.
    Fail here with an actionable message rather than registering zero tools and
    presenting an empty, apparently-working server.
    """
    missing = [attr for attr in ("tools", "tool_implementations")
               if not hasattr(app.mcp_server, attr)]
    if missing:
        raise RuntimeError(
            f"This version of awslabs.mcp_lambda_handler no longer exposes "
            f"{', '.join(missing)} on MCPLambdaHandler, which local stdio mode reads to "
            f"mirror the Lambda transport's tools. Pin the version in "
            f"src/requirements.txt (see the ~=0.1.15 constraint there), or register the "
            f"tools on the stdio server explicitly."
        )
    return app.mcp_server.tools, app.mcp_server.tool_implementations


def build_server():
    """Register every tool from app.py's registry onto a local stdio server.

    The functions in app.py already have real type-annotated signatures and return
    JSON strings, so they are registered directly — only wrapped so a failure comes
    back as JSON rather than an opaque protocol error. The schema the client sees is
    still derived from the same signature the Lambda path uses.
    """
    server = _Server("mwaa-serverless-mcp")
    specs, implementations = _tool_registry()
    registered, skipped = 0, []
    for name, spec in specs.items():
        fn = implementations.get(name)
        if fn is None:
            skipped.append(name)
            continue
        description = spec.get("description") or ""
        if not description:
            doc = inspect.getdoc(fn) or ""
            description = re.split(r"\n\s*Args:\s*\n", doc, maxsplit=1)[0].strip()
        server.add_tool(_wrap_tool_errors(fn, name), name=name, description=description)
        registered += 1
    if skipped:
        log.warning("Skipped tools with no implementation: %s", ", ".join(skipped))
    if not registered:
        raise RuntimeError(
            "No tools were registered. app.py's tool registry was readable but empty, "
            "which means the @mcp_server.tool() decorators did not run."
        )
    log.info("Registered %d tools for local stdio transport", registered)
    return server


def main() -> None:
    # Fail early with a clear message rather than on the first tool call.
    try:
        import boto3
        sts = boto3.client("sts")
        ident = sts.get_caller_identity()
        log.info("AWS identity: %s (account %s, region %s)",
                 ident.get("Arn", "?"), ident.get("Account", "?"),
                 boto3.Session().region_name or "unset")
        if not boto3.Session().region_name:
            log.warning("No AWS region configured. Set AWS_REGION or AWS_DEFAULT_REGION "
                        "to the region your MWAA Serverless workflows live in.")
    except Exception as e:
        log.warning("Could not resolve AWS credentials (%s). Authoring and validation "
                    "tools will still work; anything that calls AWS will fail.", e)

    build_server().run(transport="stdio")


if __name__ == "__main__":
    main()
