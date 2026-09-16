"""
Runtime configuration, resolved from three layers.

Precedence, highest first:

  1. Environment variables  — best for the deployed Lambda, where editing a file
                              means rebuilding and redeploying.
  2. A JSON config file     — best for local use, and the layer a human edits.
  3. Built-in defaults      — sensible values so nothing has to be configured.

The config file is looked up in this order, first hit wins:

  $MWAA_MCP_CONFIG                        explicit path, if set
  ./mcp_config.json                       next to the source (git-ignored)
  ~/.mwaa-serverless-mcp/config.json      per-user, survives a git clean

Copy `mcp_config.example.json` to `mcp_config.json` and edit it. Call the
`get_server_config` tool to see the effective values and where each one came from.
"""

import json
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

# ── Built-in defaults ────────────────────────────────────────────────────
#
# bedrock_model_candidates is a LIST, not a single id, because Bedrock retires
# models. The id this server originally hardcoded
# (anthropic.claude-3-haiku-20240307-v1:0) has since reached end of life and now
# returns ResourceNotFoundException, which disabled failure analysis with no
# visible error. Each candidate is tried in order until one responds.
#
# The "us." prefixed entries are cross-Region inference profile ids. Most current
# models are not offered for direct on-demand invocation, so profiles come first.
DEFAULTS = {
    "bedrock_model_id": None,
    "bedrock_model_candidates": [
        "us.anthropic.claude-haiku-4-5-20251001-v1:0",
        "us.amazon.nova-lite-v1:0",
        "amazon.nova-lite-v1:0",
        "amazon.nova-micro-v1:0",
    ],
    "bedrock_max_tokens": 2000,
    "bedrock_region": None,          # None = same Region as the workflows
    "default_poll_seconds": 90,
    "max_poll_seconds": 110,
    "log_level": "INFO",
}

# Environment variable for each setting.
ENV_KEYS = {
    "bedrock_model_id": "BEDROCK_MODEL_ID",
    "bedrock_model_candidates": "BEDROCK_MODEL_CANDIDATES",   # comma-separated
    "bedrock_max_tokens": "BEDROCK_MAX_TOKENS",
    "bedrock_region": "BEDROCK_REGION",
    "default_poll_seconds": "MWAA_MCP_DEFAULT_POLL_SECONDS",
    "max_poll_seconds": "MWAA_MCP_MAX_POLL_SECONDS",
    "log_level": "LOG_LEVEL",
}

_INT_KEYS = {"bedrock_max_tokens", "default_poll_seconds", "max_poll_seconds"}
_LIST_KEYS = {"bedrock_model_candidates"}

_cache = None


def _candidate_paths():
    explicit = os.environ.get("MWAA_MCP_CONFIG", "").strip()
    if explicit:
        yield Path(explicit).expanduser()
    yield Path(__file__).resolve().parent / "mcp_config.json"
    yield Path.home() / ".mwaa-serverless-mcp" / "config.json"


def _load_file():
    """Return (settings, path, error) from the first config file found."""
    for path in _candidate_paths():
        try:
            if not path.is_file():
                continue
        except OSError:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            return {}, str(path), f"could not be parsed: {e}"
        if not isinstance(data, dict):
            return {}, str(path), "top level must be a JSON object"
        unknown = [k for k in data if k not in DEFAULTS]
        err = f"ignoring unknown key(s): {', '.join(unknown)}" if unknown else None
        return {k: v for k, v in data.items() if k in DEFAULTS}, str(path), err
    return {}, None, None


def _coerce(key, raw):
    if key in _INT_KEYS:
        return int(raw)
    if key in _LIST_KEYS:
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
        return [p.strip() for p in str(raw).split(",") if p.strip()]
    return raw


def load(refresh: bool = False) -> dict:
    """Resolve configuration. Cached; pass refresh=True to re-read."""
    global _cache
    if _cache is not None and not refresh:
        return _cache

    settings = dict(DEFAULTS)
    sources = {k: "default" for k in DEFAULTS}
    problems = []

    file_settings, file_path, file_error = _load_file()
    if file_error:
        problems.append(f"config file {file_path}: {file_error}")
    for key, value in file_settings.items():
        try:
            settings[key] = _coerce(key, value)
            sources[key] = f"file:{file_path}"
        except (TypeError, ValueError) as e:
            problems.append(f"config file {file_path}: '{key}' is invalid ({e}); using default")

    for key, env_name in ENV_KEYS.items():
        raw = os.environ.get(env_name)
        if raw is None or raw.strip() == "":
            continue
        try:
            settings[key] = _coerce(key, raw)
            sources[key] = f"env:{env_name}"
        except (TypeError, ValueError) as e:
            problems.append(f"env {env_name}={raw!r} is invalid ({e}); using previous value")

    # A pinned model must actually be reachable, so keep it as the sole candidate
    # rather than silently falling through to a different model than asked for.
    if settings.get("bedrock_model_id"):
        settings["_effective_model_candidates"] = [settings["bedrock_model_id"]]
        settings["_model_selection"] = "pinned"
    else:
        settings["_effective_model_candidates"] = list(settings["bedrock_model_candidates"])
        settings["_model_selection"] = "fallback chain"

    if settings["max_poll_seconds"] < settings["default_poll_seconds"]:
        problems.append(
            f"default_poll_seconds ({settings['default_poll_seconds']}) exceeds "
            f"max_poll_seconds ({settings['max_poll_seconds']}); it will be capped."
        )

    settings["_sources"] = sources
    settings["_config_file"] = file_path
    settings["_config_file_searched"] = [str(p) for p in _candidate_paths()]
    settings["_problems"] = problems

    for p in problems:
        log.warning("config: %s", p)

    _cache = settings
    return settings


def get(key: str, default=None):
    """One setting."""
    return load().get(key, default if default is not None else DEFAULTS.get(key))


def describe() -> dict:
    """Effective configuration and the provenance of every value.

    Returned by the `get_server_config` tool so a misconfiguration is visible
    rather than something to be inferred from behaviour.
    """
    s = load()
    return {
        "effective": {k: s[k] for k in DEFAULTS},
        "value_source": s["_sources"],
        "model_selection": s["_model_selection"],
        "models_that_will_be_tried_in_order": s["_effective_model_candidates"],
        "config_file_in_use": s["_config_file"] or "none found — using defaults and env vars",
        "config_file_search_order": s["_config_file_searched"],
        "environment_variable_names": ENV_KEYS,
        "problems": s["_problems"] or None,
        "how_to_change_the_model": {
            "locally": "Copy src/mcp_config.example.json to src/mcp_config.json and set "
                       "\"bedrock_model_id\", or export BEDROCK_MODEL_ID. No redeploy needed.",
            "deployed": "Set BEDROCK_MODEL_ID in the Lambda's Environment.Variables "
                        "(template.yaml has a commented-out line) and redeploy, or change it "
                        "in the console for an immediate effect with no rebuild.",
            "precedence": "environment variable > config file > built-in default",
            "note": "Setting bedrock_model_id pins exactly one model and disables the fallback "
                    "chain, so an unreachable id fails loudly instead of quietly using another.",
        },
    }
