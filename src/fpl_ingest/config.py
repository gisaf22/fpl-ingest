"""Runtime configuration for the ingest CLI.

Reads configuration from explicit CLI overrides first, then environment
variables, then a simple ``~/.fpl/config.yaml`` key-value file, and finally
falls back to hard-coded defaults.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

DEFAULT_STALE_AFTER_HOURS: float = 26.0

_DEFAULT_RAW_DIR = Path.home() / ".fpl" / "raw"
_CONFIG_FILE = Path.home() / ".fpl" / "config.yaml"

_KV_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*):\s*(.+)$")


@dataclass(frozen=True)
class IngestConfig:
    """Resolved runtime settings for a single ingest invocation."""

    raw_dir: Path            # Directory for raw JSON cache files from the API
    storage_backend: str     # "local" or "s3" — selects the RawStorageBackend
    s3_bucket: str | None    # Destination bucket when storage_backend == "s3"


def load_fpl_config() -> dict:
    """Read ~/.fpl/config.yaml and return its key-value pairs as a dict.

    This intentionally supports only simple ``key: value`` lines because the
    project uses it for a few path settings, not general YAML configuration.
    Comments and blank lines are ignored. Returns {} if the file is absent or
    unreadable.
    """
    try:
        text = _CONFIG_FILE.read_text(encoding="utf-8")
    except OSError:
        return {}

    result: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _KV_RE.match(line)
        if m:
            result[m.group(1)] = m.group(2).strip()
    return result


def _resolve_path(
    explicit: str | None,
    env_var: str,
    config_key: str,
    default: Path,
) -> Path:
    """Single priority-chain resolver used by all three public resolvers."""
    if explicit is not None:
        return Path(explicit).expanduser().resolve()
    env_val = os.environ.get(env_var)
    if env_val:
        return Path(env_val).expanduser().resolve()
    cfg = load_fpl_config()
    if config_key in cfg:
        return Path(cfg[config_key]).expanduser().resolve()
    return default.expanduser().resolve()


def resolve_raw_dir(explicit: str | None = None) -> Path:
    """Return the absolute path to the raw cache dir following the priority chain."""
    return _resolve_path(explicit, "FPL_RAW_DIR", "raw_dir", _DEFAULT_RAW_DIR)


def resolve_storage_backend() -> str:
    """Return the storage backend name ("local" or "s3") via env/config/default.

    Local disk stays the default so nothing changes for existing local dev
    workflows; CI opts in to S3 by setting ``FPL_STORAGE_BACKEND=s3``.
    """
    env_val = os.environ.get("FPL_STORAGE_BACKEND")
    if env_val:
        return env_val
    cfg = load_fpl_config()
    return cfg.get("storage_backend", "local")


def resolve_s3_bucket() -> str | None:
    """Return the destination S3 bucket name via env/config, or None."""
    env_val = os.environ.get("FPL_S3_BUCKET")
    if env_val:
        return env_val
    cfg = load_fpl_config()
    return cfg.get("s3_bucket")


def default_config() -> IngestConfig:
    """Build config from environment variables, falling back to defaults.

    Returns:
        IngestConfig with explicit resolution shared by both ingest and schema
        workflows.
    """
    return IngestConfig(
        raw_dir=resolve_raw_dir(),
        storage_backend=resolve_storage_backend(),
        s3_bucket=resolve_s3_bucket(),
    )


def resolve_config(
    *,
    raw_dir: Path | None = None,
) -> IngestConfig:
    """Merge CLI path overrides onto the shared resolution chain.

    Args:
        raw_dir: Explicit raw-cache directory from CLI, or None to resolve via
            the env/config/default chain.

    Returns:
        IngestConfig with CLI overrides applied over env/config/default
        resolution.
    """
    return IngestConfig(
        raw_dir=resolve_raw_dir(str(raw_dir) if raw_dir is not None else None),
        storage_backend=resolve_storage_backend(),
        s3_bucket=resolve_s3_bucket(),
    )
