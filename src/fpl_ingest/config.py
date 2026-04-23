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

_DEFAULT_DB_PATH = Path.home() / ".fpl" / "fpl.db"
_DEFAULT_RAW_DIR = Path.home() / ".fpl" / "raw"
_CONFIG_FILE = Path.home() / ".fpl" / "config.yaml"

_KV_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*):\s*(.+)$")


@dataclass(frozen=True)
class IngestConfig:
    """Resolved runtime settings for a single ingest invocation."""

    db_path: Path   # SQLite database file path
    raw_dir: Path   # Directory for raw JSON cache files from the API


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


def resolve_db_path(explicit: str | None = None) -> Path:
    """Return the absolute path to fpl.db following the priority chain."""
    return _resolve_path(explicit, "FPL_DB_PATH", "db_path", _DEFAULT_DB_PATH)


def resolve_db_path_with_source(explicit: str | None = None) -> tuple[Path, str]:
    """Return the resolved database path plus the source used to resolve it."""
    if explicit is not None:
        return Path(explicit).expanduser().resolve(), "flag"
    env_val = os.environ.get("FPL_DB_PATH")
    if env_val:
        return Path(env_val).expanduser().resolve(), "env"
    cfg = load_fpl_config()
    if "db_path" in cfg:
        return Path(cfg["db_path"]).expanduser().resolve(), "config"
    return _DEFAULT_DB_PATH.expanduser().resolve(), "default"


def resolve_raw_dir(explicit: str | None = None) -> Path:
    """Return the absolute path to the raw cache dir following the priority chain."""
    return _resolve_path(explicit, "FPL_RAW_DIR", "raw_dir", _DEFAULT_RAW_DIR)


def default_config() -> IngestConfig:
    """Build config from environment variables, falling back to defaults.

    Returns:
        IngestConfig with explicit resolution shared by both ingest and schema
        workflows.
    """
    return IngestConfig(
        db_path=resolve_db_path(),
        raw_dir=resolve_raw_dir(),
    )


def resolve_config(
    *,
    db_path: Path | None = None,
    raw_dir: Path | None = None,
) -> IngestConfig:
    """Merge CLI path overrides onto the shared resolution chain.

    Args:
        db_path: Explicit database path from CLI, or None to resolve via the
            env/config/default chain.
        raw_dir: Explicit raw-cache directory from CLI, or None to resolve via
            the env/config/default chain.

    Returns:
        IngestConfig with CLI overrides applied over env/config/default
        resolution.
    """
    return IngestConfig(
        db_path=resolve_db_path(str(db_path) if db_path is not None else None),
        raw_dir=resolve_raw_dir(str(raw_dir) if raw_dir is not None else None),
    )
