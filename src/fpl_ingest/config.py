"""Runtime configuration for the ingest CLI.

Reads configuration from environment variables with sensible defaults,
and merges CLI overrides on top. This module does not perform validation
beyond type coercion — invalid paths are discovered at runtime when the
pipeline tries to use them.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Default paths when environment variables are not set.
_DEFAULT_DB_PATH = Path.home() / ".fpl" / "fpl.db"
_DEFAULT_RAW_DIR = Path.home() / ".fpl" / "raw"


@dataclass(frozen=True)
class IngestConfig:
    """Resolved runtime settings for a single ingest invocation."""

    db_path: Path   # SQLite database file path
    raw_dir: Path   # Directory for raw JSON cache files from the API


def default_config() -> IngestConfig:
    """Build config from environment variables, falling back to defaults.

    Returns:
        IngestConfig with paths from FPL_DB_PATH / FPL_RAW_DIR env vars,
        or ~/.fpl/fpl.db and ~/.fpl/raw if those are not set.
    """
    return IngestConfig(
        db_path=Path(os.environ.get("FPL_DB_PATH", _DEFAULT_DB_PATH)),
        raw_dir=Path(os.environ.get("FPL_RAW_DIR", _DEFAULT_RAW_DIR)),
    )


def resolve_config(
    *,
    db_path: Path | None = None,
    raw_dir: Path | None = None,
) -> IngestConfig:
    """Merge CLI path overrides onto environment-backed defaults.

    Args:
        db_path: Explicit database path from CLI, or None to use the default.
        raw_dir: Explicit raw-cache directory from CLI, or None to use the default.

    Returns:
        IngestConfig with CLI overrides applied over environment defaults.
    """
    defaults = default_config()
    return IngestConfig(
        db_path=db_path or defaults.db_path,
        raw_dir=raw_dir or defaults.raw_dir,
    )
