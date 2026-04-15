"""Runtime configuration helpers for the ingest CLI."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class IngestConfig:
    """Resolved runtime settings for a single ingest invocation."""

    db_path: Path
    raw_dir: Path


def parse_positive_int(
    value: int | str,
    *,
    name: str,
    max_value: int | None = None,
) -> int:
    """Parse and validate a positive integer configuration value."""
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be at least 1")
    if max_value is not None and parsed > max_value:
        raise ValueError(f"{name} must be at most {max_value}")
    return parsed


def default_config() -> IngestConfig:
    """Build config defaults from environment variables."""
    return IngestConfig(
        db_path=Path(os.environ.get("FPL_DB_PATH", Path.home() / ".fpl" / "fpl.db")),
        raw_dir=Path(os.environ.get("FPL_RAW_DIR", Path.home() / ".fpl" / "raw")),
    )


def resolve_config(
    *,
    db_path: Path | None = None,
    raw_dir: Path | None = None,
) -> IngestConfig:
    """Merge CLI overrides onto environment-backed defaults."""
    defaults = default_config()
    return IngestConfig(
        db_path=db_path or defaults.db_path,
        raw_dir=raw_dir or defaults.raw_dir,
    )
