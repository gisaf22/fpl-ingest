"""Lightweight stage outcome summaries for ingest stages."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StageResult:
    """Minimal per-stage outcome summary."""

    stage: str
    fetched: int = 0
    upserted: int = 0
    skipped: int = 0
    errors: int = 0

    def summary_line(self) -> str:
        """Human-readable single-line summary for logs and errors."""
        return (
            f"{self.stage}: fetched={self.fetched}, upserted={self.upserted}, "
            f"skipped={self.skipped}, errors={self.errors}"
        )
