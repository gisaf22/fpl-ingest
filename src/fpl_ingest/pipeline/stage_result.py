"""StageResult — immutable outcome dataclass for pipeline stages.

Provides a single immutable dataclass that each pipeline stage returns to
the CLI. This module has no dependencies on other fpl_ingest modules.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StageResult:
    """Immutable per-stage outcome summary returned by each pipeline stage."""

    stage: str
    fetched: int = 0
    """Raw item count from the API before validation. Does not reflect successfully stored rows."""
    upserted: int = 0
    skipped: int = 0
    errors: int = 0

    def summary_line(self) -> str:
        """Return a human-readable single-line summary for logs and error messages."""
        return (
            f"{self.stage}: fetched={self.fetched}, upserted={self.upserted}, "
            f"skipped={self.skipped}, errors={self.errors}"
        )
