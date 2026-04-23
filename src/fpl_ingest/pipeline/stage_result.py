"""Canonical immutable outcome dataclass for pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class StageResult:
    """Immutable per-stage outcome summary returned by each pipeline stage.

    Canonical metric semantics:
    - fetched: raw records received from source payloads before validation
    - validated: records that passed schema validation
    - written: validated records persisted to SQLite
    - skipped: records rejected by validation

    Invariants:
    - fetched >= validated >= written
    - skipped == fetched - validated
    """

    stage: str
    fetched: int = 0
    validated: int = 0
    written: int = 0
    skipped: int = 0
    errors: int = 0

    def __post_init__(self) -> None:
        for field_name in ("fetched", "validated", "written", "skipped", "errors"):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(f"StageResult.{field_name} must be non-negative, got {value}")

        if self.fetched < self.validated:
            raise ValueError(
                f"StageResult invariant violated for {self.stage}: "
                f"fetched={self.fetched} < validated={self.validated}"
            )
        if self.validated < self.written:
            raise ValueError(
                f"StageResult invariant violated for {self.stage}: "
                f"validated={self.validated} < written={self.written}"
            )
        expected_skipped = self.fetched - self.validated
        if self.skipped != expected_skipped:
            raise ValueError(
                f"StageResult invariant violated for {self.stage}: "
                f"skipped={self.skipped} != fetched-validated={expected_skipped}"
            )

    @property
    def upserted(self) -> int:
        """Backward-compatible alias for the canonical written metric."""
        return self.written

    @property
    def is_clean(self) -> bool:
        """Return True when the stage completed with no skips or errors."""
        return self.skipped == 0 and self.errors == 0

    @property
    def failure_reason(self) -> str | None:
        """Return the canonical failure reason label for unclean stage results."""
        if self.errors:
            return "validation_error"
        if self.skipped:
            return "skipped_records"
        return None

    def summary_line(self) -> str:
        """Return the canonical one-line stage metric summary for logs."""
        return (
            f"[stage={self.stage}] fetched={self.fetched} "
            f"validated={self.validated} written={self.written} skipped={self.skipped} errors={self.errors}"
        )

    @staticmethod
    def totals(stage_results: Iterable["StageResult"]) -> tuple[int, int, int, int, int]:
        """Return aggregate fetched, validated, written, skipped, and error counts."""
        results = list(stage_results)
        return (
            sum(result.fetched for result in results),
            sum(result.validated for result in results),
            sum(result.written for result in results),
            sum(result.skipped for result in results),
            sum(result.errors for result in results),
        )
