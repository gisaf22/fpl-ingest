"""Canonical immutable outcome record returned by every ingest stage.

``StageResult`` is the shared contract between extract stages and the runner.
Its four counting fields (fetched, validated, written, skipped) follow strict
invariants enforced at construction time, ensuring the audit table is always
internally consistent. The runner, replay, and integrity checks all read from
this shape — no stage should return a plain dict or raise instead of returning
a result.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Generic, Iterable, TypeVar

_T = TypeVar("_T")


@dataclass(frozen=True)
class StageResult:
    """Immutable per-stage outcome summary returned by each ingest stage.

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


@dataclass(frozen=True)
class StageMetadata:
    """Static stage contract declared by each stage implementation."""

    name: str
    dependencies: tuple[str, ...] = ()
    raw_artifacts: tuple[str, ...] = ()
    output_tables: tuple[str, ...] = ()


@dataclass(frozen=True)
class StageLineage:
    """Runtime lineage for one stage execution."""

    stage: str
    raw_artifacts: tuple[str, ...] = ()
    output_tables: tuple[str, ...] = ()

    @classmethod
    def from_metadata(
        cls,
        metadata: StageMetadata,
        *,
        raw_artifacts: Iterable[str | Path] | None = None,
        output_tables: Iterable[str] | None = None,
    ) -> "StageLineage":
        return cls(
            stage=metadata.name,
            raw_artifacts=tuple(str(path) for path in (raw_artifacts or metadata.raw_artifacts)),
            output_tables=tuple(output_tables or metadata.output_tables),
        )


@dataclass(frozen=True)
class StageOutcome(Generic[_T]):
    """Explicit stage execution contract returned by live and replay stages."""

    result: StageResult
    output: _T | None = None
    lineage: StageLineage | None = None
    metadata: dict[str, str] = field(default_factory=dict)
