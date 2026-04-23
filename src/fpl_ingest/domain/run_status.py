"""Canonical terminal run-status semantics for ingest runs."""

from __future__ import annotations

from typing import Literal

RunStatus = Literal["SUCCESS", "FAILED", "FAILED_PARTIAL"]

RUN_STATUS_SUCCESS = "SUCCESS"
RUN_STATUS_FAILED = "FAILED"
RUN_STATUS_FAILED_PARTIAL = "FAILED_PARTIAL"


def classify_run(*, errors: int, skipped: int, strict_mode: bool) -> RunStatus:
    """Return the terminal run status using the canonical precedence order.

    Precedence is strict and deterministic:
    FAILED > FAILED_PARTIAL > SUCCESS
    """
    if strict_mode or errors > 0:
        return RUN_STATUS_FAILED
    if skipped > 0:
        return RUN_STATUS_FAILED_PARTIAL
    return RUN_STATUS_SUCCESS


def classify_run_from_results(stage_results: list[object], *, strict_mode: bool) -> RunStatus:
    """Classify the terminal run status from canonical stage results."""
    skipped = sum(getattr(result, "skipped", 0) for result in stage_results)
    errors = sum(getattr(result, "errors", 0) for result in stage_results)
    return classify_run(errors=errors, skipped=skipped, strict_mode=strict_mode)
