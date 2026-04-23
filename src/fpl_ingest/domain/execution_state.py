"""Canonical execution-state contract for ingest runs."""

from __future__ import annotations

from dataclasses import dataclass

PIPELINE_STATE_RUNNING = "RUNNING"
PIPELINE_STATE_FAILED = "FAILED"


@dataclass
class PipelineExecutionState:
    """Shared fail-fast state used to block post-failure side effects."""

    state: str = PIPELINE_STATE_RUNNING

    def fail(self) -> None:
        self.state = PIPELINE_STATE_FAILED

    @property
    def is_failed(self) -> bool:
        return self.state == PIPELINE_STATE_FAILED
