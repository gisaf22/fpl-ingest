"""Shared fail-fast execution state for a single ingest run.

Carries the RUNNING / FAILED sentinel used by orchestration and raw-cache
writers. Persistence does not inspect this object; stages must make write
suppression decisions explicitly before calling the store.
"""

from __future__ import annotations

from dataclasses import dataclass

PIPELINE_STATE_RUNNING = "RUNNING"
PIPELINE_STATE_FAILED = "FAILED"


@dataclass
class PipelineExecutionState:
    """Shared fail-fast state used by orchestration and cache writers."""

    state: str = PIPELINE_STATE_RUNNING

    def fail(self) -> None:
        self.state = PIPELINE_STATE_FAILED

    @property
    def is_failed(self) -> bool:
        return self.state == PIPELINE_STATE_FAILED
