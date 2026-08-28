"""Regression tests for strict-mode fail-fast cancellation.

Covers ``player_histories`` (element-summary capture): it is the stage whose
concurrent fetch and strict-mode cancellation semantics this migration was
explicitly required to keep unmodified (strategy doc B.1). A strict abort
must cancel in-flight fetches, write no raw payloads, and trip the fail-fast
sentinel — the same guarantee ``tests/extract/stages/test_gameweeks.py``
already covers for the gameweeks stage.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.orchestration.execution_state import PIPELINE_STATE_FAILED, PipelineExecutionState
from fpl_ingest.extract.stages.element_summary import ingest_player_histories

pytestmark = pytest.mark.integration


async def _fail_on_first(id_: int) -> object:
    """Fail immediately for id==1; simulate a long in-flight request for others."""
    if id_ == 1:
        raise RuntimeError("boom")
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        raise
    return object()


def _writer(tmp_path: Path) -> LocalRawWriter:
    return LocalRawWriter(tmp_path / "raw", "fpl", run_id="20260824T080000Z-abc123")


async def _run_player_histories(
    client, writer: LocalRawWriter, raw_dir: Path, state: PipelineExecutionState
) -> object:
    return await ingest_player_histories(
        client, writer, raw_dir, [1, 2], [], event_finality=None, strict=True, execution_state=state,
    )


def test_strict_abort_blocks_writes_and_leaves_no_partial_data(tmp_path: Path) -> None:
    """A strict fetch failure must block all raw writes for this stage."""
    writer = _writer(tmp_path)

    async def _run() -> None:
        client = SimpleNamespace(get_element_summary_raw=_fail_on_first)
        state = PipelineExecutionState()
        result = (await _run_player_histories(client, writer, tmp_path / "raw", state)).result

        assert result.errors == 1
        assert result.validated == 0
        assert result.written == 0
        assert result.skipped == result.fetched
        assert state.state == PIPELINE_STATE_FAILED

        assert not (tmp_path / "raw" / "fpl" / "element-summary").exists(), (
            "strict abort must not write any element-summary payload"
        )

    asyncio.run(_run())
