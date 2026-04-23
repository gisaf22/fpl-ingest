from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from fpl_ingest.domain.execution_state import PIPELINE_STATE_FAILED, PipelineExecutionState
from fpl_ingest.pipeline.gameweeks import ingest_gameweeks
from fpl_ingest.pipeline.history import ingest_player_histories

pytestmark = pytest.mark.unit


def test_strict_gameweek_fetch_cancels_in_flight_tasks_and_blocks_writes(tmp_path: Path):
    cancelled = asyncio.Event()

    async def get_gw(gameweek_id: int):
        if gameweek_id == 1:
            raise RuntimeError("boom")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def _run():
        client = SimpleNamespace(get_gw=get_gw)
        store = MagicMock()
        execution_state = PipelineExecutionState()
        events = [
            SimpleNamespace(id=1, finished=True, is_current=False),
            SimpleNamespace(id=2, finished=True, is_current=False),
        ]
        result = await ingest_gameweeks(
            client,
            store,
            tmp_path,
            events,
            force=True,
            strict=True,
            execution_state=execution_state,
        )
        assert result.errors == 1
        assert result.validated == 0
        assert result.written == 0
        assert result.skipped == 0
        assert cancelled.is_set()
        assert execution_state.state == PIPELINE_STATE_FAILED
        store.upsert_models.assert_not_called()
        assert not (tmp_path / "gw_1.json").exists()
        assert not (tmp_path / "gw_2.json").exists()

    asyncio.run(_run())


def test_strict_player_history_fetch_cancels_in_flight_tasks_and_blocks_writes(tmp_path: Path):
    cancelled = asyncio.Event()

    async def get_player_history(player_id: int):
        if player_id == 1:
            raise RuntimeError("boom")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def _run():
        client = SimpleNamespace(get_player_history=get_player_history)
        store = MagicMock()
        execution_state = PipelineExecutionState()
        result = await ingest_player_histories(
            client,
            store,
            tmp_path,
            [1, 2],
            force=True,
            strict=True,
            execution_state=execution_state,
        )
        assert result.errors == 1
        assert result.validated == 0
        assert result.written == 0
        assert result.skipped == 0
        assert cancelled.is_set()
        assert execution_state.state == PIPELINE_STATE_FAILED
        store.upsert_models.assert_not_called()
        assert not (tmp_path / "players" / "1.json").exists()
        assert not (tmp_path / "players" / "2.json").exists()

    asyncio.run(_run())
