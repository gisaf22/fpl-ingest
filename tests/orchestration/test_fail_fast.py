"""Regression tests for strict-mode fail-fast cancellation."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from fpl_ingest.orchestration.execution_state import PIPELINE_STATE_FAILED, PipelineExecutionState
from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.extract.stages.gameweeks import ingest_gameweeks
from fpl_ingest.extract.stages.element_summary import ingest_player_histories
from fpl_ingest.load.store import SQLiteStore

pytestmark = pytest.mark.integration


async def _fail_on_first(id_: int) -> dict:
    """Fail immediately for id==1; simulate a long in-flight request for others."""
    if id_ == 1:
        raise RuntimeError("boom")
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        raise
    return {}


def _make_store(tmp_path: Path) -> SQLiteStore:
    store = SQLiteStore(tmp_path / "test.db")
    with store.transaction():
        setup_store(store)
    return store


async def _run_gameweeks(client, store: SQLiteStore, raw_dir: Path, state: PipelineExecutionState) -> object:
    events = [
        SimpleNamespace(id=1, finished=True, is_current=False),
        SimpleNamespace(id=2, finished=True, is_current=False),
    ]
    return await ingest_gameweeks(
        client, store, raw_dir, events,
        force=True, strict=True, execution_state=state,
    )


async def _run_player_histories(client, store: SQLiteStore, raw_dir: Path, state: PipelineExecutionState) -> object:
    return await ingest_player_histories(
        client, store, raw_dir, [1, 2],
        force=True, strict=True, execution_state=state,
    )


def _gameweek_cache_paths(raw_dir: Path) -> list[Path]:
    return [raw_dir / "gw_1.json", raw_dir / "gw_2.json"]


def _history_cache_paths(raw_dir: Path) -> list[Path]:
    return [raw_dir / "players" / "1.json", raw_dir / "players" / "2.json"]


@pytest.mark.parametrize(
    "run_fn,table,cache_paths_fn",
    [
        pytest.param(_run_gameweeks, "gameweeks", _gameweek_cache_paths, id="gameweeks"),
        pytest.param(_run_player_histories, "player_histories", _history_cache_paths, id="player_histories"),
    ],
)
def test_strict_abort_blocks_writes_and_leaves_no_partial_data(
    run_fn, table, cache_paths_fn, tmp_path: Path
) -> None:
    """A strict fetch failure must block all DB writes and produce no partial cache files."""
    store = _make_store(tmp_path)
    raw_dir = tmp_path / "raw"

    async def _run() -> None:
        client = SimpleNamespace(
            get_gw=_fail_on_first,
            get_player_history=_fail_on_first,
        )
        state = PipelineExecutionState()
        result = (await run_fn(client, store, raw_dir, state)).result

        assert result.errors == 1
        assert result.validated == 0
        assert result.written == 0
        assert result.skipped == 0
        assert state.state == PIPELINE_STATE_FAILED

        rows = store.query(f"SELECT COUNT(*) as n FROM {table}")
        assert rows[0]["n"] == 0, (
            f"strict abort must leave {table} empty; found {rows[0]['n']} row(s)"
        )

        for path in cache_paths_fn(raw_dir):
            assert not path.exists(), f"strict abort must not write cache file {path.name}"

    asyncio.run(_run())
