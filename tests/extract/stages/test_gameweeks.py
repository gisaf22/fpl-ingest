"""Direct unit tests for gameweek ingest."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from fpl_ingest.extract.stages.gameweeks import ingest_gameweeks
from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.load.store import SQLiteStore
from fpl_ingest.transform.models import EventModel

pytestmark = pytest.mark.unit


def _store(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    with store.transaction():
        setup_store(store)
    return store


def _event(event_id: int, *, finished: bool, is_current: bool = False) -> EventModel:
    return EventModel(
        id=event_id,
        name=f"Gameweek {event_id}",
        deadline_time="2025-08-15T18:30:00Z",
        deadline_time_epoch=1755282600,
        deadline_time_game_offset=0,
        finished=finished,
        data_checked=finished,
        is_previous=False,
        is_current=is_current,
        is_next=False,
        can_enter=False,
        can_manage=True,
        cup_leagues_created=False,
        h2h_ko_matches_created=False,
    )


def _gw(player_ids: list[int]) -> dict:
    return {
        "elements": [
            {
                "id": player_id,
                "stats": {"minutes": 90, "total_points": player_id},
                "explain": [],
            }
            for player_id in player_ids
        ]
    }


def _count(store: SQLiteStore, table: str) -> int:
    return store.query(f"SELECT COUNT(*) AS count FROM {table}")[0]["count"]


@pytest.mark.asyncio
async def test_ingest_gameweeks_happy_path_finished_gameweek_without_cache(tmp_path):
    store = _store(tmp_path)
    client = AsyncMock()
    client.get_gw.return_value = _gw([1, 2])
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    result = (await ingest_gameweeks(client, store, raw_dir, [_event(1, finished=True)], force=False)).result

    assert result.stage == "gameweeks"
    assert result.errors == 0
    assert result.skipped == 0
    assert (raw_dir / "gw_1.json").exists()
    assert _count(store, "gameweeks") == 2


@pytest.mark.asyncio
async def test_ingest_gameweeks_skips_finished_gameweek_with_cache(tmp_path):
    client = AsyncMock()
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "gw_1.json").write_text("{}")

    result = (await ingest_gameweeks(client, _store(tmp_path), raw_dir, [_event(1, finished=True)], force=False)).result

    client.get_gw.assert_not_called()
    assert result.fetched == 0


@pytest.mark.asyncio
async def test_ingest_gameweeks_force_bypasses_cache(tmp_path):
    client = AsyncMock()
    client.get_gw.return_value = _gw([1])
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "gw_1.json").write_text("{}")

    await ingest_gameweeks(client, _store(tmp_path), raw_dir, [_event(1, finished=True)], force=True)

    client.get_gw.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_ingest_gameweeks_fetches_current_unfinished_gameweek(tmp_path):
    client = AsyncMock()
    client.get_gw.return_value = _gw([1])
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    result = (await ingest_gameweeks(
        client,
        _store(tmp_path),
        raw_dir,
        [_event(2, finished=False, is_current=True)],
        force=False,
    )).result

    client.get_gw.assert_awaited_once_with(2)
    assert result.fetched > 0


@pytest.mark.asyncio
async def test_ingest_gameweeks_no_events_returns_zero_counts(tmp_path):
    client = AsyncMock()

    result = (await ingest_gameweeks(client, _store(tmp_path), tmp_path / "raw", [], force=False)).result

    assert result.fetched == 0
    assert result.errors == 0


@pytest.mark.asyncio
async def test_ingest_gameweeks_non_strict_failure_commits_other_gameweeks(tmp_path):
    store = _store(tmp_path)
    client = AsyncMock()
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    async def get_gw(gameweek_id: int):
        if gameweek_id == 1:
            raise Exception("network down")
        return _gw([20, 21])

    client.get_gw.side_effect = get_gw

    result = (await ingest_gameweeks(
        client,
        store,
        raw_dir,
        [_event(1, finished=True), _event(2, finished=True)],
        force=False,
        strict=False,
    )).result

    assert result.errors == 1
    assert store.query("SELECT DISTINCT round FROM gameweeks") == [{"round": 2}]
    assert _count(store, "gameweeks") == 2
