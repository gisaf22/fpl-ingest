"""Direct unit tests for player history ingest."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from fpl_ingest.extract.stages.element_summary import ingest_player_histories
from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.load.store import SQLiteStore

pytestmark = pytest.mark.unit


def _store(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    with store.transaction():
        setup_store(store)
    return store


def _history_row(player_id: int, fixture_id: int, round_id: int) -> dict:
    return {
        "element": player_id,
        "fixture": fixture_id,
        "round": round_id,
        "opponent_team": 2,
        "was_home": True,
        "kickoff_time": "2025-08-16T14:00:00Z",
        "value": 55,
        "selected": 1000,
        "transfers_in": 10,
        "transfers_out": 5,
        "transfers_balance": 5,
    }


def _history(player_id: int, fixture_start: int = 100) -> dict:
    return {
        "history": [
            _history_row(player_id, fixture_start, 1),
            _history_row(player_id, fixture_start + 1, 2),
        ]
    }


def _count(store: SQLiteStore, table: str) -> int:
    return store.query(f"SELECT COUNT(*) AS count FROM {table}")[0]["count"]


@pytest.mark.asyncio
async def test_ingest_player_histories_happy_path_two_players(tmp_path):
    store = _store(tmp_path)
    client = AsyncMock()
    client.get_player_history.side_effect = [_history(1, 100), _history(2, 200)]
    raw_dir = tmp_path / "raw"

    result = (await ingest_player_histories(client, store, raw_dir, [1, 2])).result

    assert result.stage == "player_histories"
    assert result.errors == 0
    assert result.skipped == 0
    assert _count(store, "player_histories") == 4
    assert (raw_dir / "players" / "1.json").exists()
    assert (raw_dir / "players" / "2.json").exists()


@pytest.mark.asyncio
async def test_ingest_player_histories_empty_history_counts_error(tmp_path):
    client = AsyncMock()
    client.get_player_history.return_value = {"history": []}

    result = (await ingest_player_histories(client, _store(tmp_path), tmp_path / "raw", [1])).result

    assert result.fetched == 0
    assert result.errors == 0
    assert result.written == 0


@pytest.mark.asyncio
async def test_ingest_player_histories_non_strict_failure_commits_other_players(tmp_path):
    store = _store(tmp_path)
    client = AsyncMock()
    client.get_player_history.side_effect = [Exception("timeout"), _history(2, 200)]

    result = (await ingest_player_histories(client, store, tmp_path / "raw", [1, 2], strict=False)).result

    assert result.errors == 1
    assert _count(store, "player_histories") == 2


@pytest.mark.asyncio
async def test_ingest_player_histories_writes_raw_cache_files(tmp_path):
    client = AsyncMock()
    client.get_player_history.return_value = _history(1)
    raw_dir = tmp_path / "raw"

    await ingest_player_histories(client, _store(tmp_path), raw_dir, [1])

    path = raw_dir / "players" / "1.json"
    assert path.exists()
    assert isinstance(json.loads(path.read_text()), dict)


@pytest.mark.asyncio
async def test_ingest_player_histories_empty_player_ids_returns_immediately(tmp_path):
    client = AsyncMock()

    result = (await ingest_player_histories(client, _store(tmp_path), tmp_path / "raw", [])).result

    assert result.fetched == 0
    assert result.errors == 0
    client.get_player_history.assert_not_called()


@pytest.mark.asyncio
async def test_ingest_player_histories_partial_validation_failure(tmp_path):
    store = _store(tmp_path)
    valid = _history_row(1, 100, 1)
    invalid = _history_row(1, 101, 2)
    invalid.pop("fixture")
    client = AsyncMock()
    client.get_player_history.return_value = {"history": [valid, invalid]}

    result = (await ingest_player_histories(client, store, tmp_path / "raw", [1])).result

    assert result.skipped == 1
    assert result.written == 1
    assert _count(store, "player_histories") == 1
