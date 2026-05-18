"""Direct unit tests for fixture ingest."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from fpl_ingest.extract.stages.fixtures import ingest_fixtures
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.load.store import SQLiteStore

pytestmark = pytest.mark.unit


def _store(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    with store.transaction():
        setup_store(store)
    return store


def _fixture(fixture_id: int, stats: list[dict] | None = None) -> dict:
    fixture = {
        "id": fixture_id,
        "code": 1000 + fixture_id,
        "event": 1,
        "team_h": 1,
        "team_a": 2,
        "team_h_score": 1,
        "team_a_score": 0,
        "team_h_difficulty": 3,
        "team_a_difficulty": 4,
        "kickoff_time": "2025-08-16T14:00:00Z",
        "finished": True,
        "finished_provisional": True,
    }
    if stats is not None:
        fixture["stats"] = stats
    return fixture


def _count(store: SQLiteStore, table: str) -> int:
    return store.query(f"SELECT COUNT(*) AS count FROM {table}")[0]["count"]


@pytest.mark.asyncio
async def test_ingest_fixtures_happy_path_without_stats(tmp_path):
    store = _store(tmp_path)
    client = AsyncMock()
    client.get_fixtures.return_value = [_fixture(1, stats=[]), _fixture(2)]
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    result = (await ingest_fixtures(client, store, raw_dir)).result

    assert result.stage == "fixtures"
    assert result.errors == 0
    assert result.skipped == 0
    assert json.loads((raw_dir / "fixtures.json").read_text())
    assert _count(store, "fixtures") == 2


@pytest.mark.asyncio
async def test_ingest_fixtures_happy_path_with_stats_rows(tmp_path):
    store = _store(tmp_path)
    client = AsyncMock()
    client.get_fixtures.return_value = [
        _fixture(
            1,
            stats=[
                {
                    "identifier": "goals_scored",
                    "h": [{"element": 1, "value": 1}],
                    "a": [{"element": 2, "value": 0}],
                }
            ],
        )
    ]

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    result = (await ingest_fixtures(client, store, raw_dir)).result

    assert result.written > result.fetched / 2
    assert _count(store, "fixture_stats") >= 1


@pytest.mark.asyncio
async def test_ingest_fixtures_client_error_returns_error_result(tmp_path):
    store = _store(tmp_path)
    client = AsyncMock()
    client.get_fixtures.side_effect = FPLClientError("test error")

    result = (await ingest_fixtures(client, store, tmp_path / "raw")).result

    assert result.errors == 1
    assert result.fetched == 0
    assert _count(store, "fixtures") == 0


@pytest.mark.asyncio
async def test_ingest_fixtures_empty_list_returns_zero_counts(tmp_path):
    client = AsyncMock()
    client.get_fixtures.return_value = []

    result = (await ingest_fixtures(client, _store(tmp_path), tmp_path / "raw")).result

    assert result.fetched == 0
    assert result.errors == 0
