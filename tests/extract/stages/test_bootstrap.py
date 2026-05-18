"""Direct unit tests for the bootstrap extract stage."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from fpl_ingest.extract.stages.bootstrap import ingest_core_data
from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.load.store import SQLiteStore

pytestmark = pytest.mark.unit


def _store(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    with store.transaction():
        setup_store(store)
    return store


def _player(player_id: int, team_id: int = 1, element_type: int = 1) -> dict:
    return {
        "id": player_id,
        "first_name": f"First{player_id}",
        "second_name": f"Last{player_id}",
        "web_name": f"Player{player_id}",
        "team": team_id,
        "team_code": 100 + team_id,
        "element_type": element_type,
        "now_cost": 50,
        "status": "a",
        "code": 1000 + player_id,
        "form_rank": player_id,
        "form_rank_type": player_id,
        "points_per_game_rank": player_id,
        "points_per_game_rank_type": player_id,
        "now_cost_rank": player_id,
        "now_cost_rank_type": player_id,
        "selected_rank": player_id,
        "selected_rank_type": player_id,
        "influence_rank": player_id,
        "influence_rank_type": player_id,
        "creativity_rank": player_id,
        "creativity_rank_type": player_id,
        "threat_rank": player_id,
        "threat_rank_type": player_id,
        "ict_index_rank": player_id,
        "ict_index_rank_type": player_id,
    }


def _team(team_id: int) -> dict:
    return {
        "id": team_id,
        "name": f"Team {team_id}",
        "short_name": f"T{team_id}",
        "code": 100 + team_id,
        "strength": 3,
        "strength_overall_home": 3,
        "strength_overall_away": 3,
        "strength_attack_home": 3,
        "strength_attack_away": 3,
        "strength_defence_home": 3,
        "strength_defence_away": 3,
        "position": team_id,
        "unavailable": False,
    }


def _event(event_id: int) -> dict:
    return {
        "id": event_id,
        "name": f"Gameweek {event_id}",
        "deadline_time": "2025-08-15T18:30:00Z",
        "deadline_time_epoch": 1755282600,
        "deadline_time_game_offset": 0,
        "finished": True,
        "data_checked": True,
        "is_previous": False,
        "is_current": False,
        "is_next": False,
        "can_enter": False,
        "can_manage": True,
        "cup_leagues_created": False,
        "h2h_ko_matches_created": False,
        "average_entry_score": 42,
        "highest_score": 100,
        "most_selected": 1,
        "most_transferred_in": 1,
        "most_captained": 1,
        "most_vice_captained": 1,
        "top_element": 1,
        "top_element_info": {"id": 1, "points": 12},
        "transfers_made": 0,
        "chip_plays": [],
    }


def _element_type(type_id: int, short_name: str) -> dict:
    return {
        "id": type_id,
        "singular_name": short_name,
        "singular_name_short": short_name,
        "plural_name": f"{short_name}s",
        "plural_name_short": short_name,
        "squad_select": 2,
        "squad_min_select": 1,
        "squad_max_select": 5,
        "squad_min_play": 1,
        "squad_max_play": 5,
        "ui_shirt_specific": False,
        "element_count": 1,
    }


def _bootstrap() -> dict:
    return {
        "elements": [_player(1, team_id=1, element_type=1), _player(2, team_id=2, element_type=2)],
        "teams": [_team(1), _team(2)],
        "events": [_event(1)],
        "element_types": [_element_type(1, "GKP"), _element_type(2, "DEF")],
    }


@pytest.mark.asyncio
async def test_ingest_core_data_happy_path_all_rows_valid(tmp_path):
    client = AsyncMock()
    client.get_bootstrap.return_value = _bootstrap()
    cache_dir = tmp_path / "raw"
    cache_dir.mkdir()

    outcome = await ingest_core_data(client, _store(tmp_path), cache_dir)
    data = outcome.output
    result = outcome.result

    assert result.stage == "core"
    assert result.errors == 0
    assert result.skipped == 0
    assert result.fetched == result.written
    assert len(data.players) == 2
    assert len(data.teams) == 2
    assert json.loads((cache_dir / "bootstrap.json").read_text())


@pytest.mark.asyncio
async def test_ingest_core_data_writes_bootstrap_atomically(tmp_path):
    client = AsyncMock()
    client.get_bootstrap.return_value = _bootstrap()
    cache_dir = tmp_path / "raw"
    cache_dir.mkdir()

    await ingest_core_data(client, _store(tmp_path), cache_dir)

    assert (cache_dir / "bootstrap.json").exists()
    assert not (cache_dir / "bootstrap.tmp").exists()


@pytest.mark.asyncio
async def test_ingest_core_data_swallows_cache_write_os_error(tmp_path):
    client = AsyncMock()
    client.get_bootstrap.return_value = _bootstrap()
    cache_dir = tmp_path / "raw"
    cache_dir.write_text("not a directory")

    result = (await ingest_core_data(client, _store(tmp_path), cache_dir)).result

    assert result.errors == 0


@pytest.mark.asyncio
async def test_ingest_core_data_empty_bootstrap_sections(tmp_path):
    client = AsyncMock()
    client.get_bootstrap.return_value = {
        "elements": [],
        "teams": [],
        "events": [],
        "element_types": [],
    }

    result = (await ingest_core_data(client, _store(tmp_path), tmp_path / "raw")).result

    assert result.fetched == 0
    assert result.written == 0
    assert result.errors == 0
