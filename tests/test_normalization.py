"""Logic tests — verify transforms and storage are mathematically sound.

These use pure mock data (no HTTP). If ``cost_to_millions`` drifts or
``flatten_live_element`` drops a stat field, these tests catch it before
the modelling stage sees NaNs.
"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from fpl_ingest.store import SQLiteStore
from fpl_ingest.models import PlayerHistoryModel, PlayerModel, GameweekModel
from fpl_ingest.transforms import cost_to_millions, flatten_live_element, flatten_live_elements, flatten_fixture_stats, flatten_event, ELEMENT_TYPE_TO_POS, POS_TO_ELEMENT_TYPE


# ---------------------------------------------------------------------------
# Transforms: cost_to_millions
# ---------------------------------------------------------------------------


class TestCostToMillions:
    def test_standard_conversion(self):
        assert cost_to_millions(100) == 10.0

    def test_fractional(self):
        assert cost_to_millions(45) == 4.5

    def test_premium(self):
        assert cost_to_millions(145) == 14.5

    def test_zero(self):
        assert cost_to_millions(0) == 0.0

    def test_budget(self):
        assert cost_to_millions(39) == 3.9



# ---------------------------------------------------------------------------
# Transforms: position mappings (bidirectional consistency)
# ---------------------------------------------------------------------------


class TestPositionMappings:
    def test_forward_mapping_complete(self):
        assert set(ELEMENT_TYPE_TO_POS.keys()) == {1, 2, 3, 4}
        assert set(ELEMENT_TYPE_TO_POS.values()) == {"GKP", "DEF", "MID", "FWD"}

    def test_reverse_mapping_complete(self):
        assert set(POS_TO_ELEMENT_TYPE.keys()) == {"GKP", "DEF", "MID", "FWD"}

    def test_roundtrip(self):
        for code, pos in ELEMENT_TYPE_TO_POS.items():
            assert POS_TO_ELEMENT_TYPE[pos] == code


# ---------------------------------------------------------------------------
# Transforms: flatten_live_element
# ---------------------------------------------------------------------------


class TestFlattenLiveElement:
    ELEMENT = {
        "id": 42,
        "stats": {
            "minutes": 90,
            "goals_scored": 2,
            "assists": 1,
            "clean_sheets": 0,
            "goals_conceded": 1,
            "expected_goals": "1.20",
            "expected_assists": "0.55",
            "total_points": 15,
            "bonus": 3,
            "starts": 1,
        },
    }

    def test_adds_element_id_and_round(self):
        flat = flatten_live_element(self.ELEMENT, gw=5)
        assert flat["element_id"] == 42
        assert flat["round"] == 5

    def test_preserves_all_stats(self):
        flat = flatten_live_element(self.ELEMENT, gw=5)
        assert flat["goals_scored"] == 2
        assert flat["assists"] == 1
        assert flat["expected_goals"] == "1.20"

    def test_validates_into_gameweek_model(self):
        flat = flatten_live_element(self.ELEMENT, gw=5)
        gw = GameweekModel.model_validate(flat)
        assert gw.element_id == 42
        assert gw.goals_scored == 2
        assert gw.expected_goals == pytest.approx(1.20)

    def test_missing_id_raises(self):
        with pytest.raises(ValueError, match="missing 'id'"):
            flatten_live_element({"stats": {}}, gw=1)


class TestFlattenLiveElements:
    def test_skips_elements_without_id(self):
        elements = [
            {"id": 1, "stats": {"minutes": 90}},
            {"stats": {"minutes": 45}},  # no id — should be skipped
            {"id": 3, "stats": {"minutes": 60}},
        ]
        result = flatten_live_elements(elements, gw=10)
        assert len(result) == 2
        assert result[0]["element_id"] == 1
        assert result[1]["element_id"] == 3

    def test_empty_list(self):
        assert flatten_live_elements([], gw=1) == []


# ---------------------------------------------------------------------------
# Store: SQLiteStore round-trip
# ---------------------------------------------------------------------------


class TestSQLiteStore:
    """Verify the storage layer persists and retrieves data correctly."""

    @pytest.fixture
    def store(self, tmp_path):
        return SQLiteStore(tmp_path / "test.db")

    def test_register_and_upsert_players(self, store):
        store.register_table("players", PlayerModel)
        raw = [
            {"id": 1, "web_name": "Salah", "team": 11, "element_type": 3, "now_cost": 130},
            {"id": 2, "web_name": "Haaland", "team": 13, "element_type": 4, "now_cost": 145},
        ]
        inserted, skipped = store.upsert_models("players", PlayerModel, raw)
        assert inserted == 2
        assert skipped == 0

    def test_query_returns_persisted_data(self, store):
        store.register_table("players", PlayerModel)
        store.upsert_models(
            "players",
            PlayerModel,
            [{"id": 1, "web_name": "Salah", "team": 11, "element_type": 3, "now_cost": 130}],
        )
        rows = store.query("SELECT * FROM players WHERE id = ?", (1,))
        assert len(rows) == 1
        assert rows[0]["web_name"] == "Salah"

    def test_upsert_replaces_on_conflict(self, store):
        store.register_table("players", PlayerModel)
        store.upsert_models(
            "players", PlayerModel,
            [{"id": 1, "web_name": "Salah", "team": 11, "element_type": 3, "now_cost": 130}],
        )
        store.upsert_models(
            "players", PlayerModel,
            [{"id": 1, "web_name": "Salah", "team": 11, "element_type": 3, "now_cost": 135}],
        )
        rows = store.query("SELECT now_cost FROM players WHERE id = 1")
        assert rows[0]["now_cost"] == 135

    def test_skips_invalid_rows(self, store):
        store.register_table("players", PlayerModel)
        raw = [
            {"id": 1, "web_name": "Good", "team": 1, "element_type": 3, "now_cost": 55},
            {"web_name": "NoId"},  # missing required 'id'
        ]
        inserted, skipped = store.upsert_models("players", PlayerModel, raw)
        assert inserted == 1
        assert skipped == 1

    def test_gameweek_unique_constraint(self, store):
        store.register_table(
            "gameweeks",
            GameweekModel,
            unique_constraint=GameweekModel.DEFAULT_UNIQUE,
        )
        row = {"element_id": 1, "round": 5, "minutes": 90, "total_points": 8}
        store.upsert_models("gameweeks", GameweekModel, [row])
        # Upsert same key with new points
        row2 = {"element_id": 1, "round": 5, "minutes": 90, "total_points": 12}
        store.upsert_models("gameweeks", GameweekModel, [row2])
        rows = store.query(
            "SELECT total_points FROM gameweeks WHERE element_id = 1 AND round = 5"
        )
        assert len(rows) == 1
        assert rows[0]["total_points"] == 12

    def test_player_history_preserves_multiple_fixtures_in_same_round(self, store):
        store.register_table(
            "player_histories",
            PlayerHistoryModel,
            unique_constraint=PlayerHistoryModel.DEFAULT_UNIQUE,
        )
        rows = [
            {"element_id": 1, "round": 5, "fixture": 101, "minutes": 90, "total_points": 8},
            {"element_id": 1, "round": 5, "fixture": 102, "minutes": 45, "total_points": 4},
        ]
        inserted, skipped = store.upsert_models("player_histories", PlayerHistoryModel, rows)
        assert inserted == 2
        assert skipped == 0

        persisted = store.query(
            "SELECT fixture, total_points FROM player_histories "
            "WHERE element_id = 1 AND round = 5 ORDER BY fixture"
        )
        assert persisted == [
            {"fixture": 101, "total_points": 8},
            {"fixture": 102, "total_points": 4},
        ]

    def test_player_history_replay_updates_same_fixture_row(self, store):
        store.register_table(
            "player_histories",
            PlayerHistoryModel,
            unique_constraint=PlayerHistoryModel.DEFAULT_UNIQUE,
        )
        original = {"element_id": 1, "round": 5, "fixture": 101, "minutes": 90, "total_points": 8}
        replayed = {"element_id": 1, "round": 5, "fixture": 101, "minutes": 90, "total_points": 10}

        store.upsert_models("player_histories", PlayerHistoryModel, [original])
        store.upsert_models("player_histories", PlayerHistoryModel, [replayed])

        rows = store.query(
            "SELECT total_points FROM player_histories "
            "WHERE element_id = 1 AND round = 5 AND fixture = 101"
        )
        assert len(rows) == 1
        assert rows[0]["total_points"] == 10

    def test_create_index(self, store):
        store.register_table("players", PlayerModel)
        # Should not raise
        store.create_index("players", ["web_name"])

    def test_player_helper_properties_are_not_persisted(self, store):
        store.register_table("players", PlayerModel)
        raw = {"id": 1, "web_name": "Salah", "team": 11, "element_type": 3, "now_cost": 130}
        model = PlayerModel.model_validate(raw)

        dumped = model.model_dump()
        assert "position" not in dumped
        assert "cost_millions" not in dumped
        assert "display_name" not in dumped


class TestSchemaEvolution:
    """register_table migrates existing tables when the model gains new fields."""

    def test_new_field_added_as_column(self, tmp_path):
        from typing import Optional
        from pydantic import BaseModel

        class V1(BaseModel):
            id: int
            name: str

        class V2(BaseModel):
            id: int
            name: str
            score: Optional[int] = None

        store = SQLiteStore(tmp_path / "evo.db")
        store.register_table("things", V1)

        # Simulate a model gaining a new field between runs.
        store.register_table("things", V2)

        conn = store._get_connection()
        cols = {row[1] for row in conn.execute("PRAGMA table_info(things)").fetchall()}
        conn.close()

        assert "score" in cols, "new column must be added by migration"
        assert "name" in cols, "existing column must be preserved"

    def test_existing_columns_not_duplicated(self, tmp_path):
        from pydantic import BaseModel

        class M(BaseModel):
            id: int
            name: str

        store = SQLiteStore(tmp_path / "evo2.db")
        store.register_table("things", M)
        store.register_table("things", M)  # second call must be a no-op

        conn = store._get_connection()
        col_names = [row[1] for row in conn.execute("PRAGMA table_info(things)").fetchall()]
        conn.close()

        assert col_names.count("name") == 1, "column must not be duplicated"


# ---------------------------------------------------------------------------
# Transforms: flatten_fixture_stats
# ---------------------------------------------------------------------------


class TestFlattenFixtureStats:
    FIXTURE = {
        "id": 301,
        "stats": [
            {
                "identifier": "goals_scored",
                "h": [{"element": 10, "value": 2}],
                "a": [{"element": 20, "value": 1}],
            },
            {
                "identifier": "assists",
                "h": [{"element": 11, "value": 1}],
                "a": [],
            },
        ],
    }

    def test_returns_one_row_per_stat_entry(self):
        rows = flatten_fixture_stats(self.FIXTURE)
        # 1 home goals + 1 away goals + 1 home assists = 3
        assert len(rows) == 3

    def test_row_contains_all_expected_fields(self):
        rows = flatten_fixture_stats(self.FIXTURE)
        first = rows[0]
        assert first["fixture_id"] == 301
        assert first["identifier"] == "goals_scored"
        assert first["element"] == 10
        assert first["value"] == 2
        assert first["side"] == "h"

    def test_side_field_distinguishes_home_away(self):
        rows = flatten_fixture_stats(self.FIXTURE)
        sides = {r["side"] for r in rows}
        assert sides == {"h", "a"}

    def test_missing_id_returns_empty(self):
        assert flatten_fixture_stats({"stats": []}) == []

    def test_empty_stats_returns_empty(self):
        assert flatten_fixture_stats({"id": 1, "stats": []}) == []


# ---------------------------------------------------------------------------
# Transforms: flatten_event
# ---------------------------------------------------------------------------


class TestFlattenEvent:
    EVENT = {
        "id": 24,
        "name": "Gameweek 24",
        "deadline_time": "2026-02-15T10:00:00Z",
        "top_element_info": {"element": 316, "points": 20},
        "chip_plays": [{"chip_name": "bboost", "num_played": 50000}],
        "overrides": {},
        "some_other_field": "value",
    }

    def test_excludes_nested_keys(self):
        flat = flatten_event(self.EVENT)
        assert "chip_plays" not in flat
        assert "top_element_info" not in flat
        assert "overrides" not in flat

    def test_top_element_points_extracted(self):
        flat = flatten_event(self.EVENT)
        assert flat["top_element_points"] == 20

    def test_chip_plays_json_serialised(self):
        import json
        flat = flatten_event(self.EVENT)
        parsed = json.loads(flat["chip_plays_json"])
        assert parsed[0]["chip_name"] == "bboost"

    def test_none_top_element_info(self):
        event = dict(self.EVENT, top_element_info=None)
        flat = flatten_event(event)
        assert flat["top_element_points"] is None

    def test_empty_chip_plays_gives_none(self):
        event = dict(self.EVENT, chip_plays=[])
        flat = flatten_event(event)
        assert flat["chip_plays_json"] is None

    def test_other_fields_preserved(self):
        flat = flatten_event(self.EVENT)
        assert flat["some_other_field"] == "value"
        assert flat["id"] == 24
