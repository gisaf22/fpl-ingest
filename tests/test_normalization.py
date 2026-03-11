"""Logic tests — verify transforms and storage are mathematically sound.

These use pure mock data (no HTTP). If ``cost_to_millions`` drifts or
``flatten_live_element`` drops a stat field, these tests catch it before
the modelling stage sees NaNs.
"""

import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from fpl_ingest import (
    SQLiteStore,
    PlayerModel,
    GameweekModel,
    cost_to_millions,
    get_season_id,
    flatten_live_element,
    flatten_live_elements,
    ELEMENT_TYPE_TO_POS,
    POS_TO_ELEMENT_TYPE,
)


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
# Transforms: get_season_id
# ---------------------------------------------------------------------------


class TestGetSeasonId:
    """Season IDs: 2015/16 = 0, 2016/17 = 1, …, 2025/26 = 10."""

    def test_aug_start(self):
        # Aug 2025 → 2025/26 season → id 10
        assert get_season_id(datetime(2025, 8, 1)) == 10

    def test_dec_same_year(self):
        # Dec 2025 → still 2025/26
        assert get_season_id(datetime(2025, 12, 25)) == 10

    def test_jan_next_year(self):
        # Jan 2026 → still 2025/26 (rolls back to 2025)
        assert get_season_id(datetime(2026, 1, 15)) == 10

    def test_may_end(self):
        # May 2026 → still 2025/26
        assert get_season_id(datetime(2026, 5, 30)) == 10

    def test_july_boundary(self):
        # Jul 2026 → still 2025/26 (pre-August)
        assert get_season_id(datetime(2026, 7, 31)) == 10

    def test_first_season(self):
        # 2015/16 = 0
        assert get_season_id(datetime(2015, 9, 1)) == 0

    def test_mid_era(self):
        # 2020/21 = 5
        assert get_season_id(datetime(2021, 3, 1)) == 5


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
            {"id": 1, "web_name": "Salah", "element_type": 3, "now_cost": 130},
            {"id": 2, "web_name": "Haaland", "element_type": 4, "now_cost": 145},
        ]
        inserted, skipped = store.upsert_models("players", PlayerModel, raw)
        assert inserted == 2
        assert skipped == 0

    def test_query_returns_persisted_data(self, store):
        store.register_table("players", PlayerModel)
        store.upsert_models(
            "players",
            PlayerModel,
            [{"id": 1, "web_name": "Salah", "now_cost": 130}],
        )
        rows = store.query("SELECT * FROM players WHERE id = ?", (1,))
        assert len(rows) == 1
        assert rows[0]["web_name"] == "Salah"

    def test_upsert_replaces_on_conflict(self, store):
        store.register_table("players", PlayerModel)
        store.upsert_models(
            "players", PlayerModel,
            [{"id": 1, "web_name": "Salah", "now_cost": 130}],
        )
        store.upsert_models(
            "players", PlayerModel,
            [{"id": 1, "web_name": "Salah", "now_cost": 135}],
        )
        rows = store.query("SELECT now_cost FROM players WHERE id = 1")
        assert rows[0]["now_cost"] == 135

    def test_skips_invalid_rows(self, store):
        store.register_table("players", PlayerModel)
        raw = [
            {"id": 1, "web_name": "Good"},
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

    def test_create_index(self, store):
        store.register_table("players", PlayerModel)
        # Should not raise
        store.create_index("players", ["web_name"])
