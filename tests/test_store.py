"""Tests for store.py.

Covers SQLiteStore round-trips, upsert behavior, conflict resolution,
schema evolution (column migration), and index creation.
"""

import sqlite3

import pytest

pytestmark = pytest.mark.unit

from fpl_ingest.models import GameweekModel, PlayerHistoryModel, PlayerModel
from fpl_ingest.store import SQLiteStore


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
            unique_constraint=GameweekModel.GRAIN_CONSTRAINT,
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
            unique_constraint=PlayerHistoryModel.GRAIN_CONSTRAINT,
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
            unique_constraint=PlayerHistoryModel.GRAIN_CONSTRAINT,
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
