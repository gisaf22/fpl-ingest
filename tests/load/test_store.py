"""Tests for load/store.py."""

import sqlite3

import pytest

pytestmark = pytest.mark.integration

from fpl_ingest.schema import compile_contract
from fpl_ingest.transform.models import GameweekModel, PlayerHistoryModel, PlayerModel
from fpl_ingest.orchestration.run_status import RUN_STATUS_FAILED, RUN_STATUS_SUCCESS
from fpl_ingest.load.store import SQLiteStore
from tests.factories import player_row as _player_row, store_history_row as _history_row


def _contract_table(name: str):
    return compile_contract().tables[name]


class TestSQLiteStore:
    """Verify the storage layer persists and retrieves data correctly."""

    @pytest.fixture
    def store(self, tmp_path):
        return SQLiteStore(tmp_path / "test.db")

    def test_register_and_upsert_players(self, store):
        store.register_contract_table(_contract_table("players"))
        raw = [
            _player_row(id=1, web_name="Salah", team=11, element_type=3, now_cost=130),
            _player_row(id=2, web_name="Haaland", team=13, element_type=4, now_cost=145,
                        team_code=43, code=223094),
        ]
        inserted, skipped = store.upsert_models("players", PlayerModel, raw)
        assert inserted == 2
        assert skipped == 0

    def test_query_returns_persisted_data(self, store):
        store.register_contract_table(_contract_table("players"))
        store.upsert_models("players", PlayerModel, [_player_row(id=1, web_name="Salah")])
        rows = store.query("SELECT * FROM players WHERE id = ?", (1,))
        assert len(rows) == 1
        assert rows[0]["web_name"] == "Salah"

    def test_upsert_replaces_on_conflict(self, store):
        store.register_contract_table(_contract_table("players"))
        store.upsert_models("players", PlayerModel, [_player_row(id=1, now_cost=130)])
        store.upsert_models("players", PlayerModel, [_player_row(id=1, now_cost=135)])
        rows = store.query("SELECT now_cost FROM players WHERE id = 1")
        assert rows[0]["now_cost"] == 135

    def test_skips_invalid_rows(self, store):
        store.register_contract_table(_contract_table("players"))
        raw = [
            _player_row(id=1, web_name="Good", team=1, element_type=3, now_cost=55),
            {"web_name": "NoId"},  # missing required 'id'
        ]
        inserted, skipped = store.upsert_models("players", PlayerModel, raw)
        assert inserted == 1
        assert skipped == 1

    def test_gameweek_unique_constraint(self, store):
        store.register_contract_table(_contract_table("gameweeks"))
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
        store.register_contract_table(_contract_table("player_histories"))
        rows = [
            _history_row(fixture=101, round=5, minutes=90, total_points=8),
            _history_row(fixture=102, round=5, minutes=45, total_points=4),
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
        store.register_contract_table(_contract_table("player_histories"))
        original = _history_row(round=5, total_points=8)
        replayed = _history_row(round=5, total_points=10)

        store.upsert_models("player_histories", PlayerHistoryModel, [original])
        store.upsert_models("player_histories", PlayerHistoryModel, [replayed])

        rows = store.query(
            "SELECT total_points FROM player_histories "
            "WHERE element_id = 1 AND round = 5 AND fixture = 101"
        )
        assert len(rows) == 1
        assert rows[0]["total_points"] == 10

    def test_contract_indexes_are_created(self, store):
        store.register_contract_table(_contract_table("fixture_stats"))
        indexes = store.query("PRAGMA index_list(fixture_stats)")
        assert any(row["name"] == "idx_fixture_stats_element" for row in indexes)

    def test_runs_table_persists_final_status(self, store):
        store.setup_runs_table()
        store.record_run("2026-04-21T00:00:00+00:00", "core", 10, 10, 10, 0, 0)
        store.finalize_run("2026-04-21T00:00:00+00:00", errors=0, skipped=0, strict_mode=False)

        rows = store.query("SELECT stage, status FROM _runs WHERE started_at = ?", ("2026-04-21T00:00:00+00:00",))
        assert rows == [{"stage": "core", "status": RUN_STATUS_SUCCESS}]

    def test_stage_lineage_records_artifacts_and_output_tables(self, store):
        store.setup_runs_table()
        store.record_stage_lineage(
            "2026-04-21T00:00:00+00:00",
            "fixtures",
            artifact_paths=("raw/fixtures.json",),
            output_tables=("fixtures", "fixture_stats"),
        )

        rows = store.query(
            "SELECT stage, artifact_path, output_table FROM _stage_lineage ORDER BY output_table"
        )
        assert rows == [
            {"stage": "fixtures", "artifact_path": "raw/fixtures.json", "output_table": "fixture_stats"},
            {"stage": "fixtures", "artifact_path": "raw/fixtures.json", "output_table": "fixtures"},
        ]

    def test_finalize_run_is_atomic_with_metadata_updates(self, store):
        store.setup_runs_table()
        store.record_run("2026-04-21T00:00:00+00:00", "core", 10, 10, 10, 0, 0)

        # Create _metadata with a wrong schema so set_metadata fails mid-transaction.
        # This proves that when the metadata write fails, the run-status update also
        # rolls back — the transaction is truly atomic.
        conn = sqlite3.connect(store.db_path)
        conn.execute("CREATE TABLE _metadata (key TEXT PRIMARY KEY)")  # missing value, updated_at
        conn.commit()
        conn.close()

        with pytest.raises(sqlite3.OperationalError):
            store.finalize_run(
                "2026-04-21T00:00:00+00:00",
                errors=0,
                skipped=0,
                strict_mode=False,
                metadata_updates={"last_successful_run_at": "2026-04-21T00:00:00+00:00"},
            )

        # Both the status update and the metadata write must have rolled back.
        rows = store.query("SELECT stage, status FROM _runs WHERE started_at = ?", ("2026-04-21T00:00:00+00:00",))
        assert rows == [{"stage": "core", "status": None}]

    def test_store_writes_are_deterministic_after_orchestration_failure_state(self, tmp_path):
        store = SQLiteStore(tmp_path / "failed.db")
        store.setup_runs_table()
        store.setup_metadata_table()
        store.register_contract_table(_contract_table("players"))
        store.record_run("2026-04-21T00:00:00+00:00", "core", 1, 1, 1, 0, 1)

        inserted, skipped = store.upsert_models("players", PlayerModel, [_player_row(id=1, web_name="Salah")])
        store.finalize_run(
            "2026-04-21T00:00:00+00:00",
            errors=1,
            skipped=0,
            strict_mode=False,
            metadata_updates={"last_successful_run_at": "2026-04-21T00:00:00+00:00"},
        )

        assert (inserted, skipped) == (1, 0)
        assert len(store.query("SELECT * FROM players")) == 1
        assert store.query("SELECT stage, status FROM _runs") == [{"stage": "core", "status": RUN_STATUS_FAILED}]
        assert len(store.query("SELECT * FROM _metadata")) == 1
