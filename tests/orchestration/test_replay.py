"""Tests for the replay pipeline command."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from fpl_ingest.cli import build_parser, main
from fpl_ingest.orchestration.replay import (
    ReplayError,
    replay_core_stage,
    replay_fixtures_stage,
    replay_from_cache,
    replay_gameweeks_stage,
    replay_player_histories_stage,
)
from fpl_ingest.orchestration.stage_result import StageResult
from tests.factories import fixture_row, history_row, player_row, team_row


_BOOTSTRAP = {
    "events": [],
    "elements": [player_row(id=1, team=11, element_type=3, now_cost=130)],
    "teams": [team_row(id=11)],
    "element_types": [],
}

_FIXTURES = [fixture_row(id=101, event=1, team_h=11, team_a=7)]

_GW_DATA = {
    "elements": [
        {
            "id": 1,
            "stats": {
                "minutes": 90, "goals_scored": 1, "assists": 0, "clean_sheets": 0,
                "goals_conceded": 1, "own_goals": 0, "penalties_saved": 0,
                "penalties_missed": 0, "yellow_cards": 0, "red_cards": 0, "saves": 0,
                "bonus": 2, "bps": 28, "influence": "42.0", "creativity": "18.0",
                "threat": "55.0", "ict_index": "11.5", "starts": 1,
                "expected_goals": "0.62", "expected_assists": "0.14",
                "expected_goal_involvements": "0.76", "expected_goals_conceded": "1.10",
                "total_points": 8, "in_dreamteam": False,
            },
        }
    ]
}

_PLAYER_HISTORY = {"history": [history_row(element=1, fixture=101, round=1)], "history_past": []}


def _write_bootstrap(raw_dir: Path) -> None:
    (raw_dir / "bootstrap.json").write_text(json.dumps(_BOOTSTRAP), encoding="utf-8")


def _write_fixtures(raw_dir: Path) -> None:
    (raw_dir / "fixtures.json").write_text(json.dumps(_FIXTURES), encoding="utf-8")


def _write_gw(raw_dir: Path, gw_id: int = 1) -> None:
    (raw_dir / f"gw_{gw_id}.json").write_text(json.dumps(_GW_DATA), encoding="utf-8")


def _write_player_history(raw_dir: Path, player_id: int = 1) -> None:
    players_dir = raw_dir / "players"
    players_dir.mkdir(parents=True, exist_ok=True)
    (players_dir / f"{player_id}.json").write_text(
        json.dumps(_PLAYER_HISTORY), encoding="utf-8"
    )


class _RecordingStore:
    def __init__(self) -> None:
        self.stage_results: list[tuple[str, StageResult]] = []
        self.finalize_calls: list[str] = []
        self._registered_tables: dict = {}

    @contextmanager
    def transaction(self):
        yield

    def register_contract_table(self, table) -> None:
        self._registered_tables[table.name] = table

    def setup_runs_table(self) -> None:
        pass

    def setup_metadata_table(self) -> None:
        pass

    def upsert_models(self, table_name, schema, raw_dicts, **kwargs):
        return (len(raw_dicts), 0)

    def record_stage_result(self, started_at: str, result: StageResult) -> None:
        self.stage_results.append((started_at, result))

    def finalize_run(self, started_at, status=None, *, errors=0, skipped=0, strict_mode=False, metadata_updates=None):
        from fpl_ingest.orchestration.run_status import classify_run
        resolved = status or classify_run(errors=errors, skipped=skipped, strict_mode=strict_mode)
        self.finalize_calls.append(resolved)
        return resolved

    def set_metadata(self, key, value):
        pass


class TestReplayError:
    pytestmark = pytest.mark.unit

    def test_missing_raw_dir_raises(self, tmp_path):
        store = _RecordingStore()
        import logging
        with pytest.raises(ReplayError, match="does not exist"):
            replay_from_cache(tmp_path / "no_such_dir", store, logging.getLogger(), strict=False)

    def test_empty_raw_dir_raises(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        store = _RecordingStore()
        import logging
        with pytest.raises(ReplayError, match="no recognised cache files"):
            replay_from_cache(raw_dir, store, logging.getLogger(), strict=False)


class TestReplayCoreStage:
    pytestmark = pytest.mark.unit

    def test_missing_bootstrap_returns_error_result(self, tmp_path):
        store = _RecordingStore()
        outcome = replay_core_stage(store, tmp_path)
        result = outcome.result
        assert outcome.output is None
        assert result.errors == 1
        assert result.stage == "core"

    def test_valid_bootstrap_produces_clean_result(self, tmp_path):
        _write_bootstrap(tmp_path)
        store = _RecordingStore()
        outcome = replay_core_stage(store, tmp_path)
        result = outcome.result
        assert outcome.output is not None
        assert result.errors == 0
        assert result.fetched > 0


class TestReplayFixturesStage:
    pytestmark = pytest.mark.unit

    def test_missing_fixtures_returns_error_result(self, tmp_path):
        store = _RecordingStore()
        result = replay_fixtures_stage(store, tmp_path).result
        assert result.errors == 1
        assert result.stage == "fixtures"

    def test_valid_fixtures_produces_result(self, tmp_path):
        _write_fixtures(tmp_path)
        store = _RecordingStore()
        result = replay_fixtures_stage(store, tmp_path).result
        assert result.errors == 0
        assert result.fetched >= 1


class TestReplayGameweeksStage:
    pytestmark = pytest.mark.unit

    def test_no_gw_files_returns_empty_result(self, tmp_path):
        store = _RecordingStore()
        result = replay_gameweeks_stage(store, tmp_path).result
        assert result.stage == "gameweeks"
        assert result.fetched == 0
        assert result.errors == 0

    def test_valid_gw_file_produces_result(self, tmp_path):
        _write_gw(tmp_path, gw_id=1)
        store = _RecordingStore()
        result = replay_gameweeks_stage(store, tmp_path).result
        assert result.stage == "gameweeks"
        assert result.fetched >= 1


class TestReplayPlayerHistoriesStage:
    pytestmark = pytest.mark.unit

    def test_missing_players_dir_returns_empty_result(self, tmp_path):
        store = _RecordingStore()
        result = replay_player_histories_stage(store, tmp_path).result
        assert result.stage == "player_histories"
        assert result.fetched == 0

    def test_valid_player_file_produces_result(self, tmp_path):
        _write_player_history(tmp_path, player_id=1)
        store = _RecordingStore()
        result = replay_player_histories_stage(store, tmp_path).result
        assert result.stage == "player_histories"
        assert result.fetched >= 1


class TestReplayOrchestration:
    pytestmark = pytest.mark.unit

    def test_full_replay_produces_four_stage_results(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_bootstrap(raw_dir)
        _write_fixtures(raw_dir)
        _write_gw(raw_dir, gw_id=1)
        _write_player_history(raw_dir, player_id=1)

        store = _RecordingStore()
        import logging
        result = replay_from_cache(raw_dir, store, logging.getLogger(), strict=False)

        stages = [r.stage for _, r in store.stage_results]
        assert "core" in stages
        assert "fixtures" in stages
        assert "gameweeks" in stages
        assert "player_histories" in stages
        assert len(store.finalize_calls) == 1

    def test_replay_returns_zero_on_clean_run(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_bootstrap(raw_dir)
        _write_fixtures(raw_dir)

        store = _RecordingStore()
        import logging
        exit_code = replay_from_cache(raw_dir, store, logging.getLogger(), strict=False)

        assert exit_code == 0

    def test_strict_mode_aborts_on_error_stage(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_fixtures(raw_dir)

        store = _RecordingStore()
        import logging
        exit_code = replay_from_cache(raw_dir, store, logging.getLogger(), strict=True)

        assert exit_code == 1
        stages = [r.stage for _, r in store.stage_results]
        assert "core" in stages
        assert "fixtures" not in stages


class TestReplayCLI:
    pytestmark = pytest.mark.unit

    def test_replay_missing_raw_dir_exits_one(self, tmp_path):
        db = tmp_path / "test.db"
        raw = tmp_path / "no_such_raw"
        from unittest.mock import patch
        with patch("fpl_ingest.cli.SQLiteStore"):
            with pytest.raises(SystemExit) as exc:
                main(["--db", str(db), "replay", "--raw-dir", str(raw)])
        assert exc.value.code == 1


class TestStrictReplayMetadata:
    pytestmark = pytest.mark.integration

    def test_strict_abort_does_not_write_last_successful_run_at(self, tmp_path):
        import logging
        from fpl_ingest.load.db_setup import setup_store
        from fpl_ingest.load.store import SQLiteStore

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_fixtures(raw_dir)

        store = SQLiteStore(tmp_path / "test.db")
        with store.transaction():
            setup_store(store)

        exit_code = replay_from_cache(raw_dir, store, logging.getLogger(), strict=True)

        assert exit_code == 1
        meta = store.query("SELECT key FROM _metadata WHERE key = 'last_successful_run_at'")
        assert meta == [], "last_successful_run_at must not be written after a strict replay abort"
