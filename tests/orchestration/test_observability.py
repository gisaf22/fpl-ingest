"""Unit tests for the three observability improvements."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from fpl_ingest.cli import build_parser, main, run_status
from fpl_ingest.cli_formatters import _humanize_age, format_status_output
from fpl_ingest.orchestration.runner import (
    _check_stale_freshness,
    _resolve_stale_threshold,
)
from tests.factories import run_row as _make_run


class TestHumanizeAge:
    pytestmark = pytest.mark.unit

    def test_just_now(self):
        dt = datetime.now(timezone.utc) - timedelta(seconds=5)
        assert _humanize_age(dt) == "just now"

    def test_minutes(self):
        dt = datetime.now(timezone.utc) - timedelta(minutes=3)
        assert _humanize_age(dt) == "3 minutes ago"

    def test_one_minute(self):
        dt = datetime.now(timezone.utc) - timedelta(minutes=1)
        assert _humanize_age(dt) == "1 minute ago"

    def test_hours(self):
        dt = datetime.now(timezone.utc) - timedelta(hours=5)
        assert _humanize_age(dt) == "5 hours ago"

    def test_one_hour(self):
        dt = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _humanize_age(dt) == "1 hour ago"

    def test_days(self):
        dt = datetime.now(timezone.utc) - timedelta(days=3)
        assert _humanize_age(dt) == "3 days ago"

    def test_one_day(self):
        dt = datetime.now(timezone.utc) - timedelta(days=1)
        assert _humanize_age(dt) == "1 day ago"


class TestFormatStatusOutput:
    pytestmark = pytest.mark.unit

    def test_no_runs_returns_no_runs_recorded(self):
        out = format_status_output(runs=[], last_successful_run_at=None)
        assert out == "No runs recorded"

    def test_healthy_summary_line_when_recent_run(self):
        recent = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        out = format_status_output(runs=[_make_run(started_at=recent)], last_successful_run_at=recent)
        assert "System healthy" in out

    def test_warning_summary_line_when_stale(self):
        stale = (datetime.now(timezone.utc) - timedelta(hours=30)).isoformat()
        out = format_status_output(runs=[_make_run(started_at=stale)], last_successful_run_at=stale)
        assert "WARNING" in out

    def test_warning_when_last_successful_run_at_is_none(self):
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        out = format_status_output(runs=[_make_run(started_at=recent)], last_successful_run_at=None)
        assert "WARNING" in out

    def test_output_contains_run_data(self):
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        out = format_status_output(runs=[_make_run(started_at=recent, stage="fixtures")], last_successful_run_at=recent)
        assert "fixtures" in out

    def test_multiple_runs_all_appear(self):
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        runs = [_make_run(stage="core"), _make_run(stage="fixtures")]
        out = format_status_output(runs=runs, last_successful_run_at=recent)
        assert "core" in out
        assert "fixtures" in out


class TestStatusNoDB:
    pytestmark = pytest.mark.integration

    def test_no_db_prints_no_runs_recorded_and_exits_0(self, tmp_path, capsys):
        db = tmp_path / "nonexistent.db"
        with pytest.raises(SystemExit) as exc:
            main(["--db", str(db), "status"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        assert "No runs recorded" in captured.out


class TestStatusRunsLimit:
    pytestmark = pytest.mark.unit

    def test_runs_limit_passed_to_query(self, tmp_path):
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute(
            "CREATE TABLE _runs (id INTEGER PRIMARY KEY, started_at TEXT, stage TEXT, "
            "fetched INTEGER DEFAULT 0, validated INTEGER DEFAULT 0, written INTEGER DEFAULT 0, "
            "skipped INTEGER DEFAULT 0, errors INTEGER DEFAULT 0, status TEXT)"
        )
        conn.execute(
            "CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
        )
        for i in range(8):
            conn.execute(
                "INSERT INTO _runs (started_at, stage, fetched, validated, written, skipped, errors, status) "
                "VALUES (?, ?, 0, 0, 0, 0, 0, ?)",
                (f"2026-05-{i+1:02d}T08:00:00+00:00", "core", "success"),
            )
        conn.commit()
        conn.close()

        captured_runs: list = []

        def _fake_format(**kwargs):
            captured_runs.extend(kwargs["runs"])
            return "ok"

        with patch("fpl_ingest.cli.format_status_output", side_effect=_fake_format):
            with pytest.raises(SystemExit):
                main(["--db", str(db), "status", "--runs", "3"])

        assert len(captured_runs) == 3


class TestResolveStaleThreshold:
    pytestmark = pytest.mark.unit

    def test_default_is_26(self):
        args = SimpleNamespace()
        assert _resolve_stale_threshold(args) == 26.0

    def test_cli_arg_takes_priority(self):
        args = SimpleNamespace(stale_after_hours=12.0)
        assert _resolve_stale_threshold(args) == 12.0

    def test_env_var_used_when_no_cli(self, monkeypatch):
        monkeypatch.setenv("FPL_STALE_AFTER_HOURS", "48")
        args = SimpleNamespace()
        assert _resolve_stale_threshold(args) == 48.0

    def test_cli_beats_env_var(self, monkeypatch):
        monkeypatch.setenv("FPL_STALE_AFTER_HOURS", "48")
        args = SimpleNamespace(stale_after_hours=6.0)
        assert _resolve_stale_threshold(args) == 6.0

    def test_invalid_env_var_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("FPL_STALE_AFTER_HOURS", "not-a-number")
        args = SimpleNamespace()
        assert _resolve_stale_threshold(args) == 26.0


class TestCheckStaleFreshness:
    pytestmark = pytest.mark.integration

    def _make_store_with_metadata(self, tmp_path, last_successful_run_at: str | None):
        from fpl_ingest.load.store import SQLiteStore

        store = SQLiteStore(tmp_path / "test.db")
        store.setup_metadata_table()
        if last_successful_run_at is not None:
            store.set_metadata("last_successful_run_at", last_successful_run_at)
        return store

    def test_no_warning_when_run_is_recent(self, tmp_path):
        recent = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        store = self._make_store_with_metadata(tmp_path, recent)
        logger = MagicMock()
        _check_stale_freshness(store, logger, 26.0)
        logger.warning.assert_not_called()

    def test_warning_when_run_is_stale(self, tmp_path):
        stale = (datetime.now(timezone.utc) - timedelta(hours=30)).isoformat()
        store = self._make_store_with_metadata(tmp_path, stale)
        logger = MagicMock()
        _check_stale_freshness(store, logger, 26.0)
        logger.warning.assert_called_once()
        assert "stale" in logger.warning.call_args.args[0]

    def test_no_warning_when_metadata_table_missing(self, tmp_path):
        from fpl_ingest.load.store import SQLiteStore

        store = SQLiteStore(tmp_path / "fresh.db")
        logger = MagicMock()
        _check_stale_freshness(store, logger, 26.0)
        logger.warning.assert_not_called()

    def test_no_warning_when_last_successful_run_at_is_null(self, tmp_path):
        store = self._make_store_with_metadata(tmp_path, None)
        logger = MagicMock()
        _check_stale_freshness(store, logger, 26.0)
        logger.warning.assert_not_called()

    def test_warning_uses_logger_not_print(self, tmp_path, capsys):
        stale = (datetime.now(timezone.utc) - timedelta(hours=50)).isoformat()
        store = self._make_store_with_metadata(tmp_path, stale)
        logger = MagicMock()
        _check_stale_freshness(store, logger, 26.0)
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""
        logger.warning.assert_called_once()
