"""Unit tests for status-output formatting.

The ``status`` CLI sub-command and its SQLite-backed freshness checks
(``_check_stale_freshness``, ``_resolve_stale_threshold``, ``cli.run_status``)
were removed along with the SQLite run audit trail they depended on — see the
NOTE in ``fpl_ingest.cli`` and ``fpl_ingest.orchestration.runner``. What
remains here are the pure formatting helpers, which take already-assembled
data and have no store dependency.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from fpl_ingest.cli_formatters import _humanize_age, format_status_output
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
