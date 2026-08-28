"""Unit tests for the scheduled_run_live.yml match-window check."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from fpl_ingest.live_window import WINDOW_MINUTES, is_match_live

pytestmark = pytest.mark.unit

_KICKOFF = datetime(2026, 8, 24, 14, 0, 0, tzinfo=timezone.utc)


def _fixture(**overrides: object) -> dict:
    base = {"id": 1, "kickoff_time": "2026-08-24T14:00:00Z", "finished": False}
    base.update(overrides)
    return base


def test_no_fixtures_is_not_live() -> None:
    assert is_match_live([], now=_KICKOFF) is False


def test_before_kickoff_is_not_live() -> None:
    fixtures = [_fixture()]
    assert is_match_live(fixtures, now=_KICKOFF - timedelta(minutes=1)) is False


def test_within_window_is_live() -> None:
    fixtures = [_fixture()]
    assert is_match_live(fixtures, now=_KICKOFF + timedelta(minutes=60)) is True


def test_after_window_is_not_live() -> None:
    fixtures = [_fixture()]
    now = _KICKOFF + timedelta(minutes=WINDOW_MINUTES + 1)
    assert is_match_live(fixtures, now=now) is False


def test_finished_fixture_is_not_live_even_inside_window() -> None:
    fixtures = [_fixture(finished=True)]
    assert is_match_live(fixtures, now=_KICKOFF + timedelta(minutes=60)) is False


def test_fixture_missing_kickoff_time_is_skipped() -> None:
    fixtures = [_fixture(kickoff_time=None)]
    assert is_match_live(fixtures, now=_KICKOFF) is False


def test_one_live_fixture_among_others_is_live() -> None:
    fixtures = [
        _fixture(id=1, kickoff_time="2026-08-23T14:00:00Z", finished=True),
        _fixture(id=2, kickoff_time="2026-08-24T14:00:00Z", finished=False),
    ]
    assert is_match_live(fixtures, now=_KICKOFF + timedelta(minutes=10)) is True
