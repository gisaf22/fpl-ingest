"""Unit tests for the scheduled_run_live.yml match-window check."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from fpl_ingest.live_window import (
    DEADLINE_LEAD_MINUTES,
    EXCEPTION_PAD_MINUTES,
    KICKOFF_LEAD_MINUTES,
    WINDOW_MINUTES,
    is_deadline_day_pull,
    is_match_live,
)

pytestmark = pytest.mark.unit

_KICKOFF = datetime(2026, 8, 24, 14, 0, 0, tzinfo=timezone.utc)
_DEADLINE = datetime(2026, 8, 22, 18, 30, 0, tzinfo=timezone.utc)


def _event(**overrides: object) -> dict:
    base = {"id": 1, "deadline_time": "2026-08-22T18:30:00Z"}
    base.update(overrides)
    return base


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


# ---------------------------------------------------------------------------
# Deadline-day dead zone
# ---------------------------------------------------------------------------


def test_squarely_inside_dead_zone_does_not_pull() -> None:
    """Midway between deadline and kickoff, well clear of both exceptions."""
    events = [_event()]
    fixtures = [_fixture(event=1)]
    now = _DEADLINE + (_KICKOFF - _DEADLINE) / 2
    assert is_deadline_day_pull(events, fixtures, now=now) is False
    assert is_match_live(fixtures, now=now) is False


def test_pre_deadline_exception_pulls_at_the_exact_boundary() -> None:
    events = [_event()]
    fixtures = [_fixture(event=1)]
    now = _DEADLINE - timedelta(minutes=DEADLINE_LEAD_MINUTES)
    assert is_deadline_day_pull(events, fixtures, now=now) is True


def test_pre_kickoff_exception_pulls_at_the_exact_boundary() -> None:
    events = [_event()]
    fixtures = [_fixture(event=1)]
    now = _KICKOFF - timedelta(minutes=KICKOFF_LEAD_MINUTES)
    assert is_deadline_day_pull(events, fixtures, now=now) is True


def test_pre_deadline_exception_holds_across_its_pad() -> None:
    events = [_event()]
    fixtures = [_fixture(event=1)]
    target = _DEADLINE - timedelta(minutes=DEADLINE_LEAD_MINUTES)
    for offset in (-EXCEPTION_PAD_MINUTES, EXCEPTION_PAD_MINUTES):
        now = target + timedelta(minutes=offset)
        assert is_deadline_day_pull(events, fixtures, now=now) is True


def test_just_outside_the_exception_pad_does_not_pull() -> None:
    events = [_event()]
    fixtures = [_fixture(event=1)]
    target = _DEADLINE - timedelta(minutes=DEADLINE_LEAD_MINUTES)
    now = target - timedelta(minutes=EXCEPTION_PAD_MINUTES, seconds=1)
    assert is_deadline_day_pull(events, fixtures, now=now) is False


def test_non_deadline_day_is_unaffected() -> None:
    """No deadline_time / no matching fixture -> normal cadence, untouched."""
    events: list[dict] = [{"id": 1, "deadline_time": None}]
    fixtures = [_fixture()]
    assert is_deadline_day_pull(events, fixtures, now=_KICKOFF - timedelta(days=3)) is False
    # Existing match-window behavior is unaffected by the new check existing.
    assert is_match_live(fixtures, now=_KICKOFF + timedelta(minutes=10)) is True


def test_event_with_no_fixtures_is_skipped() -> None:
    events = [_event(id=99, deadline_time="2026-08-22T18:30:00Z")]
    fixtures = [_fixture(event=1)]  # no fixture for event 99
    now = datetime(2026, 8, 22, 18, 20, 0, tzinfo=timezone.utc)
    assert is_deadline_day_pull(events, fixtures, now=now) is False
