"""Unit tests for the pre-deadline capture gate.

The gate is pure: a list of bootstrap-static events and an instant in, a
decision out. No client, no filesystem.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from fpl_ingest.orchestration.pre_deadline import (
    PRE_DEADLINE_WINDOW,
    in_pre_deadline_window,
    next_deadline,
)

NOW = datetime(2026, 10, 10, 9, 0, tzinfo=timezone.utc)


def _event(id_: int, deadline: datetime | str | None, *, finished: bool = False) -> dict:
    if isinstance(deadline, datetime):
        deadline = deadline.strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"id": id_, "deadline_time": deadline, "finished": finished}


class TestWindowBoundaries:

    def test_window_is_75_minutes(self):
        assert PRE_DEADLINE_WINDOW == timedelta(minutes=75)

    def test_deadline_in_74_minutes_captures(self):
        events = [_event(6, NOW + timedelta(minutes=74))]
        assert in_pre_deadline_window(events, now=NOW) is True

    def test_deadline_in_exactly_75_minutes_captures(self):
        events = [_event(6, NOW + timedelta(minutes=75))]
        assert in_pre_deadline_window(events, now=NOW) is True

    def test_deadline_in_76_minutes_is_a_no_op(self):
        events = [_event(6, NOW + timedelta(minutes=76))]
        assert in_pre_deadline_window(events, now=NOW) is False

    def test_deadline_already_passed_is_a_no_op(self):
        events = [_event(6, NOW - timedelta(minutes=1))]
        assert in_pre_deadline_window(events, now=NOW) is False

    def test_deadline_exactly_now_is_a_no_op(self):
        # Squads lock at deadline_time; a capture at that instant is no longer
        # pre-deadline state.
        events = [_event(6, NOW)]
        assert in_pre_deadline_window(events, now=NOW) is False


class TestNextEventSelection:

    def test_no_events_is_a_no_op(self):
        assert next_deadline([], now=NOW) is None
        assert in_pre_deadline_window([], now=NOW) is False

    def test_all_events_finished_is_a_no_op(self):
        events = [_event(38, NOW + timedelta(minutes=30), finished=True)]
        assert in_pre_deadline_window(events, now=NOW) is False

    def test_missing_or_unparseable_deadline_is_skipped(self):
        events = [_event(6, None), _event(7, "not-a-date"), {"id": 8, "finished": False}]
        assert next_deadline(events, now=NOW) is None

    def test_passed_unfinished_round_does_not_mask_the_next_deadline(self):
        # A round still in progress (unfinished, deadline gone) must not hide
        # the following round's deadline.
        events = [
            _event(5, NOW - timedelta(days=2)),
            _event(6, NOW + timedelta(minutes=60)),
        ]
        assert next_deadline(events, now=NOW) == NOW + timedelta(minutes=60)
        assert in_pre_deadline_window(events, now=NOW) is True

    def test_earliest_future_deadline_wins_regardless_of_order(self):
        events = [
            _event(7, NOW + timedelta(days=7)),
            _event(6, NOW + timedelta(minutes=30)),
        ]
        assert next_deadline(events, now=NOW) == NOW + timedelta(minutes=30)


class TestDeadlineTimeIsUtc:
    """``deadline_time`` is UTC (``Z``); the UK leaves BST on 2026-10-25.

    GW8's deadline is 17:30Z (18:30 BST) and GW9's is 11:00Z (11:00 GMT). The
    gate must compare instants, never wall-clock hours, on both sides.
    """

    @pytest.mark.parametrize(
        ("deadline", "minutes_before", "expected"),
        [
            ("2026-10-23T17:30:00Z", 74, True),   # BST side
            ("2026-10-23T17:30:00Z", 76, False),
            ("2026-10-31T11:00:00Z", 74, True),   # GMT side
            ("2026-10-31T11:00:00Z", 76, False),
        ],
    )
    def test_either_side_of_the_clock_change(self, deadline, minutes_before, expected):
        instant = datetime.fromisoformat(deadline.replace("Z", "+00:00"))
        now = instant - timedelta(minutes=minutes_before)
        assert in_pre_deadline_window([_event(8, deadline)], now=now) is expected

    def test_non_utc_now_compares_as_the_same_instant(self):
        deadline = "2026-10-23T17:30:00Z"
        bst = timezone(timedelta(hours=1))
        now = datetime(2026, 10, 23, 17, 16, tzinfo=bst)  # 16:16Z, 74 min before
        assert in_pre_deadline_window([_event(8, deadline)], now=now) is True
