"""Gate for the pre-deadline bootstrap-static and fixtures capture.

``scheduled_run_pre_deadline.yml`` runs every 30 minutes through the part of
the day FPL deadlines fall in, and relies on this gate to write a capture only
when a transfer deadline is close. The twice-daily run lands ~07:15 and
~19:15 UTC, while deadlines fall between 10:00 and 18:30 UTC, so without this
capture the last player-availability state before a deadline is hours stale.

The decision is made from the bootstrap-static payload the command has just
fetched — the same one it then writes — so there is no marker and no state
between runs. It reuses the retired ``live_window`` module's deadline parsing;
its match-window logic is not coming back (46d3c19).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

#: A capture is written when the next deadline is at most this far away. With
#: the workflow's 30-minute cron, any scheduling lag under 30 minutes (GitHub's
#: observed lag is 11-18) leaves at least two ticks inside the window for every
#: 2026-27 deadline, the last landing 12-30 minutes before it.
PRE_DEADLINE_WINDOW = timedelta(minutes=75)

#: The manifest ``trigger`` value for runs this gate lets through.
PRE_DEADLINE_TRIGGER = "pre_deadline"


def _parse_deadline(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def next_deadline(events: list[Any], *, now: datetime) -> datetime | None:
    """Return the earliest deadline after ``now`` among unfinished events.

    Not simply the first unfinished event: a round still in progress is
    unfinished but its deadline has passed, and it must not hide the next
    round's deadline.
    """
    upcoming = []
    for event in events:
        if not isinstance(event, dict) or event.get("finished") is not False:
            continue
        deadline = _parse_deadline(event.get("deadline_time"))
        if deadline is not None and deadline > now:
            upcoming.append(deadline)
    return min(upcoming) if upcoming else None


def in_pre_deadline_window(events: list[Any], *, now: datetime) -> bool:
    """Return True if the next deadline is within ``PRE_DEADLINE_WINDOW`` of ``now``."""
    deadline = next_deadline(events, now=now)
    return deadline is not None and deadline - now <= PRE_DEADLINE_WINDOW
