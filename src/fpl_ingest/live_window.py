"""Match-window check used to gate the 30-minute scheduled_run_live.yml.

GitHub Actions cron has no native "only while a match is on" schedule, so
``scheduled_run_live.yml`` runs every 30 minutes year-round and relies on this
module to no-op outside match windows rather than fetching event-live around
the clock.

Fetches fixtures directly from the public, unauthenticated FPL API rather
than reading back the most recently ingested fixtures object from S3: the
IAM role backing the workflow grants ``s3:PutObject``/``s3:GetObject`` only,
not ``s3:ListBucket``, so there is no way to discover the latest run's
``run_id``-keyed key without listing. A live fetch needs no credentials and
is always current.

Deadline-day dead zone: between a gameweek's transfer deadline and 5 minutes
before its first kickoff, squads are locked and no match has started, so
``is_match_live`` is already ``False`` throughout that stretch and nothing
extra needs to suppress a pull there. Two moments in that otherwise-dead
stretch are still worth a pull — T-10min before the deadline (last chance to
catch late transfers/price changes before lock) and T-5min before the first
kickoff (catches late team news/postponements) — so ``is_deadline_day_pull``
adds those two narrow exceptions back in, ORed with ``is_match_live`` in
``main``. It reads ``events`` (bootstrap-static, for ``deadline_time``) the
same live-fetch-from-API way this module already reads fixtures.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from fpl_ingest.extract.http.sync_client import FPLClient
from fpl_ingest.extract.http.sync_http import FPLClientError

#: Kickoff to full-time is ~105 minutes (90 + stoppage); this pads for
#: halftime and any delay before the FPL API flips a fixture's "finished".
WINDOW_MINUTES = 150

#: How long before deadline_time the "last chance" exception pull targets.
DEADLINE_LEAD_MINUTES = 10

#: How long before first kickoff the "late team news" exception pull targets;
#: also where the dead zone itself ends.
KICKOFF_LEAD_MINUTES = 5

#: Half-width of the window around each exception's target instant. The live
#: workflow's cron only ticks every 30 minutes, so a pad of half that
#: interval guarantees the nearest tick to any target instant falls inside it.
EXCEPTION_PAD_MINUTES = 15


def is_match_live(fixtures: list[dict[str, Any]], *, now: datetime | None = None) -> bool:
    """Return True if any fixture's kickoff window currently contains ``now``.

    A fixture already marked ``finished`` is skipped regardless of how its
    kickoff time compares to the window, so a match that wraps up early
    doesn't keep the window open until the padded cutoff.
    """
    now = now or datetime.now(timezone.utc)
    for fixture in fixtures:
        if fixture.get("finished"):
            continue
        kickoff_raw = fixture.get("kickoff_time")
        if not kickoff_raw:
            continue
        kickoff = datetime.fromisoformat(str(kickoff_raw).replace("Z", "+00:00"))
        window_end = kickoff + timedelta(minutes=WINDOW_MINUTES)
        if kickoff <= now <= window_end:
            return True
    return False


def _first_kickoff(
    fixtures: list[dict[str, Any]], event_id: Any
) -> datetime | None:
    """Return the earliest kickoff among ``fixtures`` belonging to ``event_id``."""
    kickoffs = []
    for fixture in fixtures:
        if fixture.get("event") != event_id:
            continue
        kickoff_raw = fixture.get("kickoff_time")
        if not kickoff_raw:
            continue
        kickoffs.append(datetime.fromisoformat(str(kickoff_raw).replace("Z", "+00:00")))
    return min(kickoffs) if kickoffs else None


def is_deadline_day_pull(
    events: list[dict[str, Any]],
    fixtures: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> bool:
    """Return True if ``now`` falls in one of the two deadline-day exception windows.

    For every gameweek with a known ``deadline_time`` and at least one
    fixture, checks whether ``now`` is near T-``DEADLINE_LEAD_MINUTES``
    before that deadline, or T-``KICKOFF_LEAD_MINUTES`` before its earliest
    kickoff. Both targets otherwise fall inside the dead zone (deadline to
    5 minutes before kickoff), where ``is_match_live`` is always False since
    the dead zone ends before any kickoff — so this is purely additive.
    """
    now = now or datetime.now(timezone.utc)
    pad = timedelta(minutes=EXCEPTION_PAD_MINUTES)
    for event in events:
        deadline_raw = event.get("deadline_time")
        if not deadline_raw:
            continue
        deadline = datetime.fromisoformat(str(deadline_raw).replace("Z", "+00:00"))
        kickoff = _first_kickoff(fixtures, event.get("id"))
        if kickoff is None:
            continue
        pre_deadline_target = deadline - timedelta(minutes=DEADLINE_LEAD_MINUTES)
        pre_kickoff_target = kickoff - timedelta(minutes=KICKOFF_LEAD_MINUTES)
        if abs(now - pre_deadline_target) <= pad:
            return True
        if abs(now - pre_kickoff_target) <= pad:
            return True
    return False


def main() -> int:
    """Exit 0 if a match is currently live or a deadline-day exception window is open, 1 otherwise.

    On fixtures-fetch failure, fails open (exit 0) so the live workflow still
    runs the full fetch rather than silently going dark on a match day. A
    bootstrap-static fetch failure (needed only for the deadline-day check)
    does not fail open the same way — it just means that check contributes no
    extra pull windows this run; the match-window check still applies.
    """
    client = FPLClient()
    fixtures = client.get_fixtures()
    if fixtures is None:
        print("check-live-window: fixtures fetch failed; assuming live", file=sys.stderr)
        return 0

    live = is_match_live(fixtures)
    deadline_pull = False
    if not live:
        try:
            events = client.get_bootstrap().get("events", [])
        except FPLClientError as exc:
            print(f"check-live-window: bootstrap fetch failed ({exc}); skipping deadline-day check", file=sys.stderr)
            events = []
        deadline_pull = is_deadline_day_pull(events, fixtures)

    reason = "live" if live else "deadline-day pull window" if deadline_pull else "no live match"
    live = live or deadline_pull
    print(f"check-live-window: {reason}")
    return 0 if live else 1


if __name__ == "__main__":
    sys.exit(main())
