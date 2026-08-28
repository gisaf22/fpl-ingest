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
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from fpl_ingest.extract.http.sync_client import FPLClient

#: Kickoff to full-time is ~105 minutes (90 + stoppage); this pads for
#: halftime and any delay before the FPL API flips a fixture's "finished".
WINDOW_MINUTES = 150


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


def main() -> int:
    """Exit 0 if a match is currently live, 1 otherwise.

    On fetch failure, fails open (exit 0) so the live workflow still runs the
    full fetch rather than silently going dark on a match day.
    """
    client = FPLClient()
    fixtures = client.get_fixtures()
    if fixtures is None:
        print("check-live-window: fixtures fetch failed; assuming live", file=sys.stderr)
        return 0
    live = is_match_live(fixtures)
    print(f"check-live-window: {'live' if live else 'no live match'}")
    return 0 if live else 1


if __name__ == "__main__":
    sys.exit(main())
