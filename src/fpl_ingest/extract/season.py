"""Derive a run's season from its own bootstrap-static capture (#62 D5, D6).

The season is ``Y-(Y+1 mod 100)``, where Y is the UTC deadline year of the
lowest-id event carrying a parseable ``deadline_time`` in a shape-valid
bootstrap. A June/July payload still lists last season's events, so it still
names last season — the rule never consults the clock.

There is no fallback. Without a usable bootstrap the season is None and the
run logs ``season_source=none`` at ERROR; the capture itself still happens.
Reading a previous run's manifest instead would break the write-only
``RawStorageBackend`` invariant.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

SEASON_SOURCE_BOOTSTRAP = "bootstrap"
SEASON_SOURCE_NONE = "none"


def resolve_season(
    payload: Any, *, shape_ok: bool, logger: logging.Logger
) -> str | None:
    """Return the run's season from a bootstrap-static payload, logging the source.

    Args:
        payload: The decoded bootstrap-static body, or None when it was not
            fetched.
        shape_ok: Whether that payload passed its shape check.
        logger: Where the ``season_source=`` line goes.
    """
    year = _first_deadline_year(payload) if shape_ok else None
    if year is None:
        logger.error(
            "season_source=%s season=null: no usable bootstrap-static in this run",
            SEASON_SOURCE_NONE,
        )
        return None
    season = f"{year}-{(year + 1) % 100:02d}"
    logger.info("season_source=%s season=%s", SEASON_SOURCE_BOOTSTRAP, season)
    return season


def _first_deadline_year(payload: Any) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    events = payload.get("events")
    if not isinstance(events, list):
        return None
    dated = [
        (event["id"], deadline)
        for event in events
        if isinstance(event, Mapping)
        and isinstance(event.get("id"), int)
        and (deadline := _parse_deadline(event.get("deadline_time"))) is not None
    ]
    if not dated:
        return None
    _, first_deadline = min(dated, key=lambda pair: pair[0])
    return first_deadline.year


def _parse_deadline(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        # Python 3.10's fromisoformat does not accept a trailing "Z".
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
