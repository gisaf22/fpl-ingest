"""ICT readiness: whether a gameweek's ratification-only fields have been published.

Shared by ``gameweeks.py`` and ``element_summary.py``, whose settlement markers
are write-once: once written, that stage never re-fetches the gameweek. Both
markers are triggered by event-status reporting the gameweek ratified, which
assumes FPL has also populated ``influence`` / ``creativity`` / ``threat`` /
``ict_index`` by then. Every pre-ratification capture of 2026-27 GW2-5 carried
0.0 in all four for every player who played, and whether ICT can lag
``bonus_added`` inside the 12h gap between runs is unobservable. A marker
written in that gap would freeze zero ICT permanently, so each stage checks
its own captured rows with :func:`ict_ready` before writing its marker.

This gates a marker, not a capture: the payload is written either way and its
``shape_validation`` is untouched (strategy doc B.2 keeps value checks out of
the raw-capture verdict).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any

#: The ratification-only fields FPL leaves at "0.0" until it publishes them.
ICT_FIELDS = ("influence", "creativity", "threat", "ict_index")

#: Manifest ``markers_withheld`` reason when :func:`ict_ready` returns False.
ICT_NOT_READY_REASON = (
    "ict_not_ready: no row with minutes > 0 has a nonzero influence/creativity/"
    "threat/ict_index, or a row's minutes or ICT value is missing or unparseable"
)


def ict_ready(rows: Iterable[Any]) -> bool:
    """Whether one gameweek's per-player rows carry published ICT values.

    ``rows`` are the stat mappings for the gameweek being marked: event-live
    ``elements[].stats``, or element-summary ``history[]`` rows whose ``round``
    is that gameweek.

    * If any row has ``minutes > 0``, at least one such row must have a
      nonzero value in one of the four ICT fields — "at least one", not
      "all": real ratified data has legitimate all-zero ICT cameos.
    * If no row has ``minutes > 0``, the result is vacuously True.
    * Numeric strings (FPL sends ICT as ``"5.6"``) are parsed. A row whose
      ``minutes`` is missing or unparseable, or a played row with a missing or
      unparseable ICT field, makes the whole set not ready: it is not read as
      zero, because withholding a marker only costs a retry next run while
      writing one wrongly is permanent.
    """
    played = False
    populated = False
    for row in rows:
        if not isinstance(row, Mapping):
            return False
        minutes = _number(row.get("minutes"))
        if minutes is None:
            return False
        if minutes <= 0:
            continue
        played = True
        values = [_number(row.get(field)) for field in ICT_FIELDS]
        if any(value is None for value in values):
            return False
        if any(value != 0 for value in values):
            populated = True
    return populated or not played


def _number(value: Any) -> float | None:
    """Parse an int, float, or numeric string; None for anything else."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None
