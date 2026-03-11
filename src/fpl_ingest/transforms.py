"""Generic FPL data transformations and constants.

Position mappings, cost conversions, season ID calculation, and
live-data flattening that are not specific to any consuming project.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

# Position mapping: element_type → code
ELEMENT_TYPE_TO_POS = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}
POS_TO_ELEMENT_TYPE = {"GKP": 1, "DEF": 2, "MID": 3, "FWD": 4}


def cost_to_millions(now_cost: int) -> float:
    """Convert FPL ``now_cost`` (tenths) to millions."""
    return now_cost / 10.0


def get_season_id(reference_date: datetime | None = None) -> int:
    """Calculate FPL season ID from a date.

    Season IDs: 2015/16 = 0, 2016/17 = 1, …, 2025/26 = 10.
    Season runs Aug–May: Aug–Dec uses current year, Jan–Jul previous year.
    """
    dt = reference_date or datetime.now()
    start_year = dt.year if dt.month >= 8 else dt.year - 1
    return start_year - 2015


def flatten_live_element(element: Dict[str, Any], gw: int) -> Dict[str, Any]:
    """Flatten a live-endpoint element into a dict suitable for GameweekModel.

    The FPL live endpoint returns elements as ``{"id": 123, "stats": {...}}``.
    This unpacks the nested stats and adds the required ``element_id`` and
    ``round`` keys so the result can be validated directly::

        flat = flatten_live_element(raw_element, gw=24)
        entry = GameweekModel.model_validate(flat)

    Returns:
        Flat dict with ``element_id``, ``round``, and all stat fields.

    Raises:
        ValueError: If the element has no ``id``.
    """
    player_id = element.get("id")
    if player_id is None:
        raise ValueError("Element is missing 'id'")
    stats = element.get("stats", {})
    return {"element_id": player_id, "round": gw, **stats}


def flatten_live_elements(
    elements: List[Dict[str, Any]], gw: int
) -> List[Dict[str, Any]]:
    """Flatten a list of live elements, skipping those without an id."""
    results = []
    for elem in elements:
        if elem.get("id") is None:
            continue
        results.append(flatten_live_element(elem, gw))
    return results
