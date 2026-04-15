"""Generic FPL data transformations and constants.

Position mappings, cost conversions, season ID calculation, and
live-data flattening that are not specific to any consuming project.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from fpl_ingest.types import JSON

# Position mapping: element_type → code
ELEMENT_TYPE_TO_POS = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}
POS_TO_ELEMENT_TYPE = {"GKP": 1, "DEF": 2, "MID": 3, "FWD": 4}


def cost_to_millions(now_cost: int) -> float:
    """Convert FPL ``now_cost`` (tenths) to millions."""
    return now_cost / 10.0



def flatten_live_element(element: JSON, gw: int) -> JSON:
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
    elements: List[JSON], gw: int
) -> List[JSON]:
    """Flatten a list of live elements, skipping those without an id."""
    results = []
    for elem in elements:
        if elem.get("id") is None:
            continue
        results.append(flatten_live_element(elem, gw))
    return results


def flatten_fixture_stats(fixture: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten nested fixture stats into individual rows.

    Each fixture has a ``stats`` list like::

        [{"identifier": "goals_scored", "h": [...], "a": [...]}, ...]

    Each entry in ``h`` / ``a`` is ``{"element": 123, "value": 1}``.
    Returns a flat list of dicts suitable for FixtureStatModel.
    """
    fixture_id = fixture.get("id")
    if fixture_id is None:
        return []
    rows: List[Dict[str, Any]] = []
    for stat_group in fixture.get("stats", []):
        identifier = stat_group.get("identifier", "")
        for side in ("h", "a"):
            for entry in stat_group.get(side, []):
                rows.append({
                    "fixture_id": fixture_id,
                    "identifier": identifier,
                    "element": entry["element"],
                    "value": entry["value"],
                    "side": side,
                })
    return rows



def flatten_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a bootstrap event into a dict suitable for EventModel.

    Serialises nested ``chip_plays`` and ``top_element_info`` into the flat
    schema expected by EventModel.
    """
    top_info = event.get("top_element_info", {})
    chip_plays = event.get("chip_plays", [])
    return {
        **{k: v for k, v in event.items()
           if k not in ("chip_plays", "top_element_info", "overrides")},
        "top_element_points": top_info.get("points") if top_info else None,
        "chip_plays_json": json.dumps(chip_plays) if chip_plays else None,
    }


