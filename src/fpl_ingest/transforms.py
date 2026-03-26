"""Generic FPL data transformations and constants.

Position mappings, cost conversions, season ID calculation, and
live-data flattening that are not specific to any consuming project.
"""

from __future__ import annotations

import json
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


def flatten_explain(
    element: Dict[str, Any], gw: int
) -> List[Dict[str, Any]]:
    """Flatten GW live explain data into rows for ExplainStatModel.

    Each element has an ``explain`` list like::

        [{"fixture": 9, "stats": [{"identifier": "minutes", ...}, ...]}, ...]
    """
    player_id = element.get("id")
    if player_id is None:
        return []
    rows: List[Dict[str, Any]] = []
    for entry in element.get("explain", []):
        fixture_id = entry.get("fixture")
        for stat in entry.get("stats", []):
            rows.append({
                "element_id": player_id,
                "round": gw,
                "fixture_id": fixture_id,
                "identifier": stat.get("identifier", ""),
                "points": stat.get("points", 0),
                "value": stat.get("value", 0),
                "points_modification": stat.get("points_modification", 0),
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


def flatten_player_history_past(
    history_past: List[Dict[str, Any]], player_id: int
) -> List[Dict[str, Any]]:
    """Flatten element-summary ``history_past`` into PlayerHistoryModel rows."""
    rows: List[Dict[str, Any]] = []
    for entry in history_past:
        rows.append({
            "element_id": player_id,
            "season_name": entry.get("season_name", ""),
            "total_points": entry.get("total_points", 0),
            "minutes": entry.get("minutes", 0),
            "goals_scored": entry.get("goals_scored", 0),
            "assists": entry.get("assists", 0),
            "clean_sheets": entry.get("clean_sheets", 0),
            "goals_conceded": entry.get("goals_conceded", 0),
            "own_goals": entry.get("own_goals", 0),
            "penalties_saved": entry.get("penalties_saved", 0),
            "penalties_missed": entry.get("penalties_missed", 0),
            "yellow_cards": entry.get("yellow_cards", 0),
            "red_cards": entry.get("red_cards", 0),
            "saves": entry.get("saves", 0),
            "bonus": entry.get("bonus", 0),
            "bps": entry.get("bps", 0),
            "influence": entry.get("influence"),
            "creativity": entry.get("creativity"),
            "threat": entry.get("threat"),
            "ict_index": entry.get("ict_index"),
            "starts": entry.get("starts", 0),
            "expected_goals": entry.get("expected_goals"),
            "expected_assists": entry.get("expected_assists"),
            "expected_goal_involvements": entry.get("expected_goal_involvements"),
            "expected_goals_conceded": entry.get("expected_goals_conceded"),
            "start_cost": entry.get("start_cost"),
            "end_cost": entry.get("end_cost"),
            "element_code": entry.get("element_code"),
        })
    return rows
