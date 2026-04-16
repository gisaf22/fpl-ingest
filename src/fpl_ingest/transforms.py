"""Pure data transformations for FPL API payloads.

Converts raw API JSON into shapes suitable for Pydantic model validation
and SQLite storage. All functions are stateless and have no side effects —
no I/O, no logging, no model imports.

Responsibilities:
    cost_to_millions        Convert now_cost (tenths) to float in millions.
    flatten_live_element    Flatten one live-endpoint element for GameweekModel.
    flatten_live_elements   Flatten a list of live-endpoint elements.
    flatten_fixture_stats   Flatten fixture stats into FixtureStatModel rows.
    flatten_event           Flatten a bootstrap event for EventModel.
    validate_models         Validate a list of raw dicts against a Pydantic schema.

This module does not perform HTTP requests or database writes.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, TypeVar

from pydantic import BaseModel, ValidationError

from fpl_ingest.types import JSON

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Bidirectional position mappings between FPL element_type integers and codes.
ELEMENT_TYPE_TO_POS = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}
POS_TO_ELEMENT_TYPE = {"GKP": 1, "DEF": 2, "MID": 3, "FWD": 4}


def cost_to_millions(now_cost: int) -> float:
    """Convert FPL now_cost (tenths of millions) to a float in millions.

    Args:
        now_cost: Raw cost value from the API (e.g. 130 for £13.0m).

    Returns:
        Cost in millions (e.g. 13.0).
    """
    return now_cost / 10.0


def flatten_live_element(element: Dict[str, Any], gameweek: int) -> Dict[str, Any]:
    """Flatten a live-endpoint element dict into a GameweekModel-compatible dict.

    The FPL live endpoint nests stats inside each element:
        {"id": 123, "stats": {"minutes": 90, "goals_scored": 1, ...}}

    This unpacks the nested stats and injects element_id and round so the
    result validates directly against GameweekModel:
        flat = flatten_live_element(raw_element, gameweek=24)
        entry = GameweekModel.model_validate(flat)

    Args:
        element: Raw element dict from the live endpoint.
        gameweek: Gameweek number to inject as the 'round' field.

    Returns:
        Flat dict with element_id, round, and all stat fields.

    Raises:
        ValueError: If the element has no 'id' key.
    """
    player_id = element.get("id")
    if player_id is None:
        raise ValueError("Element is missing 'id'")
    stats = element.get("stats", {})
    return {"element_id": player_id, "round": gameweek, **stats}


def flatten_live_elements(elements: List[Any], gameweek: int) -> List[Dict[str, Any]]:
    """Flatten a list of live-endpoint elements, skipping those without an id.

    Args:
        elements: List of raw element dicts from the live endpoint.
        gameweek: Gameweek number injected into each output row.

    Returns:
        List of flat dicts suitable for GameweekModel validation.
    """
    results = []
    for element in elements:
        if not isinstance(element, dict):
            continue
        if element.get("id") is None:
            continue
        results.append(flatten_live_element(element, gameweek))
    return results


def flatten_fixture_stats(fixture: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten nested fixture stats into individual FixtureStatModel-compatible rows.

    Each fixture has a stats list like:
        [{"identifier": "goals_scored", "h": [...], "a": [...]}, ...]

    Each entry in h / a is {"element": 123, "value": 1}. Returns a flat list
    of dicts with one row per player-stat-side combination.

    Args:
        fixture: Raw fixture dict from the fixtures endpoint.

    Returns:
        List of flat stat dicts, or an empty list if the fixture has no id.
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
    """Flatten a bootstrap-static event dict into an EventModel-compatible dict.

    Serialises the nested chip_plays list to JSON and extracts
    top_element_info.points into a scalar column, removing fields that have
    no direct column equivalent in the schema.

    Args:
        event: Raw event dict from bootstrap-static.

    Returns:
        Flat dict suitable for EventModel validation.
    """
    top_info = event.get("top_element_info", {})
    chip_plays = event.get("chip_plays", [])
    return {
        **{k: v for k, v in event.items()
           if k not in ("chip_plays", "top_element_info", "overrides")},
        "top_element_points": top_info.get("points") if top_info else None,
        "chip_plays_json": json.dumps(chip_plays) if chip_plays else None,
    }


def validate_models(schema: type[T], raw_list: list[dict]) -> tuple[list[T], int]:
    """Validate a list of raw dicts against a Pydantic schema, counting invalid rows.

    Args:
        schema: Pydantic model class to validate against.
        raw_list: List of raw dicts (e.g. from FPL API JSON).

    Returns:
        (valid_instances, skipped_count) tuple.
    """
    valid, skipped = [], 0
    for raw in raw_list:
        try:
            valid.append(schema.model_validate(raw))
        except ValidationError as exc:
            skipped += 1
            logger.warning(
                "Skipped invalid %s row (id=%s): %s",
                schema.__name__, raw.get("id", "unknown"), exc.errors()[0],
            )
    return valid, skipped
