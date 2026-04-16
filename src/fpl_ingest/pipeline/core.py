"""Core (bootstrap-static) ingest pipeline stage.

Fetches bootstrap-static from the FPL API and upserts players, teams,
events, and element types into SQLite. This is always the first stage
and its output (CoreData) is passed to downstream stages.

This module orchestrates: fetch → validate → store. It does not contain
HTTP or SQL logic directly.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import NamedTuple

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.models import (
    ElementTypeModel,
    EventModel,
    PlayerModel,
    TeamModel,
)
from fpl_ingest.pipeline.stage_result import StageResult
from fpl_ingest.store import SQLiteStore
from fpl_ingest.transforms import flatten_event, validate_models

logger = logging.getLogger(__name__)


class CoreData(NamedTuple):
    """Validated domain objects extracted from bootstrap-static."""

    players: list[PlayerModel]
    teams: list[TeamModel]
    events: list[EventModel]
    element_types: list[ElementTypeModel]


async def ingest_core_data(
    client: AsyncFPLClient,
    store: SQLiteStore,
    cache_dir: Path,
) -> tuple[CoreData, StageResult]:
    """Fetch bootstrap-static and upsert players, teams, events, and element types.

    Args:
        client: Async FPL client for the bootstrap fetch.
        store: Active SQLiteStore for upsert operations.
        cache_dir: Directory to write the raw bootstrap.json cache file.

    Returns:
        Tuple of (CoreData with validated domain objects, StageResult with counts).
    """
    logger.info("Fetching bootstrap-static...")
    bootstrap = await client.get_bootstrap()
    _write_raw_cache(cache_dir / "bootstrap.json", bootstrap)

    players, player_upserted, player_skipped = _ingest_players(store, bootstrap)
    teams, team_upserted, team_skipped = _ingest_teams(store, bootstrap)
    events, event_upserted, event_skipped = _ingest_events(store, bootstrap)
    element_types, type_upserted, type_skipped = _ingest_element_types(store, bootstrap)

    data = CoreData(
        players=players,
        teams=teams,
        events=events,
        element_types=element_types,
    )
    result = StageResult(
        stage="core",
        fetched=len(players) + len(teams) + len(events) + len(element_types),
        upserted=player_upserted + team_upserted + event_upserted + type_upserted,
        skipped=player_skipped + team_skipped + event_skipped + type_skipped,
    )
    return data, result


def _write_raw_cache(path: Path, data: object) -> None:
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.rename(path)
    except OSError as exc:
        logger.warning("Could not write raw cache to %s: %s", path, exc)


def _ingest_players(
    store: SQLiteStore, bootstrap: dict
) -> tuple[list[PlayerModel], int, int]:
    raw = bootstrap.get("elements", [])
    # Players require .prepare() to flatten nested stats fields into a single dict.
    players, validation_skipped = validate_models(PlayerModel, [PlayerModel.prepare(p) for p in raw])
    upserted, store_skipped = store.upsert_models("players", PlayerModel, [m.model_dump() for m in players])
    logger.info("Players: %d raw, %d upserted, %d skipped", len(raw), upserted, validation_skipped + store_skipped)
    return players, upserted, validation_skipped + store_skipped


def _ingest_teams(
    store: SQLiteStore, bootstrap: dict
) -> tuple[list[TeamModel], int, int]:
    raw = bootstrap.get("teams", [])
    # Teams map directly to the model with no preprocessing needed.
    teams, validation_skipped = validate_models(TeamModel, raw)
    upserted, store_skipped = store.upsert_models("teams", TeamModel, [m.model_dump() for m in teams])
    logger.info("Teams: %d raw, %d upserted, %d skipped", len(raw), upserted, validation_skipped + store_skipped)
    return teams, upserted, validation_skipped + store_skipped


def _ingest_events(
    store: SQLiteStore, bootstrap: dict
) -> tuple[list[EventModel], int, int]:
    raw_events = bootstrap.get("events", [])
    # Events embed chip_plays as a nested list; flatten_event hoists them to top-level fields.
    raw = [flatten_event(e) for e in raw_events]
    events, validation_skipped = validate_models(EventModel, raw)
    upserted, store_skipped = store.upsert_models("events", EventModel, [m.model_dump() for m in events])
    logger.info("Events: %d raw, %d upserted, %d skipped", len(raw_events), upserted, validation_skipped + store_skipped)
    return events, upserted, validation_skipped + store_skipped


def _ingest_element_types(
    store: SQLiteStore, bootstrap: dict
) -> tuple[list[ElementTypeModel], int, int]:
    raw = bootstrap.get("element_types", [])
    # Element types require .prepare() to normalise singular_name_short and plural name fields.
    element_types, validation_skipped = validate_models(
        ElementTypeModel, [ElementTypeModel.prepare(et) for et in raw]
    )
    upserted, store_skipped = store.upsert_models(
        "element_types", ElementTypeModel, [m.model_dump() for m in element_types]
    )
    logger.info("Element types: %d raw, %d upserted, %d skipped", len(raw), upserted, validation_skipped + store_skipped)
    return element_types, upserted, validation_skipped + store_skipped
