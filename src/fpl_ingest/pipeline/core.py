"""Core (bootstrap-static) ingest pipeline stage.

Fetches bootstrap-static from the FPL API and upserts players, teams,
events, and element types into SQLite. This is always the first stage
and its output (CoreData) is passed to downstream stages.

This module orchestrates: fetch → validate → store. It does not contain
HTTP or SQL logic directly.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import NamedTuple

from fpl_ingest.transport.async_client import AsyncFPLClient
from fpl_ingest.domain.execution_state import PipelineExecutionState
from fpl_ingest.domain.models import (
    ElementTypeModel,
    EventModel,
    PlayerModel,
    TeamModel,
)
from fpl_ingest.pipeline.stage_result import StageResult
from fpl_ingest.pipeline.shared import write_json_cache
from fpl_ingest.storage.store import SQLiteStore
from fpl_ingest.domain.transforms import flatten_event, validate_models

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
    *,
    execution_state: PipelineExecutionState | None = None,
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
    try:
        write_json_cache(cache_dir / "bootstrap.json", bootstrap, execution_state=execution_state)
    except OSError as exc:
        logger.warning("Could not write raw cache to %s: %s", cache_dir / "bootstrap.json", exc)

    players, player_raw, player_validated, player_written = _ingest_players(store, bootstrap)
    teams, team_raw, team_validated, team_written = _ingest_teams(store, bootstrap)
    events, event_raw, event_validated, event_written = _ingest_events(store, bootstrap)
    element_types, type_raw, type_validated, type_written = _ingest_element_types(store, bootstrap)

    data = CoreData(
        players=players,
        teams=teams,
        events=events,
        element_types=element_types,
    )
    result = StageResult(
        stage="core",
        fetched=player_raw + team_raw + event_raw + type_raw,
        validated=player_validated + team_validated + event_validated + type_validated,
        written=player_written + team_written + event_written + type_written,
        skipped=(player_raw - player_validated)
        + (team_raw - team_validated)
        + (event_raw - event_validated)
        + (type_raw - type_validated),
    )
    return data, result


def _ingest_players(
    store: SQLiteStore, bootstrap: dict
) -> tuple[list[PlayerModel], int, int, int]:
    raw = bootstrap.get("elements", [])
    # Players require .prepare() to flatten nested stats fields into a single dict.
    players, _validation_skipped = validate_models(PlayerModel, [PlayerModel.prepare(p) for p in raw])
    written, store_skipped = store.upsert_models("players", PlayerModel, [m.model_dump() for m in players])
    _assert_store_validation_consistency("players", store_skipped)
    logger.debug("Players extracted: raw=%d validated=%d written=%d", len(raw), len(players), written)
    return players, len(raw), len(players), written


def _ingest_teams(
    store: SQLiteStore, bootstrap: dict
) -> tuple[list[TeamModel], int, int, int]:
    raw = bootstrap.get("teams", [])
    # Teams map directly to the model with no preprocessing needed.
    teams, _validation_skipped = validate_models(TeamModel, raw)
    written, store_skipped = store.upsert_models("teams", TeamModel, [m.model_dump() for m in teams])
    _assert_store_validation_consistency("teams", store_skipped)
    logger.debug("Teams extracted: raw=%d validated=%d written=%d", len(raw), len(teams), written)
    return teams, len(raw), len(teams), written


def _ingest_events(
    store: SQLiteStore, bootstrap: dict
) -> tuple[list[EventModel], int, int, int]:
    raw_events = bootstrap.get("events", [])
    # Events embed chip_plays as a nested list; flatten_event hoists them to top-level fields.
    raw = [flatten_event(e) for e in raw_events]
    events, _validation_skipped = validate_models(EventModel, raw)
    written, store_skipped = store.upsert_models("events", EventModel, [m.model_dump() for m in events])
    _assert_store_validation_consistency("events", store_skipped)
    logger.debug("Events extracted: raw=%d validated=%d written=%d", len(raw_events), len(events), written)
    return events, len(raw_events), len(events), written


def _ingest_element_types(
    store: SQLiteStore, bootstrap: dict
) -> tuple[list[ElementTypeModel], int, int, int]:
    raw = bootstrap.get("element_types", [])
    # Element types require .prepare() to normalise singular_name_short and plural name fields.
    element_types, _validation_skipped = validate_models(
        ElementTypeModel, [ElementTypeModel.prepare(et) for et in raw]
    )
    written, store_skipped = store.upsert_models(
        "element_types", ElementTypeModel, [m.model_dump() for m in element_types]
    )
    _assert_store_validation_consistency("element_types", store_skipped)
    logger.debug(
        "Element types extracted: raw=%d validated=%d written=%d",
        len(raw),
        len(element_types),
        written,
    )
    return element_types, len(raw), len(element_types), written


def _assert_store_validation_consistency(table_name: str, store_skipped: int) -> None:
    """Validated stage rows must not fail schema validation inside the store."""
    if store_skipped:
        raise RuntimeError(
            f"Store validation mismatch for {table_name}: "
            f"{store_skipped} prevalidated rows were rejected during persistence"
        )
