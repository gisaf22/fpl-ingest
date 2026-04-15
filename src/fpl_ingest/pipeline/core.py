"""Core bootstrap ingest stage."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import NamedTuple, TypeVar

from pydantic import BaseModel, ValidationError

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.models import (
    ElementTypeModel,
    EventModel,
    PlayerModel,
    TeamModel,
)
from fpl_ingest.pipeline.results import StageResult
from fpl_ingest.store import SQLiteStore
from fpl_ingest.transforms import flatten_event

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class CoreData(NamedTuple):
    players: list[PlayerModel]
    teams: list[TeamModel]
    events: list[EventModel]
    element_types: list[ElementTypeModel]


def _validate_all(schema: type[T], raw_list: list[dict]) -> tuple[list[T], int]:
    """Validate a list of raw dicts against a Pydantic schema, counting invalid rows."""
    valid, skipped = [], 0
    for raw in raw_list:
        try:
            valid.append(schema.model_validate(raw))
        except ValidationError as e:
            skipped += 1
            entity_id = raw.get("id", "unknown")
            logger.warning(
                "Skipped invalid %s row (id=%s): %s",
                schema.__name__, entity_id, e.errors()[0],
            )
    return valid, skipped


async def ingest_core_data(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
) -> tuple[CoreData, StageResult]:
    """Fetch bootstrap-static and upsert players, teams, events, element_types."""
    logger.info("Fetching bootstrap-static...")
    bootstrap = await client.get_bootstrap()

    (raw_dir / "bootstrap.json").write_text(
        json.dumps(bootstrap, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    total_upserted = total_skipped = 0

    players_raw = bootstrap.get("elements", [])
    players, skipped = _validate_all(PlayerModel, [PlayerModel.prepare(p) for p in players_raw])
    ins, skip = store.upsert_models("players", PlayerModel, [m.model_dump() for m in players])
    total_upserted += ins
    total_skipped += skipped + skip
    logger.info("Players: %d upserted, %d skipped", ins, skip)

    teams, skipped = _validate_all(TeamModel, bootstrap.get("teams", []))
    ins, skip = store.upsert_models("teams", TeamModel, [m.model_dump() for m in teams])
    total_upserted += ins
    total_skipped += skipped + skip
    logger.info("Teams: %d upserted, %d skipped", ins, skip)

    events, skipped = _validate_all(EventModel, [flatten_event(e) for e in bootstrap.get("events", [])])
    ins, skip = store.upsert_models("events", EventModel, [m.model_dump() for m in events])
    total_upserted += ins
    total_skipped += skipped + skip
    logger.info("Events: %d upserted, %d skipped", ins, skip)

    element_types_raw = bootstrap.get("element_types", [])
    element_types, skipped = _validate_all(ElementTypeModel, [ElementTypeModel.prepare(et) for et in element_types_raw])
    ins, skip = store.upsert_models("element_types", ElementTypeModel, [m.model_dump() for m in element_types])
    total_upserted += ins
    total_skipped += skipped + skip
    logger.info("Element types: %d upserted, %d skipped", ins, skip)

    data = CoreData(players=players, teams=teams, events=events, element_types=element_types)
    result = StageResult(
        stage="core",
        fetched=len(players) + len(teams) + len(events) + len(element_types),
        upserted=total_upserted,
        skipped=total_skipped,
    )
    return data, result
