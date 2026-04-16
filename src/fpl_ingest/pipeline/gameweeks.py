"""Gameweek ingest pipeline stage.

Concurrently fetches live player stats for all finished gameweeks (and the
current one if active), then upserts them into SQLite. Skips gameweeks that
already have a cached JSON file unless --force is passed.

This module orchestrates: fetch → transform → store. It does not contain
HTTP or SQL logic directly.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.models import EventModel, GameweekModel
from fpl_ingest.pipeline.stage_result import StageResult
from fpl_ingest.store import SQLiteStore
from fpl_ingest.transforms import flatten_live_elements

logger = logging.getLogger(__name__)


async def ingest_gameweeks(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
    events: list[EventModel],
    *,
    force: bool,
) -> StageResult:
    """Fetch live gameweek data concurrently and upsert player entries.

    Args:
        client: Async FPL client for the HTTP fetches.
        store: Active SQLiteStore for upsert operations.
        raw_dir: Directory for raw gw_{n}.json cache files.
        events: Validated EventModel list from the core stage.
        force: If True, re-fetch all gameweeks even if cached.

    Returns:
        StageResult with fetched/upserted/skipped/error counts.
    """
    gameweek_ids_to_fetch = _select_gameweeks_to_fetch(raw_dir, events, force=force)

    if not gameweek_ids_to_fetch:
        logger.info("All finished gameweeks already collected.")
        return StageResult(stage="gameweeks")

    logger.info("Collecting %d gameweeks...", len(gameweek_ids_to_fetch))

    fetched_rows, error_count = await _fetch_gameweeks_concurrently(
        client, raw_dir, gameweek_ids_to_fetch
    )
    upserted, skipped = _upsert_gameweek_rows(store, fetched_rows)

    logger.info(
        "Gameweeks: %d collected, %d errors",
        len(fetched_rows), error_count,
    )
    return StageResult(
        stage="gameweeks",
        fetched=len(fetched_rows),
        upserted=upserted,
        skipped=skipped,
        errors=error_count,
    )


def _select_gameweeks_to_fetch(
    raw_dir: Path,
    events: list[EventModel],
    *,
    force: bool,
) -> list[int]:
    """Determine which gameweek IDs need to be fetched."""
    finished_ids = [e.id for e in events if e.finished]
    current_id = next((e.id for e in events if e.is_current), None)
    logger.info(
        "Found %d finished gameweeks, current gameweek: %s",
        len(finished_ids), current_id,
    )

    if not force:
        finished_ids = [gw for gw in finished_ids if not (raw_dir / f"gw_{gw}.json").exists()]

    # Always include the current gameweek if it isn't already in the finished list.
    if current_id and current_id not in finished_ids:
        return finished_ids + [current_id]
    return finished_ids


async def _fetch_gameweeks_concurrently(
    client: AsyncFPLClient,
    raw_dir: Path,
    gameweek_ids: list[int],
) -> tuple[dict[int, list[dict]], int]:
    """Fetch all gameweeks in parallel and return (results_by_id, error_count)."""
    raw_results = await asyncio.gather(
        *[_fetch_one_gameweek(client, raw_dir, gw) for gw in gameweek_ids],
        return_exceptions=True,
    )

    fetched_rows: dict[int, list[dict]] = {}
    error_count = 0

    for gameweek_id, result in zip(gameweek_ids, raw_results):
        if isinstance(result, BaseException):
            error_count += 1
            logger.error("Failed gameweek %d: %s", gameweek_id, result)
            continue
        gw_id, flat_rows = result
        if flat_rows is None:
            error_count += 1
        else:
            fetched_rows[gw_id] = flat_rows

    return fetched_rows, error_count


async def _fetch_one_gameweek(
    client: AsyncFPLClient,
    raw_dir: Path,
    gameweek_id: int,
) -> tuple[int, list[dict[str, Any]] | None]:
    data = await client.get_gw(gameweek_id)
    if not data:
        logger.warning("No data for gameweek %d", gameweek_id)
        return gameweek_id, None

    dest = raw_dir / f"gw_{gameweek_id}.json"
    tmp = dest.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.rename(dest)

    flat_rows = flatten_live_elements(data.get("elements", []), gameweek_id)
    logger.info("Gameweek %d — %d player entries fetched", gameweek_id, len(flat_rows))
    return gameweek_id, flat_rows


def _upsert_gameweek_rows(
    store: SQLiteStore,
    fetched_rows: dict[int, list[dict]],
) -> tuple[int, int]:
    """Upsert all gameweek rows in ascending gameweek order."""
    total_upserted = total_skipped = 0
    for gameweek_id in sorted(fetched_rows):
        rows = fetched_rows[gameweek_id]
        if rows:
            upserted, skipped = store.upsert_models("gameweeks", GameweekModel, rows)
            total_upserted += upserted
            total_skipped += skipped
    return total_upserted, total_skipped
