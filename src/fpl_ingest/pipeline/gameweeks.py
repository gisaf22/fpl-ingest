"""Gameweek ingest pipeline stage.

Concurrently fetches live player stats for all finished gameweeks (and the
current one if active), then upserts them into SQLite. Skips gameweeks that
already have a cached JSON file unless --force is passed.

This module orchestrates: fetch → transform → store. It does not contain
HTTP or SQL logic directly.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from fpl_ingest.transport.async_client import AsyncFPLClient
from fpl_ingest.domain.execution_state import PipelineExecutionState
from fpl_ingest.domain.models import EventModel, GameweekModel
from fpl_ingest.pipeline.shared import cancel_pending_tasks, write_json_cache
from fpl_ingest.pipeline.stage_result import StageResult
from fpl_ingest.storage.store import SQLiteStore
from fpl_ingest.domain.transforms import flatten_live_elements

logger = logging.getLogger(__name__)


class _StrictFetchFailure(RuntimeError):
    """Raised to abort a concurrent strict-mode fetch batch immediately."""


async def ingest_gameweeks(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
    events: list[EventModel],
    *,
    force: bool,
    strict: bool = False,
    execution_state: PipelineExecutionState | None = None,
) -> StageResult:
    """Fetch live gameweek data concurrently and upsert player entries.

    Args:
        client: Async FPL client for the HTTP fetches.
        store: Active SQLiteStore for upsert operations.
        raw_dir: Directory for raw gw_{n}.json cache files.
        events: Validated EventModel list from the core stage.
        force: If True, re-fetch all gameweeks even if cached.

    Returns:
        StageResult with canonical fetched/validated/written/skipped counts.
    """
    gameweek_ids_to_fetch = _select_gameweeks_to_fetch(raw_dir, events, force=force)

    if not gameweek_ids_to_fetch:
        logger.info("All finished gameweeks already collected.")
        return StageResult(stage="gameweeks")

    logger.info("Collecting %d gameweeks...", len(gameweek_ids_to_fetch))

    fetched_rows, error_count = await _fetch_gameweeks_concurrently(
        client, gameweek_ids_to_fetch, strict=strict
    )
    fetched_count = sum(len(rows) for _data, rows in fetched_rows.values())
    if strict and error_count > 0:
        if execution_state is not None:
            execution_state.fail()
        validated = written = 0
    else:
        _write_gameweek_caches(raw_dir, fetched_rows, execution_state=execution_state)
        validated, written = _upsert_gameweek_rows(store, fetched_rows)

    return StageResult(
        stage="gameweeks",
        fetched=fetched_count,
        validated=validated,
        written=written,
        skipped=fetched_count - validated,
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
    gameweek_ids: list[int],
    *,
    strict: bool,
) -> tuple[dict[int, tuple[dict[str, Any], list[dict[str, Any]]]], int]:
    """Fetch all gameweeks in parallel and return (results_by_id, error_count)."""
    return await _collect_gameweeks(client, gameweek_ids, strict=strict)


async def _collect_gameweeks(
    client: AsyncFPLClient,
    gameweek_ids: list[int],
    *,
    strict: bool,
) -> tuple[dict[int, tuple[dict[str, Any], list[dict[str, Any]]]], int]:
    """Fetch gameweeks, cancelling pending work on the first strict failure."""
    fetched_rows: dict[int, tuple[dict[str, Any], list[dict[str, Any]]]] = {}
    error_count = 0

    if not strict:
        raw_results = await asyncio.gather(
            *[_fetch_one_gameweek(client, gw) for gw in gameweek_ids],
            return_exceptions=True,
        )

        for gameweek_id, result in zip(gameweek_ids, raw_results):
            if isinstance(result, BaseException):
                error_count += 1
                logger.error("Failed gameweek %d: %s", gameweek_id, result)
                continue
            gw_id, data, flat_rows = result
            fetched_rows[gw_id] = (data, flat_rows)

        return fetched_rows, error_count

    tasks = {
        asyncio.create_task(_fetch_one_gameweek(client, gw)): gw
        for gw in gameweek_ids
    }

    try:
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                gameweek_id = tasks[task]
                try:
                    gw_id, data, flat_rows = task.result()
                except Exception as exc:
                    error_count += 1
                    logger.error("Failed gameweek %d: %s", gameweek_id, exc)
                    await cancel_pending_tasks(pending)
                    raise _StrictFetchFailure from exc
                fetched_rows[gw_id] = (data, flat_rows)
    except _StrictFetchFailure:
        return fetched_rows, error_count

    return fetched_rows, error_count


def _write_gameweek_caches(
    raw_dir: Path,
    fetched_rows: dict[int, tuple[dict[str, Any], list[dict[str, Any]]]],
    *,
    execution_state: PipelineExecutionState | None = None,
) -> None:
    if execution_state is not None and execution_state.is_failed:
        return
    for gameweek_id, (data, _rows) in fetched_rows.items():
        write_json_cache(raw_dir / f"gw_{gameweek_id}.json", data, execution_state=execution_state)


async def _fetch_one_gameweek(
    client: AsyncFPLClient,
    gameweek_id: int,
) -> tuple[int, dict[str, Any], list[dict[str, Any]]]:
    data = await client.get_gw(gameweek_id)
    flat_rows = flatten_live_elements(data.get("elements", []), gameweek_id)
    logger.info("Gameweek %d — %d player entries fetched", gameweek_id, len(flat_rows))
    return gameweek_id, data, flat_rows


def _upsert_gameweek_rows(
    store: SQLiteStore,
    fetched_rows: dict[int, tuple[dict[str, Any], list[dict[str, Any]]]],
) -> tuple[int, int]:
    """Upsert all gameweek rows in ascending gameweek order."""
    total_validated = total_written = 0
    for gameweek_id in sorted(fetched_rows):
        _raw_data, rows = fetched_rows[gameweek_id]
        if rows:
            written, skipped = store.upsert_models("gameweeks", GameweekModel, rows)
            validated = len(rows) - skipped
            total_validated += validated
            total_written += written
            logger.debug(
                "Gameweek %d extracted: raw=%d validated=%d written=%d skipped=%d",
                gameweek_id,
                len(rows),
                validated,
                written,
                skipped,
            )
    return total_validated, total_written
