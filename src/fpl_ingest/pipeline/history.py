"""Player history ingest pipeline stage.

Fetches per-player element-summary data (one request per player) and upserts
the fixture-level history rows into SQLite. Players with cached JSON files
are loaded from disk rather than fetched over the network.

This module orchestrates: fetch (or load from cache) → validate → store.
It does not contain HTTP or SQL logic directly.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fpl_ingest.transport.async_client import AsyncFPLClient
from fpl_ingest.domain.execution_state import PipelineExecutionState
from fpl_ingest.domain.models import PlayerHistoryModel
from fpl_ingest.pipeline.shared import cancel_pending_tasks, write_json_cache
from fpl_ingest.pipeline.stage_result import StageResult
from fpl_ingest.storage.store import SQLiteStore

logger = logging.getLogger(__name__)


class _StrictFetchFailure(RuntimeError):
    """Raised to abort a concurrent strict-mode fetch batch immediately."""


async def ingest_player_histories(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
    player_ids: list[int],
    *,
    force: bool = False,
    strict: bool = False,
    execution_state: PipelineExecutionState | None = None,
) -> StageResult:
    """Fetch per-player element-summary histories and upsert history rows.

    Args:
        client: Async FPL client for uncached player fetches.
        store: Active SQLiteStore for upsert operations.
        raw_dir: Root of the raw cache directory. Player files are stored
            under raw_dir/players/{player_id}.json.
        player_ids: List of FPL element IDs to process.
        force: If True, re-fetch all players even if cached.

    Returns:
        StageResult with canonical fetched/validated/written/skipped counts.
    """
    if not player_ids:
        return StageResult(stage="player_histories")

    history_dir = raw_dir / "players"
    history_dir.mkdir(parents=True, exist_ok=True)

    cached_ids, uncached_ids = _partition_by_cache(history_dir, player_ids, force=force)

    if not force and cached_ids:
        logger.info(
            "Skipping %d already-cached player histories (use --force to re-fetch)",
            len(cached_ids),
        )

    logger.info(
        "Fetching element-summary for %d players (%d cached, %d to fetch)...",
        len(player_ids), len(cached_ids), len(uncached_ids),
    )

    fetched = errors = validated = written = 0

    cached_fetched, cached_validated, cached_written, cached_errors = _load_and_upsert_cached(
        store, history_dir, cached_ids
    )
    fetched += cached_fetched
    errors += cached_errors
    validated += cached_validated
    written += cached_written

    if uncached_ids:
        network_fetched, network_errors, network_validated, network_written = (
            await _fetch_and_upsert_uncached(
                client,
                store,
                history_dir,
                uncached_ids,
                strict=strict,
                execution_state=execution_state,
            )
        )
        fetched += network_fetched
        errors += network_errors
        validated += network_validated
        written += network_written

    return StageResult(
        stage="player_histories",
        fetched=fetched,
        validated=validated,
        written=written,
        skipped=fetched - validated,
        errors=errors,
    )


def _partition_by_cache(
    history_dir: Path,
    player_ids: list[int],
    *,
    force: bool,
) -> tuple[list[int], list[int]]:
    """Split player IDs into cached and uncached sets.

    Returns:
        (cached_ids, uncached_ids) — when force is True, all IDs are uncached.
    """
    if force:
        return [], list(player_ids)
    cached = [pid for pid in player_ids if (history_dir / f"{pid}.json").exists()]
    uncached = [pid for pid in player_ids if pid not in set(cached)]
    return cached, uncached


def _raw_history_rows(data: Optional[Dict[str, Any]]) -> list[dict[str, Any]]:
    """Return raw history rows for one player payload."""
    if not data:
        return []
    history = data.get("history", [])
    if not history:
        return []
    return [PlayerHistoryModel.prepare(row) for row in history]


def _upsert_history_rows(store: SQLiteStore, data: Optional[Dict[str, Any]]) -> tuple[int, int, int]:
    """Validate and upsert history rows for one player."""
    rows = _raw_history_rows(data)
    if not rows:
        return 0, 0, 0
    written, skipped = store.upsert_models("player_histories", PlayerHistoryModel, rows)
    validated = len(rows) - skipped
    return len(rows), validated, written


def _load_and_upsert_cached(
    store: SQLiteStore,
    history_dir: Path,
    cached_ids: list[int],
) -> tuple[int, int, int, int]:
    """Load cached JSON files from disk and upsert their history rows.

    Returns:
        (fetched_count, validated_count, written_count, error_count)
    """
    fetched_count = validated_count = written_count = error_count = 0
    for player_id in cached_ids:
        try:
            data = json.loads((history_dir / f"{player_id}.json").read_text(encoding="utf-8"))
            fetched, validated, written = _upsert_history_rows(store, data)
            fetched_count += fetched
            validated_count += validated
            written_count += written
        except Exception as exc:
            error_count += 1
            logger.error("Failed reading cached player %d: %s", player_id, exc)
    return fetched_count, validated_count, written_count, error_count


async def _fetch_and_upsert_uncached(
    client: AsyncFPLClient,
    store: SQLiteStore,
    history_dir: Path,
    uncached_ids: list[int],
    *,
    strict: bool,
    execution_state: PipelineExecutionState | None = None,
) -> tuple[int, int, int, int]:
    """Fetch uncached players concurrently, write to disk, and upsert history rows.

    Returns:
        (fetched_count, error_count, validated_count, written_count)
    """
    raw_results = await _fetch_player_histories(client, uncached_ids, strict=strict)

    fetched_count = error_count = validated_count = written_count = 0

    if strict and any(isinstance(result, BaseException) for _, result in raw_results):
        if execution_state is not None:
            execution_state.fail()
        fetched_count = sum(
            len(_raw_history_rows(result))
            for _, result in raw_results
            if not isinstance(result, BaseException)
        )
        error_count = sum(1 for _, result in raw_results if isinstance(result, BaseException))
        return fetched_count, error_count, 0, 0

    for index, (player_id, result) in enumerate(raw_results, 1):
        if isinstance(result, BaseException):
            error_count += 1
            logger.error("Failed player fetch: %s", result)
            continue

        data = result
        if data:
            _write_player_cache(history_dir, player_id, data, execution_state=execution_state)
        raw_rows, validated, written = _upsert_history_rows(store, data)
        if data:
            validated_count += validated
            written_count += written
            fetched_count += raw_rows
            logger.debug(
                "Player %d history extracted: raw=%d validated=%d written=%d skipped=%d",
                player_id,
                raw_rows,
                validated,
                written,
                raw_rows - validated,
            )
        else:
            error_count += 1

        if index % 50 == 0:
            logger.info("[%d/%d] Player histories fetched...", index, len(uncached_ids))

    return fetched_count, error_count, validated_count, written_count


async def _fetch_player_histories(
    client: AsyncFPLClient,
    player_ids: list[int],
    *,
    strict: bool,
) -> list[tuple[int, Dict[str, Any] | BaseException]]:
    """Fetch player histories, cancelling pending work on the first strict failure."""
    if not strict:
        raw_results = await asyncio.gather(
            *[client.get_player_history(pid) for pid in player_ids],
            return_exceptions=True,
        )
        return list(zip(player_ids, raw_results))

    tasks = {
        asyncio.create_task(client.get_player_history(player_id)): player_id
        for player_id in player_ids
    }
    completed: list[tuple[int, Dict[str, Any] | BaseException]] = []

    try:
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                player_id = tasks[task]
                try:
                    completed.append((player_id, task.result()))
                except Exception as exc:
                    completed.append((player_id, exc))
                    await cancel_pending_tasks(pending)
                    raise _StrictFetchFailure from exc
    except _StrictFetchFailure:
        return completed

    return completed


def _write_player_cache(
    history_dir: Path,
    player_id: int,
    data: Dict[str, Any],
    *,
    execution_state: PipelineExecutionState | None = None,
) -> None:
    path = history_dir / f"{player_id}.json"
    write_json_cache(path, data, execution_state=execution_state)
