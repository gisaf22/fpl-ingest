"""Element-summary ingest pipeline stage.

Fetches per-player element-summary data (one request per player) and upserts
the fixture-level history rows into SQLite. Cache files are retained as raw
cache files, but they are not reused as an input source because the endpoint is
cumulative and stale files would silently miss newer rounds.

This module orchestrates: fetch → validate → store. It does not contain HTTP
or SQL logic directly.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Optional

from fpl_ingest.extract.http.client import AsyncFPLClient, cancel_pending_tasks, write_json_cache
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.transform.models import PlayerHistoryModel
from fpl_ingest.orchestration.stage_result import StageLineage, StageMetadata, StageOutcome, StageResult
from fpl_ingest.load.store import SQLiteStore

logger = logging.getLogger(__name__)

PLAYER_HISTORIES_STAGE = StageMetadata(
    name="player_histories",
    dependencies=("core",),
    raw_artifacts=("players/*.json",),
    output_tables=("player_histories",),
)


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
) -> StageOutcome[None]:
    """Fetch per-player element-summary histories and upsert history rows.

    Args:
        client: Async FPL client for uncached player fetches.
        store: Active SQLiteStore for upsert operations.
        raw_dir: Root of the raw cache directory. Player files are stored
            under raw_dir/players/{player_id}.json.
        player_ids: List of FPL element IDs to process.
        force: If True, re-fetch all players even if cached.

    Returns:
        StageOutcome with canonical fetched/validated/written/skipped counts and lineage.
    """
    if not player_ids:
        return StageOutcome(result=StageResult(stage="player_histories"), lineage=StageLineage.from_metadata(PLAYER_HISTORIES_STAGE))

    history_dir = raw_dir / "players"
    history_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Refreshing element-summary for %d players; raw player cache files will be overwritten.",
        len(player_ids),
    )

    fetched = errors = validated = written = 0

    network_fetched, network_errors, network_validated, network_written = (
        await _fetch_and_upsert_uncached(
            client,
            store,
            history_dir,
            player_ids,
            strict=strict,
            execution_state=execution_state,
        )
    )
    fetched += network_fetched
    errors += network_errors
    validated += network_validated
    written += network_written

    return StageOutcome(
        result=StageResult(
            stage="player_histories",
            fetched=fetched,
            validated=validated,
            written=written,
            skipped=fetched - validated,
            errors=errors,
        ),
        lineage=StageLineage.from_metadata(
            PLAYER_HISTORIES_STAGE,
            raw_artifacts=(history_dir / f"{player_id}.json" for player_id in player_ids),
        ),
    )

def raw_history_rows(data: Optional[Dict[str, Any]]) -> list[dict[str, Any]]:
    """Return raw history rows for one player payload."""
    if not data:
        return []
    history = data.get("history", [])
    if not history:
        return []
    return [PlayerHistoryModel.prepare(row) for row in history]


def upsert_history_rows(store: SQLiteStore, data: Optional[Dict[str, Any]]) -> tuple[int, int, int]:
    """Validate and upsert history rows for one player."""
    rows = raw_history_rows(data)
    if not rows:
        return 0, 0, 0
    written, skipped = store.upsert_models("player_histories", PlayerHistoryModel, rows)
    validated = len(rows) - skipped
    return len(rows), validated, written

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
    t_fetch_start = perf_counter()
    raw_results = await _fetch_player_histories(client, uncached_ids, strict=strict)
    fetch_duration = perf_counter() - t_fetch_start

    fetched_count = error_count = validated_count = written_count = 0
    write_duration = 0.0

    if strict and any(isinstance(result, BaseException) for _, result in raw_results):
        if execution_state is not None:
            execution_state.fail()
        fetched_count = sum(
            len(raw_history_rows(result))
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
        t_write = perf_counter()
        raw_rows, validated, written = upsert_history_rows(store, data)
        write_duration += perf_counter() - t_write
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

    logger.info(
        "[player_histories] timing: players=%d fetch=%.2fs write=%.2fs total=%.2fs",
        len(uncached_ids),
        fetch_duration,
        write_duration,
        fetch_duration + write_duration,
    )
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
