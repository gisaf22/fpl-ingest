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

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.models import PlayerHistoryModel
from fpl_ingest.pipeline.stage_result import StageResult
from fpl_ingest.store import SQLiteStore
from fpl_ingest.types import JSON

logger = logging.getLogger(__name__)


async def ingest_player_histories(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
    player_ids: list[int],
    *,
    force: bool = False,
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
        StageResult with fetched/upserted/skipped/error counts.
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

    fetched = errors = total_upserted = total_skipped = 0

    cached_upserted, cached_skipped, cached_errors = _load_and_upsert_cached(
        store, history_dir, cached_ids
    )
    fetched += len(cached_ids) - cached_errors
    errors += cached_errors
    total_upserted += cached_upserted
    total_skipped += cached_skipped

    if uncached_ids:
        network_fetched, network_errors, net_upserted, net_skipped = (
            await _fetch_and_upsert_uncached(client, store, history_dir, uncached_ids)
        )
        fetched += network_fetched
        errors += network_errors
        total_upserted += net_upserted
        total_skipped += net_skipped

    logger.info("Player histories: %d fetched, %d errors", fetched, errors)
    return StageResult(
        stage="player_histories",
        fetched=fetched,
        upserted=total_upserted,
        skipped=total_skipped,
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


def _upsert_history_rows(store: SQLiteStore, data: JSON | None) -> tuple[int, int]:
    """Validate and upsert history rows for one player."""
    if not data:
        return 0, 0
    history = data.get("history", [])
    if not history:
        return 0, 0
    return store.upsert_models(
        "player_histories", PlayerHistoryModel,
        [PlayerHistoryModel.prepare(row) for row in history],
    )


def _load_and_upsert_cached(
    store: SQLiteStore,
    history_dir: Path,
    cached_ids: list[int],
) -> tuple[int, int, int]:
    """Load cached JSON files from disk and upsert their history rows.

    Returns:
        (total_upserted, total_skipped, error_count)
    """
    total_upserted = total_skipped = error_count = 0
    for player_id in cached_ids:
        try:
            data = json.loads((history_dir / f"{player_id}.json").read_text(encoding="utf-8"))
            upserted, skipped = _upsert_history_rows(store, data)
            total_upserted += upserted
            total_skipped += skipped
        except Exception as exc:
            error_count += 1
            logger.error("Failed reading cached player %d: %s", player_id, exc)
    return total_upserted, total_skipped, error_count


async def _fetch_and_upsert_uncached(
    client: AsyncFPLClient,
    store: SQLiteStore,
    history_dir: Path,
    uncached_ids: list[int],
) -> tuple[int, int, int, int]:
    """Fetch uncached players concurrently, write to disk, and upsert history rows.

    Returns:
        (fetched_count, error_count, total_upserted, total_skipped)
    """
    raw_results = await asyncio.gather(
        *[client.get_player_history(pid) for pid in uncached_ids],
        return_exceptions=True,
    )

    fetched_count = error_count = total_upserted = total_skipped = 0

    for index, result in enumerate(raw_results, 1):
        player_id = uncached_ids[index - 1]
        if isinstance(result, Exception):
            error_count += 1
            logger.error("Failed player fetch: %s", result)
            continue

        data = result
        if data:
            _write_player_cache(history_dir, player_id, data)
        upserted, skipped = _upsert_history_rows(store, data)
        if data:
            total_upserted += upserted
            total_skipped += skipped
            fetched_count += 1
            logger.debug("  Player %d history: %d upserted, %d skipped", player_id, upserted, skipped)
        else:
            error_count += 1

        if index % 50 == 0:
            logger.info("[%d/%d] Player histories fetched...", index, len(uncached_ids))

    return fetched_count, error_count, total_upserted, total_skipped


def _write_player_cache(history_dir: Path, player_id: int, data: JSON) -> None:
    path = history_dir / f"{player_id}.json"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.rename(path)
