"""Player history ingest stage."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.models import PlayerHistoryModel
from fpl_ingest.pipeline.results import StageResult
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
    """Fetch per-player element-summary concurrently and upsert history rows."""
    if not player_ids:
        return StageResult(stage="player_histories")

    history_dir = raw_dir / "players"
    history_dir.mkdir(parents=True, exist_ok=True)

    cached_ids = [pid for pid in player_ids if (history_dir / f"{pid}.json").exists()]
    uncached_ids = [pid for pid in player_ids if pid not in set(cached_ids)]

    if not force and cached_ids:
        logger.info(
            "Skipping %d already-cached player histories (use --force to re-fetch)",
            len(cached_ids),
        )
    if force:
        uncached_ids = player_ids
        cached_ids = []

    logger.info(
        "Fetching element-summary for %d players (%d cached, %d to fetch)...",
        len(player_ids), len(cached_ids), len(uncached_ids),
    )

    fetched = errors = total_upserted = total_skipped = 0

    def _upsert(data: JSON | None) -> tuple[int, int]:
        """Validate and upsert history rows for one player. Returns (upserted, skipped)."""
        if not data:
            return 0, 0
        history = data.get("history", [])
        if not history:
            return 0, 0
        return store.upsert_models(
            "player_histories", PlayerHistoryModel,
            [PlayerHistoryModel.prepare(r) for r in history],
        )

    # --- Cached path: load from disk (no network) ---
    for pid in cached_ids:
        try:
            data = json.loads((history_dir / f"{pid}.json").read_text(encoding="utf-8"))
            ins, skip = _upsert(data)
            total_upserted += ins
            total_skipped += skip
            fetched += 1
        except Exception as e:
            errors += 1
            logger.error("Failed reading cached player %d: %s", pid, e)

    # --- Uncached path: fetch concurrently via the rate limiter ---
    async def _fetch(pid: int) -> tuple[int, JSON | None]:
        data = await client.get_player_history(pid)
        return pid, data

    if uncached_ids:
        raw_results = await asyncio.gather(
            *[_fetch(pid) for pid in uncached_ids],
            return_exceptions=True,
        )

        for i, result in enumerate(raw_results, 1):
            if isinstance(result, Exception):
                errors += 1
                logger.error("Failed player fetch: %s", result)
                continue

            pid, data = result
            if data:
                path = history_dir / f"{pid}.json"
                tmp = path.with_suffix(".tmp")
                tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                tmp.rename(path)
            ins, skip = _upsert(data)
            if data:
                total_upserted += ins
                total_skipped += skip
                fetched += 1
                logger.debug("  Player %d history: %d upserted, %d skipped", pid, ins, skip)
            else:
                errors += 1

            if i % 50 == 0:
                logger.info("[%d/%d] Player histories fetched...", i, len(uncached_ids))

    logger.info("Player histories: %d fetched, %d errors", fetched, errors)
    return StageResult(
        stage="player_histories",
        fetched=fetched,
        upserted=total_upserted,
        skipped=total_skipped,
        errors=errors,
    )
