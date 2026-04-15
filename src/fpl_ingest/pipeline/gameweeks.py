"""Gameweek ingest stage."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.models import GameweekModel
from fpl_ingest.models import EventModel
from fpl_ingest.pipeline.results import StageResult
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
    """Fetch live gameweek data concurrently and upsert player entries."""
    finished_gws = [e.id for e in events if e.finished]
    current_gw = next((e.id for e in events if e.is_current), None)
    logger.info("Found %d finished gameweeks, current GW: %s", len(finished_gws), current_gw)

    if not force:
        finished_gws = [gw for gw in finished_gws if not (raw_dir / f"gw_{gw}.json").exists()]

    gws_to_fetch = finished_gws + ([current_gw] if current_gw and current_gw not in finished_gws else [])

    if not gws_to_fetch:
        logger.info("All finished gameweeks already collected.")
        return StageResult(stage="gameweeks")

    logger.info("Collecting %d gameweeks...", len(gws_to_fetch))

    async def _fetch(gw: int) -> tuple[int, list[dict] | None]:
        data = await client.get_gw(gw)
        if not data:
            logger.warning("No data for GW%d", gw)
            return gw, None
        dest = raw_dir / f"gw_{gw}.json"
        tmp = dest.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.rename(dest)
        flat = flatten_live_elements(data.get("elements", []), gw)
        logger.info("GW%d — %d player entries fetched", gw, len(flat))
        return gw, flat

    raw_results = await asyncio.gather(
        *[_fetch(gw) for gw in gws_to_fetch],
        return_exceptions=True,
    )

    # Collect results; upsert in GW order for readable logs.
    fetched_rows: dict[int, list[dict]] = {}
    downloaded = errors = 0

    for gw, result in zip(gws_to_fetch, raw_results):
        if isinstance(result, Exception):
            errors += 1
            logger.error("Failed GW%d: %s", gw, result)
            continue
        gw_id, flat = result
        if flat is None:
            errors += 1
        else:
            fetched_rows[gw_id] = flat
            downloaded += 1

    total_upserted = total_skipped = 0
    for gw_id in sorted(fetched_rows):
        flat = fetched_rows[gw_id]
        if flat:
            ins, skip = store.upsert_models("gameweeks", GameweekModel, flat)
            total_upserted += ins
            total_skipped += skip

    logger.info("Gameweeks: %d collected, %d errors", downloaded, errors)
    return StageResult(
        stage="gameweeks",
        fetched=downloaded,
        upserted=total_upserted,
        skipped=total_skipped,
        errors=errors,
    )
