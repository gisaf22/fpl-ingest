"""Fixture ingest pipeline stage.

Fetches all season fixtures from the FPL API, validates them, and upserts
both the fixture metadata and the per-player fixture stats into SQLite.
This module orchestrates: fetch → validate → transform → store.
It does not contain HTTP or SQL logic directly.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.models import FixtureModel, FixtureStatModel
from fpl_ingest.pipeline.stage_result import StageResult
from fpl_ingest.store import SQLiteStore
from fpl_ingest.transforms import flatten_fixture_stats
from fpl_ingest.sync_http import FPLClientError

logger = logging.getLogger(__name__)


async def ingest_fixtures(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
) -> StageResult:
    """Fetch fixtures and upsert fixture rows and per-player fixture stats.

    Args:
        client: Async FPL client for the HTTP fetch.
        store: Active SQLiteStore for upsert operations.
        raw_dir: Directory to write the raw fixtures.json cache file.

    Returns:
        StageResult with fetched/upserted/skipped/error counts.
    """
    logger.info("Fetching fixtures...")
    try:
        fixtures = await client.get_fixtures()
    except FPLClientError as exc:
        logger.error("Failed to fetch fixtures: %s", exc)
        return StageResult(stage="fixtures", errors=1)

    if not fixtures:
        logger.warning("No fixture data returned")
        return StageResult(stage="fixtures")

    _write_raw_cache(raw_dir / "fixtures.json", fixtures)

    total_upserted, total_skipped = _upsert_fixtures(store, fixtures)
    stat_upserted, stat_skipped = _upsert_fixture_stats(store, fixtures)

    return StageResult(
        stage="fixtures",
        fetched=len(fixtures),
        upserted=total_upserted + stat_upserted,
        skipped=total_skipped + stat_skipped,
    )


def _write_raw_cache(path: Path, data: object) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.rename(path)


def _upsert_fixtures(store: SQLiteStore, fixtures: list) -> tuple[int, int]:
    prepared = [FixtureModel.prepare(f) for f in fixtures]
    upserted, skipped = store.upsert_models("fixtures", FixtureModel, prepared)
    logger.info("Fixtures: %d upserted, %d skipped", upserted, skipped)
    return upserted, skipped


def _upsert_fixture_stats(store: SQLiteStore, fixtures: list) -> tuple[int, int]:
    all_stats: list[dict] = []
    for fixture in fixtures:
        all_stats.extend(flatten_fixture_stats(fixture))

    if not all_stats:
        return 0, 0

    upserted, skipped = store.upsert_models("fixture_stats", FixtureStatModel, all_stats)
    logger.info("Fixture stats: %d upserted, %d skipped", upserted, skipped)
    return upserted, skipped
