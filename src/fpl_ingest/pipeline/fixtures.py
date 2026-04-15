"""Fixture ingest stage."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.models import FixtureModel, FixtureStatModel
from fpl_ingest.pipeline.results import StageResult
from fpl_ingest.store import SQLiteStore
from fpl_ingest.transforms import flatten_fixture_stats

logger = logging.getLogger(__name__)


async def ingest_fixtures(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
) -> StageResult:
    """Fetch fixtures and upsert fixtures plus per-player fixture stats."""
    logger.info("Fetching fixtures...")
    fixtures = await client.get_fixtures()

    if fixtures is None:
        logger.error("Failed to fetch fixtures")
        return StageResult(stage="fixtures", errors=1)

    if not fixtures:
        logger.warning("No fixture data returned")
        return StageResult(stage="fixtures")

    (raw_dir / "fixtures.json").write_text(
        json.dumps(fixtures, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    fixtures_for_model = [FixtureModel.prepare(f) for f in fixtures]

    ins, skip = store.upsert_models("fixtures", FixtureModel, fixtures_for_model)
    total_upserted = ins
    total_skipped = skip
    logger.info("Fixtures: %d upserted, %d skipped", ins, skip)

    all_fstats: list[dict] = []
    for fix in fixtures:
        all_fstats.extend(flatten_fixture_stats(fix))

    if all_fstats:
        ins, skip = store.upsert_models("fixture_stats", FixtureStatModel, all_fstats)
        total_upserted += ins
        total_skipped += skip
        logger.info("Fixture stats: %d upserted, %d skipped", ins, skip)

    return StageResult(
        stage="fixtures",
        fetched=len(fixtures),
        upserted=total_upserted,
        skipped=total_skipped,
    )
