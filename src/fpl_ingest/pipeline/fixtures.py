"""Fixture ingest pipeline stage.

Fetches all season fixtures from the FPL API, validates them, and upserts
both the fixture metadata and the per-player fixture stats into SQLite.
This module orchestrates: fetch → validate → transform → store.
It does not contain HTTP or SQL logic directly.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fpl_ingest.transport.async_client import AsyncFPLClient
from fpl_ingest.domain.execution_state import PipelineExecutionState
from fpl_ingest.domain.models import FixtureModel, FixtureStatModel
from fpl_ingest.pipeline.shared import write_json_cache
from fpl_ingest.pipeline.stage_result import StageResult
from fpl_ingest.storage.store import SQLiteStore
from fpl_ingest.domain.transforms import flatten_fixture_stats
from fpl_ingest.transport.sync_http import FPLClientError

logger = logging.getLogger(__name__)


async def ingest_fixtures(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
    *,
    execution_state: PipelineExecutionState | None = None,
) -> StageResult:
    """Fetch fixtures and upsert fixture rows and per-player fixture stats.

    Args:
        client: Async FPL client for the HTTP fetch.
        store: Active SQLiteStore for upsert operations.
        raw_dir: Directory to write the raw fixtures.json cache file.

    Returns:
        StageResult with canonical fetched/validated/written/skipped counts.
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

    write_json_cache(raw_dir / "fixtures.json", fixtures, execution_state=execution_state)

    fixture_fetched = len(fixtures)
    fixture_validated, fixture_written = _upsert_fixtures(store, fixtures)
    stat_rows = _flatten_fixture_stat_rows(fixtures)
    stat_fetched = len(stat_rows)
    stat_validated, stat_written = _upsert_fixture_stats(store, stat_rows)

    return StageResult(
        stage="fixtures",
        fetched=fixture_fetched + stat_fetched,
        validated=fixture_validated + stat_validated,
        written=fixture_written + stat_written,
        skipped=(fixture_fetched - fixture_validated) + (stat_fetched - stat_validated),
    )
def _upsert_fixtures(store: SQLiteStore, fixtures: list) -> tuple[int, int]:
    prepared = [FixtureModel.prepare(f) for f in fixtures]
    written, skipped = store.upsert_models("fixtures", FixtureModel, prepared)
    validated = len(prepared) - skipped
    logger.debug("Fixtures extracted: raw=%d validated=%d written=%d skipped=%d", len(prepared), validated, written, skipped)
    return validated, written


def _flatten_fixture_stat_rows(fixtures: list) -> list[dict]:
    all_stats: list[dict] = []
    for fixture in fixtures:
        all_stats.extend(flatten_fixture_stats(fixture))
    return all_stats


def _upsert_fixture_stats(store: SQLiteStore, all_stats: list[dict]) -> tuple[int, int]:
    if not all_stats:
        return 0, 0

    written, skipped = store.upsert_models("fixture_stats", FixtureStatModel, all_stats)
    validated = len(all_stats) - skipped
    logger.debug(
        "Fixture stats extracted: raw=%d validated=%d written=%d skipped=%d",
        len(all_stats),
        validated,
        written,
        skipped,
    )
    return validated, written
