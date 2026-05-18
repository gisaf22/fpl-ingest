"""Replay pipeline: re-validate and re-upsert from existing raw JSON cache.

Replay reads raw API responses from disk and runs the same Pydantic
validation and SQLite upsert logic as a normal run.  No network calls are
made and the rate limiter is never involved.

What replay guarantees:
- Validation and persistence logic is identical to a normal run.
- One StageResult and one _runs audit row are produced per stage.
- Strict mode is honoured: any unclean stage aborts the replay.
- The run is finalised with SUCCESS / FAILED_PARTIAL / FAILED using the
  same classification rules as ``fpl-ingest run``.

What replay does NOT guarantee:
- Data freshness: replay reflects the state of the raw cache at replay
  time, which may be hours or days old.
- Completeness: if a cache file for a stage is missing, that stage is
  recorded as an error and the run is marked FAILED.
- Re-fetching: replay never contacts the FPL API; stale caches are not
  refreshed.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from typing import Any

from fpl_ingest.orchestration.run_status import RUN_STATUS_SUCCESS, classify_run_from_results
from fpl_ingest.transform.transforms import flatten_live_elements
from fpl_ingest.extract.stages.bootstrap import CoreData, process_core_payload
from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.extract.stages.fixtures import process_fixtures_payload
from fpl_ingest.extract.stages.gameweeks import process_gameweek_payloads
from fpl_ingest.extract.stages.element_summary import PLAYER_HISTORIES_STAGE, upsert_history_rows
from fpl_ingest.orchestration.stage_result import StageLineage, StageOutcome, StageResult
from fpl_ingest.load.store import SQLiteStore

_logger = logging.getLogger(__name__)


class ReplayError(RuntimeError):
    """Raised when the raw cache directory is missing or empty."""


class _StrictReplayFailure(RuntimeError):
    def __init__(self, result: StageResult) -> None:
        self.result = result
        super().__init__(f"Replay stage did not complete cleanly: {result.summary_line()}")


# ---------------------------------------------------------------------------
# Per-stage replay helpers
# ---------------------------------------------------------------------------


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def replay_core_stage(store: SQLiteStore, raw_dir: Path) -> StageOutcome[CoreData]:
    """Load bootstrap.json from cache and upsert core entities."""
    cache_path = raw_dir / "bootstrap.json"
    if not cache_path.exists():
        _logger.error("Core cache missing: %s", cache_path)
        return StageOutcome(result=StageResult(stage="core", errors=1))

    bootstrap = _load_json(cache_path)
    return process_core_payload(store, bootstrap, artifact_path=cache_path)


def replay_fixtures_stage(store: SQLiteStore, raw_dir: Path) -> StageOutcome[None]:
    """Load fixtures.json from cache and upsert fixture rows and stats."""
    cache_path = raw_dir / "fixtures.json"
    if not cache_path.exists():
        _logger.error("Fixtures cache missing: %s", cache_path)
        return StageOutcome(result=StageResult(stage="fixtures", errors=1))

    fixtures = _load_json(cache_path)
    if not isinstance(fixtures, list):
        _logger.error("fixtures.json does not contain a list")
        return StageOutcome(result=StageResult(stage="fixtures", errors=1))

    return process_fixtures_payload(store, fixtures, artifact_path=cache_path)


def replay_gameweeks_stage(store: SQLiteStore, raw_dir: Path) -> StageOutcome[None]:
    """Load gw_*.json cache files and upsert gameweek rows."""
    gw_files = sorted(raw_dir.glob("gw_*.json"))
    if not gw_files:
        _logger.info("No gameweek cache files found in %s — skipping stage.", raw_dir)
        return StageOutcome(result=StageResult(stage="gameweeks"))

    fetched_rows: dict[int, tuple[dict, list]] = {}
    fetched_count = error_count = 0

    for gw_file in gw_files:
        stem = gw_file.stem
        try:
            gw_id = int(stem.split("_", 1)[1])
        except (IndexError, ValueError):
            _logger.warning("Skipping unexpected gameweek file: %s", gw_file)
            continue
        try:
            data = _load_json(gw_file)
            flat_rows = flatten_live_elements(data.get("elements", []), gw_id)
            fetched_rows[gw_id] = (data, flat_rows)
            fetched_count += len(flat_rows)
            _logger.info("Gameweek %d — %d player entries loaded from cache", gw_id, len(flat_rows))
        except Exception as exc:
            _logger.error("Failed to load gameweek cache %s: %s", gw_file, exc)
            error_count += 1

    if not fetched_rows:
        return StageOutcome(result=StageResult(stage="gameweeks", errors=error_count))

    return process_gameweek_payloads(
        store,
        fetched_rows,
        artifact_paths=(raw_dir / f"gw_{gameweek_id}.json" for gameweek_id in sorted(fetched_rows)),
        errors=error_count,
    )


def replay_player_histories_stage(store: SQLiteStore, raw_dir: Path) -> StageOutcome[None]:
    """Load players/*.json cache files and upsert player history rows."""
    history_dir = raw_dir / "players"
    if not history_dir.exists():
        _logger.info(
            "Player history cache directory not found: %s — skipping stage.", history_dir
        )
        return StageOutcome(result=StageResult(stage="player_histories"))

    player_files = sorted(history_dir.glob("*.json"))
    if not player_files:
        _logger.info("No player history cache files found — skipping stage.")
        return StageOutcome(result=StageResult(stage="player_histories"))

    fetched = errors = validated = written = 0
    for player_file in player_files:
        try:
            data = _load_json(player_file)
            raw_rows, val, writ = upsert_history_rows(store, data)
            fetched += raw_rows
            validated += val
            written += writ
        except Exception as exc:
            _logger.error("Failed to load player history %s: %s", player_file, exc)
            errors += 1

    return StageOutcome(
        result=StageResult(
            stage="player_histories",
            fetched=fetched,
            validated=validated,
            written=written,
            skipped=fetched - validated,
            errors=errors,
        ),
        lineage=StageLineage.from_metadata(PLAYER_HISTORIES_STAGE, raw_artifacts=player_files),
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def replay_from_cache(
    raw_dir: Path,
    store: SQLiteStore,
    logger: logging.Logger,
    *,
    strict: bool = False,
) -> int:
    """Re-validate and re-upsert from the raw JSON cache. No network calls.

    Args:
        raw_dir: Root of the raw cache directory (same path used by ``run``).
        store: Configured SQLiteStore instance.
        logger: Logger for progress and error messages.
        strict: If True, abort on the first stage that reports skipped rows
            or errors — same semantics as ``fpl-ingest run --strict``.

    Returns:
        0 on a fully clean replay, 1 on any failure or partial result.

    Raises:
        ReplayError: If raw_dir does not exist or contains no recognised
            cache files.
    """
    if not raw_dir.exists():
        raise ReplayError(f"Raw cache directory does not exist: {raw_dir}")

    players_dir = raw_dir / "players"
    has_files = (
        (raw_dir / "bootstrap.json").exists()
        or (raw_dir / "fixtures.json").exists()
        or any(raw_dir.glob("gw_*.json"))
        or (players_dir.exists() and any(players_dir.glob("*.json")))
    )
    if not has_files:
        raise ReplayError(
            f"Raw cache directory contains no recognised cache files: {raw_dir}"
        )

    run_started_at = datetime.now(timezone.utc).isoformat()
    stage_results: list[StageResult] = []

    with store.transaction():
        setup_store(store)

    def _record(outcome: StageOutcome[Any]) -> StageResult:
        result = outcome.result
        stage_results.append(result)
        with store.transaction():
            store.record_stage_result(run_started_at, result)
            lineage_recorder = getattr(store, "record_stage_lineage", None)
            if outcome.lineage is not None and lineage_recorder is not None:
                lineage_recorder(
                    run_started_at,
                    outcome.lineage.stage,
                    artifact_paths=outcome.lineage.raw_artifacts,
                    output_tables=outcome.lineage.output_tables,
                )
        logger.info("[replay] %s", result.summary_line())
        if strict and not result.is_clean:
            raise _StrictReplayFailure(result)
        return result

    try:
        with store.transaction():
            core_outcome = replay_core_stage(store, raw_dir)
        _record(core_outcome)

        with store.transaction():
            fixture_outcome = replay_fixtures_stage(store, raw_dir)
        _record(fixture_outcome)

        with store.transaction():
            gameweek_outcome = replay_gameweeks_stage(store, raw_dir)
        _record(gameweek_outcome)

        with store.transaction():
            history_outcome = replay_player_histories_stage(store, raw_dir)
        _record(history_outcome)

    except _StrictReplayFailure as exc:
        with store.transaction():
            store.finalize_run(
                run_started_at,
                errors=exc.result.errors,
                skipped=exc.result.skipped,
                strict_mode=True,
            )
        logger.error("[replay] Aborted in strict mode: %s", exc.result.summary_line())
        return 1

    total_fetched, total_validated, total_written, total_skipped, total_errors = (
        StageResult.totals(stage_results)
    )
    final_status = classify_run_from_results(stage_results, strict_mode=False)

    with store.transaction():
        store.finalize_run(
            run_started_at,
            errors=total_errors,
            skipped=total_skipped,
            strict_mode=False,
        )

    logger.info(
        "[replay] status=%s total_fetched=%d total_validated=%d "
        "total_written=%d total_skipped=%d total_errors=%d",
        final_status,
        total_fetched,
        total_validated,
        total_written,
        total_skipped,
        total_errors,
    )

    return 0 if final_status == RUN_STATUS_SUCCESS else 1
