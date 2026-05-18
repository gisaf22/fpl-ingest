"""Full-run pipeline orchestrator for the ingest CLI.

Sequences the four pipeline stages (core → fixtures → gameweeks → histories),
enforces transactional stage isolation, records per-stage audit rows, checks
freshness staleness before the run, and runs post-run integrity assertions
before updating the last_successful_run_at metadata. Returns 0 only when the
run is fully clean; any stage error or strict-mode abort produces exit code 1.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, TypeVar

from fpl_ingest.config import DEFAULT_STALE_AFTER_HOURS
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    classify_run_from_results,
)
from fpl_ingest.orchestration.stage_result import StageOutcome, StageResult
from fpl_ingest.extract.stages.bootstrap import CoreData, ingest_core_data
from fpl_ingest.extract.stages.fixtures import ingest_fixtures
from fpl_ingest.extract.stages.gameweeks import ingest_gameweeks
from fpl_ingest.extract.stages.element_summary import ingest_player_histories
from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.load.store import SQLiteStore
from fpl_ingest.extract.http.client import AsyncFPLClient
from fpl_ingest.extract.http.rate_config import MAX_RATE, normalize_rate
from fpl_ingest.extract.http.rate_limiter import TokenBucketLimiter

_MAX_CONCURRENT_REQUESTS = 10
_StageOutput = TypeVar("_StageOutput")


def _resolve_stale_threshold(args: Any) -> float:
    """Resolve stale-after-hours from CLI args → env var → default."""
    cli_val = getattr(args, "stale_after_hours", None)
    if cli_val is not None:
        return float(cli_val)
    env_val = os.environ.get("FPL_STALE_AFTER_HOURS")
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            pass
    return DEFAULT_STALE_AFTER_HOURS


def _check_stale_freshness(store: SQLiteStore, logger: logging.Logger, stale_after_hours: float) -> None:
    try:
        rows = store.query("SELECT value FROM _metadata WHERE key = 'last_successful_run_at'")
    except Exception:
        return
    if not rows or not rows[0].get("value"):
        return
    try:
        last_run = datetime.fromisoformat(str(rows[0]["value"]))
        age_hours = (datetime.now(timezone.utc) - last_run).total_seconds() / 3600
        if age_hours > stale_after_hours:
            logger.warning(
                "stale data detected: last_successful_run_at=%s age_hours=%.1f threshold_hours=%.1f",
                rows[0]["value"],
                age_hours,
                stale_after_hours,
            )
    except (ValueError, TypeError):
        return


class StrictRunFailure(RuntimeError):
    """Raised when strict mode aborts the run at a stage boundary."""

    def __init__(self, result: StageResult, failure_reason: str) -> None:
        self.result = result
        self.failure_reason = failure_reason
        super().__init__(f"Ingest stage did not complete cleanly: {result.summary_line()}")


def _warn_or_raise_on_unclean_stage(result: StageResult, *, strict: bool = False) -> None:
    """Warn or raise when a stage reports skipped rows or errors."""
    if not result.is_clean:
        msg = f"Ingest stage did not complete cleanly: {result.summary_line()}"
        if strict:
            raise StrictRunFailure(result, result.failure_reason or "unknown")
        logging.getLogger("fpl_ingest").warning(msg)


def _log_stage_result(
    logger: logging.Logger,
    result: StageResult,
    *,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    duration_seconds: float | None = None,
) -> None:
    summary = result.summary_line()
    if started_at is not None and ended_at is not None and duration_seconds is not None:
        summary = (
            f"{summary} started_at={started_at.isoformat()} ended_at={ended_at.isoformat()} "
            f"duration={duration_seconds:.2f}s"
        )
    logger.info(summary)


def _log_run_summary(logger: logging.Logger, *, status: str, results: Iterable[StageResult]) -> None:
    total_fetched, total_validated, total_written, total_skipped, total_errors = StageResult.totals(results)
    logger.info(
        "[run] status=%s total_fetched=%d total_validated=%d total_written=%d total_skipped=%d total_errors=%d",
        status,
        total_fetched,
        total_validated,
        total_written,
        total_skipped,
        total_errors,
    )


def _resolve_applied_rate(logger: logging.Logger, requested_rate: float) -> float:
    applied_rate = normalize_rate(requested_rate)
    if requested_rate > MAX_RATE:
        logger.warning(
            "API rate limited to safe maximum: requested_rate=%.1f applied_rate=%.1f (clamped to safe maximum)",
            requested_rate,
            applied_rate,
        )
    else:
        logger.info(
            "API rate configured: requested_rate=%.1f applied_rate=%.1f",
            requested_rate,
            applied_rate,
        )
    return applied_rate


def _warn_if_high_skip_rate(logger: logging.Logger, result: StageResult) -> None:
    total_rows = result.fetched
    if total_rows > 0 and result.skipped / total_rows > 0.01:
        logger.warning(
            "High skip rate: stage=%s skipped=%d/%d (%.1f%%)",
            result.stage,
            result.skipped,
            total_rows,
            100 * result.skipped / total_rows,
        )


def _log_partial_run_warning(logger: logging.Logger) -> None:
    logger.warning(
        "run failed - data may be partially updated and should not be considered a complete current-state dataset"
    )


def _log_fail_fast_failure(logger: logging.Logger, stage_result: StageResult) -> None:
    total_fetched, total_validated, total_written, total_skipped, total_errors = StageResult.totals([stage_result])
    logger.error(
        "Run failed fast: failure_reason=%s failed_stage=%s total_fetched=%d total_validated=%d total_written=%d total_skipped=%d total_errors=%d",
        stage_result.failure_reason,
        stage_result.stage,
        total_fetched,
        total_validated,
        total_written,
        total_skipped,
        total_errors,
    )
    logger.error("Freshness metadata not updated because the run was not fully clean.")
    _log_partial_run_warning(logger)


def _success_metadata(run_started_at: str, core: CoreData) -> dict[str, str]:
    metadata_updates = {
        "last_successful_run_at": run_started_at,
        "total_players": str(len(core.players)),
    }
    current_gameweek = next((event.id for event in core.events if event.is_current), None)
    if current_gameweek is not None:
        metadata_updates["current_gameweek"] = str(current_gameweek)
    return metadata_updates


def _exit_code(
    logger: logging.Logger,
    stage_results: list[StageResult],
    store: SQLiteStore,
    run_started_at: str,
    core: CoreData,
) -> int:
    total_fetched, total_validated, total_written, total_skipped, total_errors = StageResult.totals(stage_results)
    final_status = classify_run_from_results(stage_results, strict_mode=False)

    if final_status == RUN_STATUS_SUCCESS:
        from fpl_ingest.load.integrity import IntegrityViolation
        _integrity_checker = getattr(store, "run_integrity_checks", None)
        if _integrity_checker is not None:
            try:
                _integrity_checker()
            except IntegrityViolation as exc:
                logger.error("Post-run integrity check failed: %s", exc)
                with store.transaction():
                    store.finalize_run(run_started_at, errors=total_errors + 1, skipped=total_skipped, strict_mode=False)
                _log_run_summary(logger, status=RUN_STATUS_FAILED, results=stage_results)
                return 1
        with store.transaction():
            store.finalize_run(
                run_started_at,
                errors=total_errors,
                skipped=total_skipped,
                strict_mode=False,
                metadata_updates=_success_metadata(run_started_at, core),
            )
        _log_run_summary(logger, status=RUN_STATUS_SUCCESS, results=stage_results)
        return 0

    with store.transaction():
        store.finalize_run(run_started_at, errors=total_errors, skipped=total_skipped, strict_mode=False)
    _log_run_summary(logger, status=final_status, results=stage_results)
    logger.error("Freshness metadata not updated because the run was not fully clean.")
    _log_partial_run_warning(logger)
    return 1


def _record_stage(
    store: SQLiteStore,
    stage_results: list[StageResult],
    run_started_at: str,
    logger: logging.Logger,
    result: StageResult,
    *,
    strict: bool,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    duration_seconds: float | None = None,
) -> StageResult:
    stage_results.append(result)
    with store.transaction():
        store.record_stage_result(run_started_at, result)
    _log_stage_result(
        logger,
        result,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
    )
    _warn_or_raise_on_unclean_stage(result, strict=strict)
    _warn_if_high_skip_rate(logger, result)
    return result


def _record_stage_lineage(store: SQLiteStore, run_started_at: str, outcome: StageOutcome[Any]) -> None:
    if outcome.lineage is None:
        return
    with store.transaction():
        store.record_stage_lineage(
            run_started_at,
            outcome.lineage.stage,
            artifact_paths=outcome.lineage.raw_artifacts,
            output_tables=outcome.lineage.output_tables,
        )


async def _measure_stage(awaitable) -> tuple[_StageOutput, datetime, datetime, float]:
    stage_started_at = datetime.now(timezone.utc)
    stage_started = perf_counter()
    result = await awaitable
    stage_ended_at = datetime.now(timezone.utc)
    return result, stage_started_at, stage_ended_at, perf_counter() - stage_started


async def _execute_stage(
    *,
    awaitable,
    store: SQLiteStore,
    stage_results: list[StageResult],
    run_started_at: str,
    logger: logging.Logger,
    strict: bool,
) -> _StageOutput:
    outcome: StageOutcome[Any]
    with store.transaction():
        outcome, stage_started_at, stage_ended_at, duration_seconds = await _measure_stage(awaitable)

    _record_stage_lineage(store, run_started_at, outcome)
    _record_stage(
        store,
        stage_results,
        run_started_at,
        logger,
        outcome.result,
        strict=strict,
        started_at=stage_started_at,
        ended_at=stage_ended_at,
        duration_seconds=duration_seconds,
    )
    return outcome.output


async def _run_core_stage(
    client: AsyncFPLClient,
    store: SQLiteStore,
    raw_dir: Path,
    *,
    execution_state: PipelineExecutionState,
    stage_results: list[StageResult],
    run_started_at: str,
    logger: logging.Logger,
    strict: bool,
) -> CoreData:
    """Run the core stage. Returns CoreData or raises — never returns None."""
    outcome: StageOutcome[CoreData]
    with store.transaction():
        setup_store(store)
        outcome, stage_started_at, stage_ended_at, duration_seconds = await _measure_stage(
            ingest_core_data(
                client,
                store,
                raw_dir,
                execution_state=execution_state,
            )
        )
    _record_stage_lineage(store, run_started_at, outcome)
    _record_stage(
        store,
        stage_results,
        run_started_at,
        logger,
        outcome.result,
        strict=strict,
        started_at=stage_started_at,
        ended_at=stage_ended_at,
        duration_seconds=duration_seconds,
    )
    if outcome.output is None:
        raise RuntimeError("Core stage completed without CoreData output")
    return outcome.output


async def run_pipeline(*, args, config, logger: logging.Logger, store: SQLiteStore) -> int:
    """Execute the full ingest pipeline. Returns 0 only on a fully clean run."""
    config.raw_dir.mkdir(parents=True, exist_ok=True)

    execution_state = PipelineExecutionState()
    run_started_at = datetime.now(timezone.utc).isoformat()
    stage_results: list[StageResult] = []

    applied_rate = _resolve_applied_rate(logger, args.rate)
    _check_stale_freshness(store, logger, _resolve_stale_threshold(args))
    rate_limiter = TokenBucketLimiter(rate=applied_rate, max_concurrent=_MAX_CONCURRENT_REQUESTS)

    try:
        async with AsyncFPLClient(
            rate_limiter=rate_limiter,
            connector_limit=_MAX_CONCURRENT_REQUESTS,
        ) as client:
            core: CoreData = await _run_core_stage(
                client,
                store,
                config.raw_dir,
                execution_state=execution_state,
                stage_results=stage_results,
                run_started_at=run_started_at,
                logger=logger,
                strict=args.strict,
            )

            await _execute_stage(
                awaitable=ingest_fixtures(
                    client,
                    store,
                    config.raw_dir,
                    execution_state=execution_state,
                ),
                store=store,
                stage_results=stage_results,
                run_started_at=run_started_at,
                logger=logger,
                strict=args.strict,
            )

            await _execute_stage(
                awaitable=ingest_gameweeks(
                    client,
                    store,
                    config.raw_dir,
                    core.events,
                    force=args.force,
                    strict=args.strict,
                    execution_state=execution_state,
                ),
                store=store,
                stage_results=stage_results,
                run_started_at=run_started_at,
                logger=logger,
                strict=args.strict,
            )

            await _execute_stage(
                awaitable=ingest_player_histories(
                    client,
                    store,
                    config.raw_dir,
                    [player.id for player in core.players],
                    force=args.force,
                    strict=args.strict,
                    execution_state=execution_state,
                ),
                store=store,
                stage_results=stage_results,
                run_started_at=run_started_at,
                logger=logger,
                strict=args.strict,
            )
        return _exit_code(logger, stage_results, store, run_started_at, core)
    except StrictRunFailure as exc:
        execution_state.fail()
        with store.transaction():
            store.finalize_run(
                run_started_at,
                errors=exc.result.errors,
                skipped=exc.result.skipped,
                strict_mode=True,
            )
        _log_run_summary(logger, status=RUN_STATUS_FAILED, results=stage_results)
        _log_fail_fast_failure(logger, exc.result)
        return 1
    except Exception:
        execution_state.fail()
        total_fetched, total_validated, total_written, total_skipped, total_errors = StageResult.totals(stage_results)
        with store.transaction():
            store.finalize_run(
                run_started_at,
                errors=total_errors + 1,
                skipped=total_skipped,
                strict_mode=False,
            )
        _log_run_summary(logger, status=RUN_STATUS_FAILED, results=stage_results)
        logger.exception(
            "Run terminated unexpectedly: total_fetched=%d total_validated=%d total_written=%d total_skipped=%d stage_errors=%d additional_errors=%d",
            total_fetched,
            total_validated,
            total_written,
            total_skipped,
            total_errors,
            1,
        )
        logger.error("Freshness metadata not updated because the run did not complete successfully.")
        _log_partial_run_warning(logger)
        return 1
