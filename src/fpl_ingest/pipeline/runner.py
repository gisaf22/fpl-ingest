"""Pipeline orchestration for the ingest CLI."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, TypeVar

from fpl_ingest.domain.execution_state import PipelineExecutionState
from fpl_ingest.domain.run_status import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    classify_run_from_results,
)
from fpl_ingest.pipeline import (
    StageResult,
    ingest_core_data,
    ingest_fixtures,
    ingest_gameweeks,
    ingest_player_histories,
    setup_store,
)
from fpl_ingest.pipeline.core import CoreData
from fpl_ingest.storage.store import SQLiteStore
from fpl_ingest.transport.async_client import AsyncFPLClient
from fpl_ingest.transport.rate_config import MAX_RATE, normalize_rate
from fpl_ingest.transport.rate_limiter import TokenBucketLimiter

_MAX_CONCURRENT_REQUESTS = 10
_StageOutput = TypeVar("_StageOutput")


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
        "run failed - data may be partially updated and should not be considered a consistent snapshot"
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
    result: Any
    with store.transaction():
        result, stage_started_at, stage_ended_at, duration_seconds = await _measure_stage(awaitable)

    stage_result = result[1] if isinstance(result, tuple) else result
    _record_stage(
        store,
        stage_results,
        run_started_at,
        logger,
        stage_result,
        strict=strict,
        started_at=stage_started_at,
        ended_at=stage_ended_at,
        duration_seconds=duration_seconds,
    )
    return result


async def _run_pipeline_async(*, args, config, logger: logging.Logger, store: SQLiteStore) -> int:
    """Execute the full ingest pipeline. Returns 0 only on a fully clean run."""
    config.raw_dir.mkdir(parents=True, exist_ok=True)

    execution_state = PipelineExecutionState()
    store._execution_state = execution_state
    run_started_at = datetime.now(timezone.utc).isoformat()
    stage_results: list[StageResult] = []
    core: CoreData | None = None

    applied_rate = _resolve_applied_rate(logger, args.rate)
    rate_limiter = TokenBucketLimiter(rate=applied_rate, max_concurrent=_MAX_CONCURRENT_REQUESTS)

    try:
        async with AsyncFPLClient(
            rate_limiter=rate_limiter,
            connector_limit=_MAX_CONCURRENT_REQUESTS,
        ) as client:
            with store.transaction():
                setup_store(store)
                core_stage: StageResult
                (core, core_stage), stage_started_at, stage_ended_at, duration_seconds = await _measure_stage(
                    ingest_core_data(
                        client,
                        store,
                        config.raw_dir,
                        execution_state=execution_state,
                    )
                )
            _record_stage(
                store,
                stage_results,
                run_started_at,
                logger,
                core_stage,
                strict=args.strict,
                started_at=stage_started_at,
                ended_at=stage_ended_at,
                duration_seconds=duration_seconds,
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

            assert core is not None
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

            assert core is not None
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

    assert core is not None
    return _exit_code(logger, stage_results, store, run_started_at, core)


async def run_pipeline(*, args, config, logger: logging.Logger, store: SQLiteStore) -> int:
    """Public entrypoint for CLI-driven pipeline execution."""
    return await _run_pipeline_async(args=args, config=config, logger=logger, store=store)
