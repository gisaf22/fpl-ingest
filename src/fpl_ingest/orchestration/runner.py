"""Full-run pipeline orchestrator for the ingest CLI.

Sequences the five pipeline stages (event-status → core → fixtures →
gameweeks → histories). Every stage captures raw payloads only, through the
shared ``LocalRawWriter`` — no stage, and no part of this module, writes to a
database any more. Run/stage provenance lives entirely in the run manifest
that ``LocalRawWriter`` maintains (``_finalize_raw_manifest``).

Freshness visibility is read back from the manifest, not tracked during the
run: the ``inspect`` CLI command (``orchestration.inspect``) scans
``_manifests/`` after the fact rather than this module maintaining a
cross-run "last successful run" pointer as SQLite's ``_metadata`` table used
to. There is still no in-run staleness check before a run starts; that would
require the same manifest scan this module doesn't otherwise need.

Returns 0 only when the run is fully clean; any stage error or strict-mode
abort produces exit code 1.
"""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Iterable
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, TypeVar

from fpl_ingest import __version__ as INGEST_VERSION
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    classify_run_from_results,
)
from fpl_ingest.orchestration.stage_result import StageOutcome, StageResult
from fpl_ingest.extract.stages.bootstrap import CoreData, ingest_core_data
from fpl_ingest.extract.stages.event_status import Finality, ingest_event_status
from fpl_ingest.extract.stages.fixtures import RAW_SOURCE, ingest_fixtures
from fpl_ingest.extract.stages.gameweeks import ingest_gameweeks
from fpl_ingest.extract.stages.element_summary import ingest_player_histories
from fpl_ingest.extract.http.client import AsyncFPLClient
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.rate_config import MAX_RATE, normalize_rate
from fpl_ingest.extract.http.rate_limiter import TokenBucketLimiter

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


def _exit_code(
    logger: logging.Logger,
    stage_results: list[StageResult],
) -> int:
    final_status = classify_run_from_results(stage_results, strict_mode=False)

    if final_status == RUN_STATUS_SUCCESS:
        _log_run_summary(logger, status=RUN_STATUS_SUCCESS, results=stage_results)
        return 0

    _log_run_summary(logger, status=final_status, results=stage_results)
    logger.error("Freshness metadata not updated because the run was not fully clean.")
    _log_partial_run_warning(logger)
    return 1


def _current_git_sha(logger: logging.Logger) -> str | None:
    """Best-effort current commit SHA for manifest provenance.

    Provenance metadata must never fail the pipeline: git being unavailable,
    the working tree not being a repo, or any other git error is logged and
    swallowed, returning None instead.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
    except Exception as exc:
        logger.warning("Could not determine git_sha for manifest: %s", exc)
        return None
    return result.stdout.strip() or None


def _effective_run_config(args) -> dict[str, Any]:
    """The run configuration to record in the manifest (strategy doc A.5)."""
    raw_dir = getattr(args, "raw_dir", None)
    return {
        "raw_dir": str(raw_dir) if raw_dir is not None else None,
        "rate": args.rate,
        "strict": bool(getattr(args, "strict", False)),
        "verbose": bool(getattr(args, "verbose", False)),
    }


def _finalize_raw_manifest(
    raw_writer: LocalRawWriter,
    logger: logging.Logger,
    stage_results: list[StageResult],
    *,
    strict_mode: bool,
    event_finality: Finality | None = None,
    git_sha: str | None = None,
    ingest_version: str | None = None,
    config: dict[str, Any] | None = None,
) -> None:
    """Stamp the run's raw manifest with the same status the runner reports.

    ``classify_run_from_results`` is the single source of run status across
    the runner and now the manifest — a shape-validation failure in a
    capture stage surfaces here as FAILED_PARTIAL. Manifest finalisation must
    never be what fails a run, so a writer error is logged and swallowed.

    ``event_finality`` — this run's parsed event-status result, or None if
    that capture failed or did not validate — becomes the manifest's
    ``finality`` block (strategy doc A.5). It is omitted, not faked, when
    unavailable; a consumer must not read a missing block as "settled."
    """
    status = classify_run_from_results(stage_results, strict_mode=strict_mode)
    try:
        result = raw_writer.finalize(
            status,
            finality=event_finality,
            git_sha=git_sha,
            ingest_version=ingest_version,
            config=config,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Failed to finalize raw manifest: %s", exc)
        return
    logger.info("Raw manifest %s written to %s", status, result.manifest_location)


def _record_stage(
    stage_results: list[StageResult],
    logger: logging.Logger,
    result: StageResult,
    *,
    strict: bool,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    duration_seconds: float | None = None,
) -> StageResult:
    stage_results.append(result)
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
    stage_results: list[StageResult],
    logger: logging.Logger,
    strict: bool,
) -> _StageOutput:
    outcome: StageOutcome[Any]
    outcome, stage_started_at, stage_ended_at, duration_seconds = await _measure_stage(awaitable)

    _record_stage(
        stage_results,
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
    raw_writer: LocalRawWriter,
    *,
    execution_state: PipelineExecutionState,
    stage_results: list[StageResult],
    logger: logging.Logger,
    strict: bool,
) -> CoreData:
    """Run the core stage. Returns CoreData or raises — never returns None."""
    outcome: StageOutcome[CoreData]
    outcome, stage_started_at, stage_ended_at, duration_seconds = await _measure_stage(
        ingest_core_data(
            client,
            raw_writer,
            execution_state=execution_state,
        )
    )
    _record_stage(
        stage_results,
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


async def run_pipeline(*, args, config, logger: logging.Logger) -> int:
    """Execute the full ingest pipeline. Returns 0 only on a fully clean run."""
    config.raw_dir.mkdir(parents=True, exist_ok=True)

    execution_state = PipelineExecutionState()
    run_start = datetime.now(timezone.utc)
    run_started_at = run_start.isoformat()
    stage_results: list[StageResult] = []

    # One raw writer — and therefore one manifest — per fpl-ingest run. It is
    # finalized on every exit path below so a run always leaves a terminal
    # manifest behind, matching the status the runner reports.
    raw_writer = LocalRawWriter(config.raw_dir, RAW_SOURCE, started_at=run_start)

    applied_rate = _resolve_applied_rate(logger, args.rate)
    rate_limiter = TokenBucketLimiter(rate=applied_rate, max_concurrent=_MAX_CONCURRENT_REQUESTS)

    event_finality: Finality | None = None
    git_sha = _current_git_sha(logger)
    run_config = _effective_run_config(args)

    try:
        async with AsyncFPLClient(
            rate_limiter=rate_limiter,
            connector_limit=_MAX_CONCURRENT_REQUESTS,
        ) as client:
            event_finality = await _execute_stage(
                awaitable=ingest_event_status(
                    client,
                    raw_writer,
                    execution_state=execution_state,
                ),
                stage_results=stage_results,
                logger=logger,
                strict=args.strict,
            )

            core: CoreData = await _run_core_stage(
                client,
                raw_writer,
                execution_state=execution_state,
                stage_results=stage_results,
                logger=logger,
                strict=args.strict,
            )

            await _execute_stage(
                awaitable=ingest_fixtures(
                    client,
                    raw_writer,
                    execution_state=execution_state,
                ),
                stage_results=stage_results,
                logger=logger,
                strict=args.strict,
            )

            await _execute_stage(
                awaitable=ingest_gameweeks(
                    client,
                    raw_writer,
                    config.raw_dir,
                    core.events,
                    event_finality=event_finality,
                    strict=args.strict,
                    execution_state=execution_state,
                ),
                stage_results=stage_results,
                logger=logger,
                strict=args.strict,
            )

            await _execute_stage(
                awaitable=ingest_player_histories(
                    client,
                    raw_writer,
                    core.player_ids,
                    strict=args.strict,
                    execution_state=execution_state,
                ),
                stage_results=stage_results,
                logger=logger,
                strict=args.strict,
            )
        exit_code = _exit_code(logger, stage_results)
        _finalize_raw_manifest(
            raw_writer, logger, stage_results, strict_mode=False, event_finality=event_finality,
            git_sha=git_sha, ingest_version=INGEST_VERSION, config=run_config,
        )
        return exit_code
    except StrictRunFailure as exc:
        execution_state.fail()
        _finalize_raw_manifest(
            raw_writer, logger, stage_results, strict_mode=True, event_finality=event_finality,
            git_sha=git_sha, ingest_version=INGEST_VERSION, config=run_config,
        )
        _log_run_summary(logger, status=RUN_STATUS_FAILED, results=stage_results)
        _log_fail_fast_failure(logger, exc.result)
        return 1
    except Exception:
        execution_state.fail()
        total_fetched, total_validated, total_written, total_skipped, total_errors = StageResult.totals(stage_results)
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
