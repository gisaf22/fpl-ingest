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

The run's status (SUCCESS / PARTIAL / FAILED) comes from what it left usable —
``classify_run`` over the manifest's per-endpoint outcomes. The exit code is 0
only for SUCCESS, so any failure still fails the workflow and alerts.
"""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Awaitable, Iterable
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, TypeVar

from fpl_ingest import __version__ as INGEST_VERSION
from fpl_ingest.orchestration.execution_state import PipelineExecutionState
from fpl_ingest.orchestration.run_status import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    RunStatus,
    classify_run,
)
from fpl_ingest.orchestration.stage_result import StageOutcome, StageResult
from fpl_ingest.extract.stages.bootstrap import RAW_ENDPOINT as BOOTSTRAP_ENDPOINT
from fpl_ingest.extract.stages.bootstrap import CoreData, capture_bootstrap_raw, ingest_core_data
from fpl_ingest.extract.stages.event_status import Finality, ingest_event_status
from fpl_ingest.extract.stages.fixtures import RAW_SOURCE, ingest_fixtures
from fpl_ingest.extract.stages.gameweeks import ingest_gameweeks
from fpl_ingest.extract.stages.element_summary import ingest_player_histories
from fpl_ingest.extract.http.client import _ENDPOINTS, AsyncFPLClient
from fpl_ingest.extract.http.local_writer import LocalRawWriter, RawStorageBackend
from fpl_ingest.extract.http.rate_config import MAX_RATE, normalize_rate
from fpl_ingest.extract.http.rate_limiter import TokenBucketLimiter
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.orchestration.pre_deadline import (
    PRE_DEADLINE_TRIGGER,
    in_pre_deadline_window,
    next_deadline,
)

_MAX_CONCURRENT_REQUESTS = 10
_StageOutput = TypeVar("_StageOutput")

# The full run's stages in execution order, each with the endpoint it captures.
_PIPELINE_ENDPOINTS: tuple[tuple[str, str], ...] = (
    ("event_status", "event-status"),
    ("core", BOOTSTRAP_ENDPOINT),
    ("fixtures", "fixtures"),
    ("gameweeks", "event-live"),
    ("player_histories", "element-summary"),
)


def _build_storage_backend(config: Any) -> RawStorageBackend | None:
    """Select the raw-capture backend from config.storage_backend.

    Returns None for the "local" backend so ``LocalRawWriter`` falls back to
    its own ``LocalFilesystemBackend`` default; only "s3" needs a backend
    built here, since it's the one requiring extra config (the bucket name).
    """
    if config.storage_backend == "local":
        return None
    if config.storage_backend == "s3":
        if not config.s3_bucket:
            raise RuntimeError("FPL_STORAGE_BACKEND=s3 requires FPL_S3_BUCKET to be set")
        from fpl_ingest.extract.http.s3_backend import S3Backend

        return S3Backend(config.s3_bucket)
    raise RuntimeError(f"unknown storage backend: {config.storage_backend!r}")


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


def _log_failed_endpoints(logger: logging.Logger, endpoints: dict[str, dict[str, Any]]) -> None:
    """Name every endpoint that is not SUCCESS, so the failure says what to look at."""
    failed = [
        f"{name} {entry['outcome']} ({entry['usable']}/{entry['attempted']} usable)"
        for name, entry in endpoints.items()
        if entry["outcome"] != RUN_STATUS_SUCCESS
    ]
    if failed:
        logger.error("[run] endpoints not fully usable: %s", ", ".join(failed))


def _exit_code(
    logger: logging.Logger,
    raw_writer: LocalRawWriter,
    stage_results: list[StageResult],
) -> int:
    """Log the run's status and return 0 only when it is SUCCESS."""
    endpoints = raw_writer.endpoint_outcomes
    final_status = classify_run(endpoints)
    _log_run_summary(logger, status=final_status, results=stage_results)
    if final_status == RUN_STATUS_SUCCESS:
        return 0

    _log_failed_endpoints(logger, endpoints)
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
    *,
    event_finality: Finality | None = None,
    git_sha: str | None = None,
    ingest_version: str | None = None,
    config: dict[str, Any] | None = None,
    trigger: str | None = None,
) -> None:
    """Stamp the run's raw manifest with the same status the runner reports.

    ``classify_run`` over the writer's per-endpoint outcomes is the single
    source of run status for both the exit code and the manifest, so a
    strict-mode abort is recorded by what it left usable, like any other run.
    Manifest finalisation must never be what fails a run, so a writer error is
    logged and swallowed.

    ``event_finality`` — this run's parsed event-status result, or None if
    that capture failed or did not validate — becomes the manifest's
    ``finality`` block (strategy doc A.5). It is omitted, not faked, when
    unavailable; a consumer must not read a missing block as "settled."
    """
    status: RunStatus = classify_run(raw_writer.endpoint_outcomes)
    try:
        result = raw_writer.finalize(
            status,
            finality=event_finality,
            git_sha=git_sha,
            ingest_version=ingest_version,
            config=config,
            trigger=trigger,
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


async def _measure_stage(awaitable: Awaitable[_StageOutput]) -> tuple[_StageOutput, datetime, datetime, float]:
    stage_started_at = datetime.now(timezone.utc)
    stage_started = perf_counter()
    result = await awaitable
    stage_ended_at = datetime.now(timezone.utc)
    return result, stage_started_at, stage_ended_at, perf_counter() - stage_started


def _record_endpoints_not_attempted(
    raw_writer: LocalRawWriter,
    stage_results: list[StageResult],
    *,
    aborted_stage: str | None = None,
) -> None:
    """Record every endpoint whose stage came after the one that failed the run.

    The failing stage is the strict-mode abort's, else the first with errors —
    the same condition that trips fail-fast. Every later stage either skipped
    itself on the tripped sentinel or, after a strict abort, never ran, so none
    of its endpoint was attempted. Stages before it ran normally, and anything
    they deliberately did not fetch is not recorded.
    """
    failed_stage = aborted_stage or next(
        (result.stage for result in stage_results if result.errors > 0), None
    )
    stages = [stage for stage, _ in _PIPELINE_ENDPOINTS]
    if failed_stage not in stages:
        return
    failed_endpoint = dict(_PIPELINE_ENDPOINTS)[failed_stage]
    for _, endpoint in _PIPELINE_ENDPOINTS[stages.index(failed_stage) + 1 :]:
        raw_writer.record_not_attempted(
            endpoint, reason=f"not attempted: the {failed_endpoint} stage failed earlier in this run"
        )


async def _execute_stage(
    *,
    awaitable: Awaitable[StageOutcome[_StageOutput]],
    stage_results: list[StageResult],
    logger: logging.Logger,
    strict: bool,
    execution_state: PipelineExecutionState | None = None,
) -> _StageOutput | None:
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
    # Centralized fail-fast trigger: catches any stage's hard failure
    # (errors > 0) even if that stage's own author didn't wire up a manual
    # execution_state.fail() call. Keyed on errors specifically, not the
    # broader StageResult.is_clean, so shape-validation soft-failures
    # (skipped > 0, errors == 0) keep NOT tripping fail-fast — that
    # distinction is deliberate (see bootstrap.py/fixtures.py/event_status.py).
    if execution_state is not None and outcome.result.errors > 0:
        execution_state.fail()
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
    # Same centralized trigger as _execute_stage; the core stage bypasses
    # that helper (it must raise, not return None, on a missing output) but
    # needs the identical errors > 0 -> fail() safety net.
    if outcome.result.errors > 0:
        execution_state.fail()
    if outcome.output is None:
        raise RuntimeError("Core stage completed without CoreData output")
    return outcome.output


async def run_pipeline(*, args, config, logger: logging.Logger) -> int:
    """Execute the full ingest pipeline. Returns 0 only on a fully clean run."""
    storage_backend = _build_storage_backend(config)
    if storage_backend is None:
        config.raw_dir.mkdir(parents=True, exist_ok=True)

    execution_state = PipelineExecutionState()
    run_start = datetime.now(timezone.utc)
    run_started_at = run_start.isoformat()
    stage_results: list[StageResult] = []

    # One raw writer — and therefore one manifest — per fpl-ingest run. It is
    # finalized on every exit path below so a run always leaves a terminal
    # manifest behind, matching the status the runner reports.
    raw_writer = LocalRawWriter(
        config.raw_dir, RAW_SOURCE, started_at=run_start, backend=storage_backend
    )

    applied_rate = _resolve_applied_rate(logger, args.rate)
    rate_limiter = TokenBucketLimiter(rate=applied_rate, max_concurrent=_MAX_CONCURRENT_REQUESTS)

    event_finality: Finality | None = None
    git_sha = _current_git_sha(logger)
    run_config = _effective_run_config(args)
    trigger = getattr(args, "trigger", None)

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
                execution_state=execution_state,
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
                execution_state=execution_state,
            )

            await _execute_stage(
                awaitable=ingest_gameweeks(
                    client,
                    raw_writer,
                    core.events,
                    event_finality=event_finality,
                    strict=args.strict,
                    execution_state=execution_state,
                ),
                stage_results=stage_results,
                logger=logger,
                strict=args.strict,
                execution_state=execution_state,
            )

            await _execute_stage(
                awaitable=ingest_player_histories(
                    client,
                    raw_writer,
                    core.player_ids,
                    core.events,
                    event_finality=event_finality,
                    strict=args.strict,
                    execution_state=execution_state,
                ),
                stage_results=stage_results,
                logger=logger,
                strict=args.strict,
                execution_state=execution_state,
            )
        _record_endpoints_not_attempted(raw_writer, stage_results)
        exit_code = _exit_code(logger, raw_writer, stage_results)
        _finalize_raw_manifest(
            raw_writer, logger, event_finality=event_finality,
            git_sha=git_sha, ingest_version=INGEST_VERSION, config=run_config,
            trigger=trigger,
        )
        return exit_code
    except StrictRunFailure as exc:
        execution_state.fail()
        _record_endpoints_not_attempted(raw_writer, stage_results, aborted_stage=exc.result.stage)
        _finalize_raw_manifest(
            raw_writer, logger, event_finality=event_finality,
            git_sha=git_sha, ingest_version=INGEST_VERSION, config=run_config,
            trigger=trigger,
        )
        endpoints = raw_writer.endpoint_outcomes
        _log_run_summary(logger, status=classify_run(endpoints), results=stage_results)
        _log_failed_endpoints(logger, endpoints)
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


async def run_pre_deadline_capture(*, args, config, logger: logging.Logger) -> int:
    """Capture bootstrap-static and fixtures, only when a deadline is near.

    Fetches bootstrap-static and applies the gate in ``orchestration.pre_deadline``
    to that payload. Outside the window it returns 0 having written nothing —
    no payload and no manifest, so a no-op leaves no trace in raw storage.
    Inside it, the same payload is written through ``capture_bootstrap_raw``,
    fixtures is fetched and captured into the same run, and the manifest is
    stamped ``trigger: pre_deadline``. A failed bootstrap-static fetch writes
    nothing and returns 1, since the window cannot be evaluated without it; a
    failed fixtures fetch keeps bootstrap-static and returns 1. Either way the
    workflow's failure email fires.

    ``--force`` (the workflow's ``force`` dispatch input) skips the gate and
    records ``trigger: manual``, since the capture is then a person's choice,
    not the deadline's. A forced run needs no payload to decide, so it
    attempts fixtures even when bootstrap-static fails, keeping whichever
    endpoint succeeded and returning 1.
    """
    storage_backend = _build_storage_backend(config)
    run_start = datetime.now(timezone.utc)
    applied_rate = _resolve_applied_rate(logger, args.rate)
    rate_limiter = TokenBucketLimiter(rate=applied_rate, max_concurrent=_MAX_CONCURRENT_REQUESTS)
    force = bool(getattr(args, "force", False))

    async with AsyncFPLClient(
        rate_limiter=rate_limiter,
        connector_limit=_MAX_CONCURRENT_REQUESTS,
    ) as client:
        bootstrap_error: FPLClientError | None = None
        try:
            raw = await client.get_bootstrap_raw()
        except FPLClientError as exc:
            if not force:
                logger.error("pre-deadline: bootstrap-static fetch failed; nothing written: %s", exc)
                return 1
            logger.error("pre-deadline: bootstrap-static fetch failed; capturing fixtures anyway: %s", exc)
            bootstrap_error = exc
            raw = None

        if force:
            logger.info("pre-deadline: --force given; capturing without the deadline gate")
        else:
            assert raw is not None  # an unforced fetch failure returned above
            payload = raw.json()
            events = payload.get("events") if isinstance(payload, dict) else None
            if not isinstance(events, list):
                logger.warning("pre-deadline: bootstrap-static carried no events list; nothing written")
                events = []
            now = datetime.now(timezone.utc)
            deadline = next_deadline(events, now=now)
            if deadline is None or not in_pre_deadline_window(events, now=now):
                logger.info(
                    "pre-deadline: next deadline %s is outside the window; nothing written",
                    deadline.isoformat() if deadline is not None else "unknown",
                )
                return 0
            logger.info("pre-deadline: deadline %s is within the window; capturing", deadline.isoformat())

        if storage_backend is None:
            config.raw_dir.mkdir(parents=True, exist_ok=True)
        raw_writer = LocalRawWriter(
            config.raw_dir, RAW_SOURCE, started_at=run_start, backend=storage_backend
        )
        stage_results: list[StageResult] = []
        if raw is not None:
            _record_stage(stage_results, logger, capture_bootstrap_raw(raw, raw_writer).result, strict=False)
        else:
            raw_writer.record_failure(
                BOOTSTRAP_ENDPOINT,
                request_url=_ENDPOINTS["bootstrap"],
                error_class=type(bootstrap_error).__name__,
                message=str(bootstrap_error),
            )
            _record_stage(stage_results, logger, StageResult(stage="core", errors=1), strict=False)

        fixtures = await ingest_fixtures(client, raw_writer)
        _record_stage(stage_results, logger, fixtures.result, strict=False)

    exit_code = _exit_code(logger, raw_writer, stage_results)
    _finalize_raw_manifest(
        raw_writer, logger,
        git_sha=_current_git_sha(logger), ingest_version=INGEST_VERSION,
        config=_effective_run_config(args), trigger="manual" if force else PRE_DEADLINE_TRIGGER,
    )
    return exit_code
