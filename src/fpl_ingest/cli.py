"""CLI entry point for fpl-ingest.

Responsible for argument parsing, configuration resolution, and wiring the
pipeline stages together. All business logic lives in the pipeline modules.
This module does not contain fetch logic, transformation, or storage code.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from fpl_ingest.async_client import AsyncFPLClient
from fpl_ingest.config import IngestConfig, default_config, resolve_config
from fpl_ingest.pipeline import (
    StageResult,
    ingest_core_data,
    ingest_fixtures,
    ingest_gameweeks,
    ingest_player_histories,
    setup_store,
)
from fpl_ingest.rate_limiter import TokenBucketLimiter
from fpl_ingest.store import SQLiteStore

_MAX_CONCURRENT_REQUESTS = 10


def _argparse_positive_float(value: str) -> float:
    """Argparse type validator for strictly positive float options."""
    try:
        parsed = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a positive number, got {value!r}")
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"must be positive, got {parsed}")
    return parsed


def build_parser(config: IngestConfig | None = None) -> argparse.ArgumentParser:
    """Build the command-line argument parser.

    Args:
        config: Config used to populate default help text. Falls back to
            environment-backed defaults if not provided.

    Returns:
        Configured argparse.ArgumentParser instance.
    """
    config = config or default_config()
    parser = argparse.ArgumentParser(
        prog="fpl-ingest",
        description="Collect and store FPL API data.",
    )
    parser.add_argument("--db", type=Path, default=None,
                        help=f"SQLite database path (default: {config.db_path}).")
    parser.add_argument("--raw-dir", type=Path, default=None,
                        help=f"Directory for raw JSON cache (default: {config.raw_dir}).")
    parser.add_argument("--force", "-f", action="store_true",
                        help="Re-fetch gameweek data even if already cached.")
    parser.add_argument(
        "--rate",
        type=_argparse_positive_float,
        default=10.0,
        help="Max API requests per second (default: 10.0).",
    )
    parser.add_argument("--strict", action="store_true",
                        help="Abort the run if any stage reports skipped rows or fetch errors.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable debug logging.")
    return parser


def _warn_or_raise_on_unclean_stage(result: StageResult, *, strict: bool = False) -> None:
    """Warn (or raise in strict mode) when a stage reports skipped rows or errors."""
    if result.skipped or result.errors:
        msg = f"Ingest stage did not complete cleanly: {result.summary_line()}"
        if strict:
            raise RuntimeError(msg)
        logging.getLogger("fpl_ingest").warning(msg)


def _log_run_summary(logger: logging.Logger, results: Iterable[StageResult]) -> None:
    """Log a compact end-of-run stage summary."""
    logger.info("Stage summary:")
    for result in results:
        logger.info("  %s", result.summary_line())


async def _run_pipeline(argv: list[str] | None = None) -> int:
    """Execute the full ingest pipeline. Returns 0 on clean run, 1 on errors."""
    args = build_parser().parse_args(argv)
    config = resolve_config(db_path=args.db, raw_dir=args.raw_dir)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("fpl_ingest")

    config.raw_dir.mkdir(parents=True, exist_ok=True)

    store = SQLiteStore(config.db_path)
    run_started_at = datetime.now(timezone.utc).isoformat()
    stage_results: list[StageResult] = []

    # Closure so each stage shares store, args, and the accumulating
    # stage_results list without threading them through every call site.
    def _record_stage(result: StageResult) -> StageResult:
        stage_results.append(result)
        store.record_run(
            run_started_at,
            result.stage,
            result.fetched,
            result.upserted,
            result.skipped,
            result.errors,
        )
        _warn_or_raise_on_unclean_stage(result, strict=args.strict)
        _warn_if_high_skip_rate(logger, result)
        return result

    rate_limiter = TokenBucketLimiter(rate=args.rate, max_concurrent=_MAX_CONCURRENT_REQUESTS)

    async with AsyncFPLClient(
        rate_limiter=rate_limiter,
        connector_limit=_MAX_CONCURRENT_REQUESTS,
    ) as client:
        with store.transaction():
            setup_store(store)
            core, core_stage = await ingest_core_data(client, store, config.raw_dir)
        _record_stage(core_stage)

        with store.transaction():
            _record_stage(await ingest_fixtures(client, store, config.raw_dir))

        with store.transaction():
            _record_stage(await ingest_gameweeks(
                client, store, config.raw_dir, core.events, force=args.force,
            ))

        with store.transaction():
            _record_stage(await ingest_player_histories(
                client,
                store,
                config.raw_dir,
                [p.id for p in core.players],
                force=args.force,
            ))

    _log_run_summary(logger, stage_results)
    return _exit_code(logger, stage_results, store, run_started_at, core)


def _warn_if_high_skip_rate(logger: logging.Logger, result: StageResult) -> None:
    total_rows = result.upserted + result.skipped
    if total_rows > 0 and result.skipped / total_rows > 0.01:
        logger.warning(
            "High skip rate: stage=%s skipped=%d/%d (%.1f%%)",
            result.stage, result.skipped, total_rows,
            100 * result.skipped / total_rows,
        )


def _exit_code(
    logger: logging.Logger,
    stage_results: list[StageResult],
    store: SQLiteStore,
    run_started_at: str,
    core: object,
) -> int:
    total_errors = sum(r.errors for r in stage_results)
    if total_errors == 0:
        _write_success_metadata(store, run_started_at, core)
        logger.info("Done.")
        return 0

    logger.error(
        "Run finished with errors in %d stage(s) — check _runs table for details.",
        sum(1 for r in stage_results if r.errors),
    )
    return 1


def _write_success_metadata(store: SQLiteStore, run_started_at: str, core: object) -> None:
    current_gameweek = next((e.id for e in core.events if e.is_current), None)
    with store.transaction():
        store.set_metadata("last_successful_run_at", run_started_at)
        if current_gameweek is not None:
            store.set_metadata("current_gameweek", str(current_gameweek))
        store.set_metadata("total_players", str(len(core.players)))


def main(argv: list[str] | None = None) -> None:
    """Run the ingest pipeline."""
    sys.exit(asyncio.run(_run_pipeline(argv)))


if __name__ == "__main__":
    main()
