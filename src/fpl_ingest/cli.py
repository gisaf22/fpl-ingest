"""CLI entry point and command dispatcher for fpl-ingest.

Exposes the ``run`` and ``smoke-test`` sub-commands.
Each command handler resolves configuration, delegates to the appropriate
orchestration or extract function, and exits with a meaningful code.
This module contains no business logic — all behaviour lives in the imported
orchestration and extract modules.

NOTE — gap: the ``status`` sub-command (and its ``last_successful_run_at``/
run-history reporting) is removed here, not redirected. It read the SQLite
run audit trail (``_runs``/``_metadata``), which no longer exists. Per the
migration strategy, that provenance belongs in the run manifest
(``LocalRawWriter``) instead, but nothing currently reads manifests back for
a status report — that's follow-up work, not part of this change.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from fpl_ingest.cli_formatters import (
    format_smoke_test_failure,
    format_smoke_test_success,
)
from fpl_ingest.config import IngestConfig, default_config, resolve_config
from fpl_ingest.orchestration.runner import run_pipeline as execute_pipeline
from fpl_ingest.extract.http.rate_config import DEFAULT_RATE, MAX_RATE
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.schema.validation import (
    SmokeTestFailure,
    run_smoke_test as execute_smoke_test,
)


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------


def build_parser(config: IngestConfig | None = None) -> argparse.ArgumentParser:
    """Build the command-line argument parser."""
    def positive_float(value: str) -> float:
        try:
            parsed = float(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"expected a positive number, got {value!r}") from exc
        if parsed <= 0:
            raise argparse.ArgumentTypeError(f"must be positive, got {parsed}")
        return parsed

    config = config or default_config()
    parser = argparse.ArgumentParser(prog="fpl-ingest", description="Collect and store FPL API data.")
    parser.add_argument("--db", type=Path, default=None, help=f"SQLite database path (default resolved path: {config.db_path}).")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help=f"Directory for raw JSON cache (default resolved path: {config.raw_dir}).",
    )
    parser.add_argument("--force", "-f", action="store_true", help="Re-fetch gameweek data even if already cached.")
    parser.add_argument(
        "--rate",
        type=positive_float,
        default=DEFAULT_RATE,
        help=f"Max API requests per second (default: {DEFAULT_RATE}, hard max: {MAX_RATE}).",
    )
    parser.add_argument("--strict", action="store_true", help="Abort the run if any stage reports skipped rows or fetch errors.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging.")

    subparsers = parser.add_subparsers(dest="command")
    run_parser = subparsers.add_parser("run", help="Run a full ingestion and update the latest-state dataset.")
    for action in parser._actions:
        if action.dest not in ("help", "command"):
            run_parser._add_action(action)

    subparsers.add_parser("smoke-test", help="Run a lightweight upstream API structural drift check.")
    return parser


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def configure_logging(verbose: bool) -> logging.Logger:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger("fpl_ingest")


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def run_pipeline(args: argparse.Namespace) -> int:
    config = resolve_config(db_path=args.db, raw_dir=args.raw_dir)
    logger = configure_logging(args.verbose)
    return asyncio.run(execute_pipeline(args=args, config=config, logger=logger))


def run_smoke_test(_: argparse.Namespace | None = None) -> int:
    try:
        result = execute_smoke_test()
    except (SmokeTestFailure, FPLClientError) as exc:
        sys.stdout.write(f"{format_smoke_test_failure(exc)}\n")
        return 1
    sys.stdout.write(f"{format_smoke_test_success(result)}\n")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Run the ingest pipeline, or a subcommand if requested."""
    args, _ = build_parser().parse_known_args(argv)
    if args.command == "smoke-test":
        sys.exit(run_smoke_test(args))
    sys.exit(run_pipeline(args))


if __name__ == "__main__":
    main()
