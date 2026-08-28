"""CLI entry point and command dispatcher for fpl-ingest.

Exposes the ``run``, ``smoke-test``, and ``inspect`` sub-commands.
Each command handler resolves configuration, delegates to the appropriate
orchestration or extract function, and exits with a meaningful code.
This module contains no business logic — all behaviour lives in the imported
orchestration and extract modules.

The ``status`` sub-command was removed (not redirected) when the SQLite run
audit trail (``_runs``/``_metadata``) it read from was retired. ``inspect``
replaces it: it reads run/stage provenance back from the manifests
``LocalRawWriter`` already writes (``orchestration.inspect``), closing the
freshness-visibility gap that left after ``status`` was removed.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from fpl_ingest.cli_formatters import (
    format_run_detail,
    format_run_list,
    format_smoke_test_failure,
    format_smoke_test_success,
)
from fpl_ingest.config import IngestConfig, default_config, resolve_config
from fpl_ingest.orchestration.inspect import most_recent_run, recent_runs
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

    def add_shared_arguments(target: argparse.ArgumentParser, *, suppress_defaults: bool) -> None:
        """Declare the shared flags on ``target``.

        Each parser gets its own ``argparse.Action`` instances (never shared
        via ``_add_action``/``parents``) because argparse's subparser dispatch
        always overwrites the top-level namespace with whatever the subparser
        produced (see ``_SubParsersAction.__call__``) — including that
        subparser's own defaults for flags the user didn't repeat after the
        subcommand. Reusing the same Action objects, or even the same default
        values, doesn't avoid that: the ``run`` subparser's copies use
        ``default=SUPPRESS`` so an unset flag is simply absent from its result
        namespace instead of clobbering a value already parsed at the top
        level. This lets ``--raw-dir`` (and the other shared flags) work in
        either position, with a value given after ``run`` taking precedence.
        """
        default = argparse.SUPPRESS if suppress_defaults else None
        rate_default = argparse.SUPPRESS if suppress_defaults else DEFAULT_RATE
        target.add_argument(
            "--raw-dir",
            type=Path,
            default=default,
            help=f"Directory for raw JSON cache (default resolved path: {config.raw_dir}).",
        )
        target.add_argument(
            "--rate",
            type=positive_float,
            default=rate_default,
            help=f"Max API requests per second (default: {DEFAULT_RATE}, hard max: {MAX_RATE}).",
        )
        target.add_argument(
            "--strict", action="store_true", default=default,
            help="Abort the run if any stage reports skipped rows or fetch errors.",
        )
        target.add_argument(
            "--verbose", "-v", action="store_true", default=default,
            help="Enable debug logging.",
        )

    parser = argparse.ArgumentParser(prog="fpl-ingest", description="Collect and store FPL API data.")
    add_shared_arguments(parser, suppress_defaults=False)

    subparsers = parser.add_subparsers(dest="command")
    run_parser = subparsers.add_parser("run", help="Run a full ingestion and update the latest-state dataset.")
    add_shared_arguments(run_parser, suppress_defaults=True)

    subparsers.add_parser("smoke-test", help="Run a lightweight upstream API structural drift check.")

    inspect_parser = subparsers.add_parser(
        "inspect", help="Print a run summary read from manifests (replaces the old status command)."
    )
    inspect_parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help=f"Directory for raw JSON cache (default resolved path: {config.raw_dir}).",
    )
    inspect_parser.add_argument(
        "--list", action="store_true",
        help="List recent runs instead of summarizing just the most recent one.",
    )
    inspect_parser.add_argument(
        "--last", type=int, default=10, metavar="N",
        help="With --list, how many recent runs to show (default: 10).",
    )
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
    config = resolve_config(raw_dir=args.raw_dir)
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


def run_inspect(args: argparse.Namespace) -> int:
    config = resolve_config(raw_dir=args.raw_dir)

    if args.list:
        manifests = recent_runs(config.raw_dir, limit=args.last)
        sys.stdout.write(f"{format_run_list([m.data for m in manifests])}\n")
        return 0 if manifests else 1

    manifest = most_recent_run(config.raw_dir)
    if manifest is None:
        sys.stdout.write("No runs recorded\n")
        return 1
    sys.stdout.write(f"{format_run_detail(manifest.data)}\n")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Run the ingest pipeline, or a subcommand if requested."""
    args, _ = build_parser().parse_known_args(argv)
    if args.command == "smoke-test":
        sys.exit(run_smoke_test(args))
    if args.command == "inspect":
        sys.exit(run_inspect(args))
    sys.exit(run_pipeline(args))


if __name__ == "__main__":
    main()
