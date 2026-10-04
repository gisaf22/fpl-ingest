"""CLI entry point and command dispatcher for fpl-ingest.

Exposes the ``run``, ``pre-deadline``, ``smoke-test``, ``inspect`` and ``backfill`` sub-commands.
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
import datetime as dt
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from fpl_ingest.cli_formatters import (
    format_run_detail,
    format_run_list,
    format_smoke_test_failure,
    format_smoke_test_success,
)
from fpl_ingest.config import IngestConfig, default_config, resolve_config
from fpl_ingest.orchestration.inspect import most_recent_run, recent_runs
from fpl_ingest.orchestration.runner import run_pipeline as execute_pipeline
from fpl_ingest.orchestration.runner import run_pre_deadline_capture
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
    run_parser.add_argument(
        "--trigger", choices=("scheduled", "manual"), default=None,
        help="What started this run; recorded in the manifest's trigger field.",
    )

    pre_deadline_parser = subparsers.add_parser(
        "pre-deadline",
        help="Capture bootstrap-static and fixtures, only if a transfer deadline is within 120 minutes.",
    )
    add_shared_arguments(pre_deadline_parser, suppress_defaults=True)
    pre_deadline_parser.add_argument(
        "--force", action="store_true",
        help="Skip the deadline gate and capture now; the manifest records trigger: manual.",
    )

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

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Build capture-index catalogs for pre-2.1.0 and history runs (#63). Dry run unless --write.",
    )
    backfill_parser.add_argument("--bucket", required=True, help="The capture bucket.")
    backfill_parser.add_argument(
        "--from-date", type=dt.date.fromisoformat, default=None, metavar="YYYY-MM-DD",
        help="Only runs whose run_id starts on or after this date.",
    )
    backfill_parser.add_argument(
        "--to-date", type=dt.date.fromisoformat, default=None, metavar="YYYY-MM-DD",
        help="Only runs whose run_id starts on or before this date.",
    )
    backfill_parser.add_argument(
        "--write", action="store_true",
        help="Write catalog files. Without it the run reads everything and writes nothing.",
    )
    def at_least_one(value: str) -> int:
        try:
            parsed = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"expected an integer, got {value!r}") from exc
        if parsed < 1:
            raise argparse.ArgumentTypeError(f"must be at least 1, got {parsed}")
        return parsed

    backfill_parser.add_argument(
        "--read-concurrency", type=at_least_one, default=16, metavar="N",
        help="Parallel S3 reads per run (default: 16).",
    )
    backfill_parser.add_argument("--report-json", type=Path, required=True, help="Where to write the JSON report.")
    backfill_parser.add_argument("--summary", type=Path, required=True, help="Where to write the Markdown summary.")
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


def run_pre_deadline(args: argparse.Namespace) -> int:
    config = resolve_config(raw_dir=args.raw_dir)
    logger = configure_logging(args.verbose)
    return asyncio.run(run_pre_deadline_capture(args=args, config=config, logger=logger))


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


def run_backfill(args: argparse.Namespace, *, client: Any | None = None) -> int:
    """Run the capture-index backfill against the bucket and write its report (#67).

    Shape failures are reported and exit 0: B reports only (#63 D4). Any
    operational error, such as an unreadable object, exits 1.
    """
    from fpl_ingest import __version__
    from fpl_ingest.backfill import DryRunTree, S3Tree, build_backfill

    if client is None:
        import boto3
        from botocore.config import Config

        # The pool must cover every reader thread, or reads queue on it (#72 P3).
        client = boto3.client("s3", config=Config(max_pool_connections=max(10, args.read_concurrency)))
    sha = (os.environ.get("GITHUB_SHA") or "local")[:7]
    tree: Any = S3Tree(args.bucket, client=client, concurrency=args.read_concurrency)
    if not args.write:
        tree = DryRunTree(tree)

    started = time.monotonic()
    try:
        report = build_backfill(
            tree,
            validator_version=f"fpl-ingest/{__version__}+{sha}",
            from_date=args.from_date,
            to_date=args.to_date,
        )
    except Exception as exc:  # noqa: BLE001 - any failure must fail the job, with a summary
        logging.getLogger(__name__).exception("backfill failed")
        args.summary.write_text(f"## Capture-index backfill\n\n**Failed:** {exc!r}\n", encoding="utf-8")
        return 1
    report.runtime_seconds = round(time.monotonic() - started, 1)

    args.report_json.write_text(json.dumps(report.to_json(), indent=2) + "\n", encoding="utf-8")
    args.summary.write_text(report.to_markdown(), encoding="utf-8")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Run the ingest pipeline, or a subcommand if requested."""
    # parse_args, not parse_known_args: an unknown or removed flag must exit
    # non-zero before any work. On 2026-09-23 a removed flag was silently
    # ignored and a full ingest ran instead.
    args = build_parser().parse_args(argv)
    if args.command == "smoke-test":
        sys.exit(run_smoke_test(args))
    if args.command == "inspect":
        sys.exit(run_inspect(args))
    if args.command == "backfill":
        sys.exit(run_backfill(args))
    if args.command == "pre-deadline":
        sys.exit(run_pre_deadline(args))
    sys.exit(run_pipeline(args))


if __name__ == "__main__":
    main()
