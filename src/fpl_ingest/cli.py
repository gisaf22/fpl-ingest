"""CLI entry point and command dispatcher for fpl-ingest.

Exposes the ``run``, ``status``, ``schema``, ``replay``, and ``smoke-test``
sub-commands. Each command handler resolves configuration, delegates to the
appropriate orchestration, schema, load, or extract function, and exits with a meaningful code.
This module contains no business logic — all behaviour lives in the imported
orchestration, extract, transform, load, and schema modules.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from fpl_ingest.cli_formatters import (
    format_schema_output,
    format_smoke_test_failure,
    format_smoke_test_success,
    format_status_output,
)
from fpl_ingest.config import IngestConfig, default_config, resolve_config, resolve_db_path_with_source
from fpl_ingest.schema.definition import CONTRACT_ARTIFACT_PATH, PUBLIC_TABLES, validate_contract, write_contract_artifact
from fpl_ingest.orchestration.replay import ReplayError, replay_from_cache
from fpl_ingest.orchestration.runner import run_pipeline as execute_pipeline
from fpl_ingest.load.store import SQLiteStore
from fpl_ingest.extract.http.rate_config import DEFAULT_RATE, MAX_RATE
from fpl_ingest.extract.http.sync_http import FPLClientError
from fpl_ingest.schema.validation import (
    SmokeTestFailure,
    run_smoke_test as execute_smoke_test,
)

_SCHEMA_EXIT_VALID = 0
_SCHEMA_EXIT_INVALID = 1
_SCHEMA_EXIT_DRIFT = 2


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
    run_parser.add_argument(
        "--stale-after-hours",
        type=float,
        default=None,
        metavar="N",
        help="Warn if the last successful run is older than N hours (default: 26; env: FPL_STALE_AFTER_HOURS).",
    )
    status_parser = subparsers.add_parser("status", help="Show summary of the last run and current system state.")
    status_parser.add_argument(
        "--runs",
        type=int,
        default=5,
        metavar="N",
        help="Number of recent runs to display (default: 5).",
    )


    schema_parser = subparsers.add_parser("schema", help="Export or validate the public SQLite schema.")
    schema_parser.add_argument("--db", type=Path, default=None, help="Database path to validate against.")
    schema_subparsers = schema_parser.add_subparsers(dest="schema_command")
    export_parser = schema_subparsers.add_parser("export", help="Export the public SQLite schema.")
    export_parser.add_argument("--out", type=Path, default=None, help=f"Output file for the schema artifact (default: {CONTRACT_ARTIFACT_PATH}).")
    schema_subparsers.add_parser("validate", help="Validate a live database against the public SQLite schema.")
    replay_parser = subparsers.add_parser(
        "replay",
        help=(
            "Re-validate and re-upsert from the existing raw JSON cache. "
            "No network calls are made."
        ),
    )
    replay_parser.add_argument("--db", type=Path, default=None, help="SQLite database path.")
    replay_parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Directory containing the raw JSON cache to replay.",
    )
    replay_parser.add_argument(
        "--strict",
        action="store_true",
        help="Abort if any stage reports skipped rows or fetch errors.",
    )
    replay_parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging.")
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
    store = SQLiteStore(config.db_path)
    return asyncio.run(execute_pipeline(args=args, config=config, logger=logger, store=store))


def run_status(args: argparse.Namespace) -> int:
    config = resolve_config(db_path=args.db, raw_dir=args.raw_dir)
    if not config.db_path.exists():
        sys.stdout.write("No runs recorded\n")
        return 0
    n = getattr(args, "runs", 5)
    store = SQLiteStore(config.db_path)
    store.setup_runs_table()
    store.setup_metadata_table()
    runs = store.query("SELECT * FROM _runs ORDER BY started_at DESC, id DESC LIMIT ?", (n,))
    last_row = store.query("SELECT value FROM _metadata WHERE key = 'last_successful_run_at'")
    last_successful_run_at = last_row[0]["value"] if last_row else None
    sys.stdout.write(f"{format_status_output(runs=runs, last_successful_run_at=last_successful_run_at)}\n")
    return 0


def run_schema(args: argparse.Namespace) -> int:
    db_path, db_source = resolve_db_path_with_source(str(args.db) if args.db is not None else None)

    if args.schema_command == "export":
        sys.stdout.write(
            f"{format_schema_output(db_path=db_path, db_source=db_source, table_count=len(PUBLIC_TABLES), destination=write_contract_artifact(args.out))}\n"
        )
        return _SCHEMA_EXIT_VALID
    if args.schema_command == "validate":
        result = validate_contract(db_path)
        if result.version_mismatch:
            sys.stderr.write(
                f"WARNING: schema artifact version ({result.artifact_version}) does not match "
                f"declared SCHEMA_VERSION ({result.schema_version}). "
                "Run 'fpl-ingest schema export' to regenerate the artifact.\n"
            )
        sys.stdout.write(
            f"{format_schema_output(db_path=db_path, db_source=db_source, table_count=len(PUBLIC_TABLES), result=result)}\n"
        )
        if result.status == "valid":
            return _SCHEMA_EXIT_VALID
        if result.status == "drift":
            return _SCHEMA_EXIT_DRIFT
        return _SCHEMA_EXIT_INVALID
    raise SystemExit("schema requires a subcommand: export or validate")


def run_replay(args: argparse.Namespace) -> int:
    config = resolve_config(db_path=args.db, raw_dir=args.raw_dir)
    logger = configure_logging(args.verbose)
    store = SQLiteStore(config.db_path)
    try:
        return replay_from_cache(config.raw_dir, store, logger, strict=args.strict)
    except ReplayError as exc:
        sys.stderr.write(f"replay: {exc}\n")
        return 1


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
    if args.command == "schema":
        sys.exit(run_schema(args))
    if args.command == "smoke-test":
        sys.exit(run_smoke_test(args))
    if args.command == "status":
        sys.exit(run_status(args))
    if args.command == "replay":
        sys.exit(run_replay(args))
    sys.exit(run_pipeline(args))


if __name__ == "__main__":
    main()
