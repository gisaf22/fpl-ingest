"""Human-readable output formatters for the fpl-ingest CLI.

Converts structured data from the store, schema contract, and smoke test
into terminal-safe strings. Each formatter is a pure function: no I/O,
no logging, no side effects. All CLI output paths pass through this module
so formatting changes stay localised here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from fpl_ingest.config import DEFAULT_STALE_AFTER_HOURS
from fpl_ingest.schema.definition import ValidationResult

if TYPE_CHECKING:
    from fpl_ingest.schema.validation import SmokeTestResult


def _humanize_age(dt: datetime) -> str:
    """Return a human-readable age string relative to now."""
    delta = datetime.now(timezone.utc) - dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        m = seconds // 60
        return f"{m} minute{'s' if m != 1 else ''} ago"
    if seconds < 86400:
        h = seconds // 3600
        return f"{h} hour{'s' if h != 1 else ''} ago"
    d = seconds // 86400
    return f"{d} day{'s' if d != 1 else ''} ago"


def format_run_metrics(run: Mapping[str, object]) -> str:
    return (
        f"fetched={run['fetched']} validated={run['validated']} written={run['written']} "
        f"skipped={run['skipped']} errors={run['errors']}"
    )


def format_status_output(
    *,
    runs: Sequence[Mapping[str, object]],
    last_successful_run_at: str | None,
) -> str:
    """Format the status table with a freshness line and a stale/healthy summary."""
    if not runs:
        return "No runs recorded"

    # Staleness line
    if last_successful_run_at:
        try:
            last_dt = datetime.fromisoformat(last_successful_run_at)
            age_str = _humanize_age(last_dt)
            age_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
            freshness_line = f"Last successful run: {last_successful_run_at} ({age_str})"
            is_stale = age_hours > DEFAULT_STALE_AFTER_HOURS
        except (ValueError, TypeError):
            freshness_line = f"Last successful run: {last_successful_run_at}"
            is_stale = False
    else:
        freshness_line = "Last successful run: never"
        is_stale = True

    # Runs table
    headers = ("started_at", "stage", "status", "fetched", "validated", "written", "skipped", "errors")
    rows_data = [
        (
            str(r.get("started_at", "")),
            str(r.get("stage", "")),
            str(r.get("status") or ""),
            str(r.get("fetched", 0)),
            str(r.get("validated", 0)),
            str(r.get("written", 0)),
            str(r.get("skipped", 0)),
            str(r.get("errors", 0)),
        )
        for r in runs
    ]
    col_widths = [
        max(len(h), *(len(row[i]) for row in rows_data))
        for i, h in enumerate(headers)
    ]
    sep = "  "
    header_row = sep.join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    divider = sep.join("-" * w for w in col_widths)
    table_lines = [header_row, divider] + [
        sep.join(cell.ljust(col_widths[i]) for i, cell in enumerate(row))
        for row in rows_data
    ]

    if is_stale:
        age_label = _humanize_age(datetime.fromisoformat(last_successful_run_at)) if last_successful_run_at else "never"
        summary = f"WARNING: last successful run was {age_label}"
    else:
        summary = "System healthy"

    lines = [freshness_line, ""] + table_lines + ["", summary]
    return "\n".join(lines)


def format_schema_output(
    *,
    db_path: Path,
    db_source: str,
    table_count: int,
    result: ValidationResult | None = None,
    destination: Path | None = None,
) -> str:
    """Format schema export confirmation (destination set) or validation report (result set)."""
    lines = [
        "Public SQLite schema",
        f"db:       {db_path} (source: {db_source})",
        f"tables:   {table_count} public tables",
        "",
    ]
    if destination is not None:
        lines.extend([f"schema:   {destination}", "Export complete."])
        return "\n".join(lines)

    assert result is not None
    if result.missing_tables:
        lines.append("Missing tables:")
        lines.extend(f"  - {table_name}" for table_name in result.missing_tables)

    if result.missing_columns:
        lines.append("Missing columns:")
        for table_name, columns in sorted(result.missing_columns.items()):
            lines.append(f"  - {table_name}: {', '.join(columns)}")

    if result.extra_columns:
        lines.append("Drift columns:")
        for table_name, columns in sorted(result.extra_columns.items()):
            lines.append(f"  - {table_name}: {', '.join(columns)}")

    if result.type_mismatches:
        lines.append("Type mismatches:")
        for table_name, mismatches in sorted(result.type_mismatches.items()):
            rendered = ", ".join(
                f"{mismatch.column} expected {mismatch.expected} got {mismatch.actual}"
                for mismatch in mismatches
            )
            lines.append(f"  - {table_name}: {rendered}")

    if result.nullability_mismatches:
        lines.append("Nullability mismatches:")
        for table_name, constraint_mismatches in sorted(result.nullability_mismatches.items()):
            rendered = ", ".join(
                f"{mismatch.name} expected {mismatch.expected} got {mismatch.actual}"
                for mismatch in constraint_mismatches
            )
            lines.append(f"  - {table_name}: {rendered}")

    if result.primary_key_mismatches:
        lines.append("Primary key mismatches:")
        for table_name, mismatch in sorted(result.primary_key_mismatches.items()):
            lines.append(f"  - {table_name}: expected {mismatch.expected} got {mismatch.actual}")

    if result.unique_constraint_mismatches:
        lines.append("Unique constraint mismatches:")
        for table_name, mismatch in sorted(result.unique_constraint_mismatches.items()):
            lines.append(f"  - {table_name}: expected {mismatch.expected} got {mismatch.actual}")

    if result.index_mismatches:
        lines.append("Index mismatches:")
        for table_name, mismatch in sorted(result.index_mismatches.items()):
            lines.append(f"  - {table_name}: expected {mismatch.expected} got {mismatch.actual}")

    if result.status == "valid":
        lines.extend(
            [
                f"Status: valid (schema v{result.schema_version})",
                "Validation passed. The live database matches the public schema.",
            ]
        )
    elif result.status == "drift":
        lines.extend(
            [
                f"Status: valid with drift (schema v{result.schema_version})",
                "Validation passed with drift. Review extra columns and decide whether the schema should be updated.",
            ]
        )
    else:
        lines.extend(
            [
                f"Status: invalid (schema v{result.schema_version})",
                "Validation failed. The live database is missing required public schema elements.",
            ]
        )
    return "\n".join(lines)


def format_smoke_test_success(result: SmokeTestResult) -> str:
    return "\n".join(
        [
            "Smoke test passed.",
            f"Checked endpoints: {', '.join(result.endpoints_checked)}",
            f"Sample size: {result.sample_size}",
        ]
    )


def format_smoke_test_failure(exc: BaseException) -> str:
    return f"Smoke test failed: {exc}"
