"""Small formatters for CLI command output."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from fpl_ingest.domain.schema import ValidationResult

if TYPE_CHECKING:
    from fpl_ingest.validation.smoke_test import SmokeTestResult


def format_run_metrics(run: Mapping[str, object]) -> str:
    return (
        f"fetched={run['fetched']} validated={run['validated']} written={run['written']} "
        f"skipped={run['skipped']} errors={run['errors']}"
    )


def format_status_output(
    *,
    db_path: Path,
    raw_dir: Path,
    runs: list[Mapping[str, object]],
    metadata: list[Mapping[str, object]],
    detailed: bool,
) -> str:
    lines = [
        "fpl-ingest status",
        f"DB path:   {db_path}",
        f"Raw path:  {raw_dir}",
    ]
    if not runs:
        lines.append("No runs found.")
        return "\n".join(lines)

    last = runs[0]
    lines.extend(
        [
            f"Last run started: {last['started_at']}",
            f"Status:   {last['status']}",
            f"Stage:    {last['stage']}",
            format_run_metrics(last),
        ]
    )
    if detailed and len(runs) > 1:
        lines.extend(["", "Recent runs:"])
        for run in runs:
            lines.append(f"- {run['started_at']} | {run['stage']} | status={run['status']} {format_run_metrics(run)}")
    if metadata:
        lines.extend(["", "Metadata:"])
        for row in metadata:
            lines.append(f"{row['key']}: {row['value']} (updated {row['updated_at']})")
    return "\n".join(lines)


def format_schema_output(
    *,
    db_path: Path,
    db_source: str,
    table_count: int,
    result: ValidationResult | None = None,
    destination: Path | None = None,
) -> str:
    lines = [
        "Public SQLite schema",
        f"db:       {db_path} (source: {db_source})",
        f"tables:   {table_count} public domain tables",
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
        for table_name, mismatches in sorted(result.nullability_mismatches.items()):
            rendered = ", ".join(
                f"{mismatch.name} expected {mismatch.expected} got {mismatch.actual}"
                for mismatch in mismatches
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
