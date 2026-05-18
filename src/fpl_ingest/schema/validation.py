"""Schema integrity validation — contract validation and upstream API drift checks.

This module merges two distinct schema-integrity concerns:

  1. Contract validation against a live SQLite database — introspects PRAGMA
     metadata and compares against the compiled contract to detect missing
     tables, column mismatches, type drift, nullability drift, and index drift.

  2. Upstream API structural drift check (smoke test) — fetches bootstrap-static,
     fixtures, and a sample of player histories, then asserts the presence of
     required top-level keys and per-record fields. Raises ``SmokeTestFailure``
     on any structural mismatch so CI and operators are alerted before a full
     ingestion run ingests corrupted data.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Section 1: Contract validation against live DB
# ---------------------------------------------------------------------------

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fpl_ingest.config import resolve_db_path
from fpl_ingest.schema.definition import (
    ConstraintMismatch,
    TypeMismatch,
    ValidationResult,
)

if TYPE_CHECKING:
    from fpl_ingest.schema.compiler import CompiledColumn, CompiledContract, CompiledTable


def generate_validation_rules(schema_version: str, tables: dict[str, CompiledTable]) -> dict[str, Any]:
    """Build validation rules derived entirely from the compiled contract."""
    return {
        "schema_version": schema_version,
        "tables": {
            name: {
                "columns": {
                    column.name: {
                        "sqlite_type": column.sqlite_type,
                        "nullable": column.nullable,
                        "primary_key": column.primary_key,
                        "source": column.source,
                    }
                    for column in table.columns
                },
                "primary_key": list(table.primary_key),
                "unique_constraints": [list(table.unique_key)] if table.unique_key else [],
                "indexes": [list(index) for index in table.indexes],
            }
            for name, table in sorted(tables.items())
        },
    }


def _column_map(table: CompiledTable) -> dict[str, CompiledColumn]:
    return {column.name: column for column in table.columns}


def _normalise_index_tuple(conn: sqlite3.Connection, index_name: str) -> tuple[str, ...]:
    rows = conn.execute(f'PRAGMA index_info("{index_name}")').fetchall()
    ordered = sorted(rows, key=lambda row: row[0])
    return tuple(row[2] for row in ordered)


def _introspect_table(
    conn: sqlite3.Connection,
    table_name: str,
) -> tuple[dict[str, sqlite3.Row], tuple[str, ...], set[tuple[str, ...]], set[tuple[str, ...]]]:
    columns = {
        row["name"]: row
        for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    }
    primary_key = tuple(
        row["name"]
        for row in sorted(columns.values(), key=lambda row: row["pk"])
        if row["pk"]
    )

    unique_constraints: set[tuple[str, ...]] = set()
    indexes: set[tuple[str, ...]] = set()
    for row in conn.execute(f'PRAGMA index_list("{table_name}")').fetchall():
        columns_tuple = _normalise_index_tuple(conn, row["name"])
        if not columns_tuple:
            continue
        if row["unique"]:
            if row["origin"] != "pk":
                unique_constraints.add(columns_tuple)
        else:
            indexes.add(columns_tuple)

    return columns, primary_key, unique_constraints, indexes


def validate_contract_db(contract: CompiledContract, db_path: str | Path | None = None) -> ValidationResult:
    """Validate a live database against the compiled contract."""
    resolved = resolve_db_path(str(db_path) if db_path is not None else None)
    checked_at = datetime.now(timezone.utc).isoformat()

    try:
        conn = sqlite3.connect(resolved)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return ValidationResult(
            status="invalid",
            schema_version=contract.schema_version,
            db_path=str(resolved),
            checked_at=checked_at,
            missing_tables=["<connection_error>"],
        )

    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    extra_columns: dict[str, list[str]] = {}
    type_mismatches: dict[str, list[TypeMismatch]] = {}
    nullability_mismatches: dict[str, list[ConstraintMismatch]] = {}
    primary_key_mismatches: dict[str, ConstraintMismatch] = {}
    unique_constraint_mismatches: dict[str, ConstraintMismatch] = {}
    index_mismatches: dict[str, ConstraintMismatch] = {}

    try:
        table_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        for table_name, table in sorted(contract.tables.items()):
            if table_name not in table_names:
                missing_tables.append(table_name)
                continue

            live_columns, live_primary_key, live_unique, live_indexes = _introspect_table(conn, table_name)
            compiled_columns = _column_map(table)

            missing = sorted(name for name in compiled_columns if name not in live_columns)
            extra = sorted(name for name in live_columns if name not in compiled_columns)
            if missing:
                missing_columns[table_name] = missing
            if extra:
                extra_columns[table_name] = extra

            mismatched_types = [
                TypeMismatch(column=name, expected=column.sqlite_type, actual=live_columns[name]["type"].upper())
                for name, column in compiled_columns.items()
                if name in live_columns and live_columns[name]["type"] and live_columns[name]["type"].upper() != column.sqlite_type
            ]
            if mismatched_types:
                type_mismatches[table_name] = mismatched_types

            mismatched_nullability = [
                ConstraintMismatch(
                    name=name,
                    expected="NOT NULL" if not column.nullable else "NULLABLE",
                    actual="NOT NULL" if bool(live_columns[name]["notnull"]) else "NULLABLE",
                )
                for name, column in compiled_columns.items()
                if name in live_columns and bool(live_columns[name]["notnull"]) != (not column.nullable)
            ]
            if mismatched_nullability:
                nullability_mismatches[table_name] = mismatched_nullability

            expected_primary_key = table.primary_key
            if live_primary_key != expected_primary_key:
                primary_key_mismatches[table_name] = ConstraintMismatch(
                    name="PRIMARY KEY",
                    expected=", ".join(expected_primary_key) or "<none>",
                    actual=", ".join(live_primary_key) or "<none>",
                )

            expected_unique = {tuple(table.unique_key)} if table.unique_key else set()
            if live_unique != expected_unique:
                unique_constraint_mismatches[table_name] = ConstraintMismatch(
                    name="UNIQUE",
                    expected=str(sorted(expected_unique)),
                    actual=str(sorted(live_unique)),
                )

            expected_indexes = set(table.indexes)
            if live_indexes != expected_indexes:
                index_mismatches[table_name] = ConstraintMismatch(
                    name="INDEXES",
                    expected=str(sorted(expected_indexes)),
                    actual=str(sorted(live_indexes)),
                )
    finally:
        conn.close()

    if (
        missing_tables
        or missing_columns
        or type_mismatches
        or nullability_mismatches
        or primary_key_mismatches
        or unique_constraint_mismatches
        or index_mismatches
    ):
        status = "invalid"
    elif extra_columns:
        status = "drift"
    else:
        status = "valid"

    return ValidationResult(
        status=status,
        schema_version=contract.schema_version,
        db_path=str(resolved),
        checked_at=checked_at,
        missing_tables=sorted(missing_tables),
        missing_columns=missing_columns,
        extra_columns=extra_columns,
        type_mismatches=type_mismatches,
        nullability_mismatches=nullability_mismatches,
        primary_key_mismatches=primary_key_mismatches,
        unique_constraint_mismatches=unique_constraint_mismatches,
        index_mismatches=index_mismatches,
    )


# ---------------------------------------------------------------------------
# Section 2: Upstream API structural drift check (smoke test)
# ---------------------------------------------------------------------------

import asyncio
from dataclasses import dataclass

from fpl_ingest.extract.http.client import AsyncFPLClient

DEFAULT_SAMPLE_SIZE = 5


class SmokeTestFailure(RuntimeError):
    """Raised when the upstream API shape no longer matches expectations."""


@dataclass(frozen=True)
class SmokeTestResult:
    """Summary of the smoke test execution."""

    endpoints_checked: tuple[str, ...]
    sample_size: int


def run_smoke_test(*, sample_size: int = DEFAULT_SAMPLE_SIZE) -> SmokeTestResult:
    """Run the smoke test and return a summary or raise on structural drift."""
    return asyncio.run(run_smoke_test_async(sample_size=sample_size))


async def run_smoke_test_async(*, sample_size: int = DEFAULT_SAMPLE_SIZE) -> SmokeTestResult:
    """Run lightweight structural checks against key FPL endpoints."""
    async with AsyncFPLClient() as client:
        bootstrap = await client.get_bootstrap()
        _check_bootstrap_static(bootstrap, sample_size=sample_size)

        fixtures = await client.get_fixtures()
        _check_fixtures(fixtures, sample_size=sample_size)

        player_ids = _sample_player_ids(bootstrap, sample_size=sample_size)
        await _check_player_history(client, player_ids, sample_size=sample_size)

    return SmokeTestResult(
        endpoints_checked=("bootstrap-static", "fixtures", "element-summary"),
        sample_size=sample_size,
    )


def _check_bootstrap_static(payload: Any, *, sample_size: int) -> None:
    _require_mapping(payload, "bootstrap-static")

    for key in ("elements", "teams", "events", "element_types"):
        _require_key(payload, key, "bootstrap-static")

    _check_record_list(
        payload["elements"],
        label="elements[]",
        required_fields=("id", "team", "now_cost"),
        sample_size=sample_size,
    )
    _check_record_list(
        payload["teams"],
        label="teams[]",
        required_fields=("id", "name", "short_name"),
        sample_size=sample_size,
    )
    _check_record_list(
        payload["events"],
        label="events[]",
        required_fields=("id", "deadline_time", "finished"),
        sample_size=sample_size,
    )
    _check_record_list(
        payload["element_types"],
        label="element_types[]",
        required_fields=("id", "singular_name_short", "plural_name_short"),
        sample_size=sample_size,
    )


def _check_fixtures(payload: Any, *, sample_size: int) -> None:
    _require_list(payload, "fixtures")
    _check_record_list(
        payload,
        label="fixtures[]",
        required_fields=("id", "team_h", "team_a", "event"),
        sample_size=sample_size,
    )


async def _check_player_history(
    client: AsyncFPLClient,
    player_ids: list[int],
    *,
    sample_size: int,
) -> None:
    if not player_ids:
        raise SmokeTestFailure("Missing field: elements[].id")

    last_empty_payload = False
    for player_id in player_ids:
        payload = await client.get_player_history(player_id)
        _require_mapping(payload, f"element-summary[{player_id}]")
        _require_key(payload, "history", "element-summary")
        _require_key(payload, "history_past", "element-summary")

        history = payload["history"]
        _require_list(history, "history")
        if not history:
            last_empty_payload = True
            continue

        _check_record_list(
            history,
            label="history[]",
            required_fields=("element", "round", "fixture", "minutes", "total_points"),
            sample_size=sample_size,
        )
        return

    if last_empty_payload:
        return


def _sample_player_ids(bootstrap: dict[str, Any], *, sample_size: int) -> list[int]:
    elements = bootstrap.get("elements", [])
    if not isinstance(elements, list):
        raise SmokeTestFailure("Missing field: bootstrap-static.elements")

    player_ids: list[int] = []
    for record in elements[:sample_size]:
        if isinstance(record, dict) and isinstance(record.get("id"), int):
            player_ids.append(record["id"])
    return player_ids


def _check_record_list(
    payload: Any,
    *,
    label: str,
    required_fields: tuple[str, ...],
    sample_size: int,
) -> None:
    _require_list(payload, label)
    for record in payload[:sample_size]:
        _require_mapping(record, label)
        for field_name in required_fields:
            if field_name not in record:
                raise SmokeTestFailure(f"Missing field: {label}.{field_name}")


def _require_key(payload: dict[str, Any], key: str, label: str) -> None:
    if key not in payload:
        raise SmokeTestFailure(f"Missing field: {label}.{key}")


def _require_mapping(payload: Any, label: str) -> None:
    if not isinstance(payload, dict):
        raise SmokeTestFailure(f"Expected object: {label}")


def _require_list(payload: Any, label: str) -> None:
    if not isinstance(payload, list):
        raise SmokeTestFailure(f"Expected list: {label}")
