"""SQLite persistence layer for FPL Pydantic models.

Creates tables from Pydantic schemas, validates incoming data, and
bulk-upserts rows. This module is database-only — it has no knowledge
of the FPL API, HTTP clients, or pipeline orchestration.

Tables are created from Pydantic model fields via schema_to_create_table.
Upsert logic is driven by UNIQUE constraints declared on each model.
Column migrations (add-only) run automatically on register_table.

Usage:
    from fpl_ingest import SQLiteStore, PlayerModel

    store = SQLiteStore("fpl.db")
    with store.transaction():
        store.register_table("players", PlayerModel)
        store.upsert_models("players", PlayerModel, raw_dicts)
"""

from __future__ import annotations

import logging
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, Type

from pydantic import BaseModel, ValidationError

# Schema-generation helpers live in models.py to avoid a circular
# import: store imports models for schema generation; if models
# imported store for SQLite utilities, the import cycle would break
# at load time.
from fpl_ingest.models import pydantic_to_sqlite_column, schema_to_create_table

logger = logging.getLogger(__name__)


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_safe_identifier(value: str) -> None:
    # All SQL identifiers must come from internal schema introspection, never
    # external input. This guard makes that invariant explicit and loud.
    if not _SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")


def _parse_conflict_columns(unique_constraint: str) -> str:
    """Extract the column list from a UNIQUE(...) constraint string.

    Args:
        unique_constraint: A string like 'UNIQUE(col1, col2)'.

    Returns:
        Column list string, e.g. 'col1, col2'.
    """
    match = re.search(r"UNIQUE\s*\(([^)]+)\)", unique_constraint, re.IGNORECASE)
    return match.group(1).strip() if match else unique_constraint


class SQLiteStore:
    """SQLite store that persists FPL Pydantic models.

    Manages a single shared connection within a transaction block. Outside
    a transaction, each operation opens and closes its own connection.
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._registered_tables: Dict[str, Type[BaseModel]] = {}
        self._active_conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _get_connection(self) -> sqlite3.Connection:
        """Open a database connection with production-safe PRAGMA settings.

        WAL mode: readers never block writers and vice versa.
        synchronous=NORMAL: safe with WAL; skips redundant full-sync calls.
        busy_timeout: retry on lock instead of raising immediately — handles
            overlapping runs from two processes.
        """
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """Open a single connection for the duration of the block.

        All store operations within the block share this connection.
        Commits on success, rolls back on exception.
        """
        conn = self._get_connection()
        self._active_conn = conn
        try:
            yield
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._active_conn = None
            conn.close()

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def register_table(
        self,
        table_name: str,
        schema: Type[BaseModel],
        *,
        extra_columns: Optional[List[str]] = None,
        unique_constraint: Optional[str] = None,
    ) -> None:
        """Create a table from a Pydantic schema (if it doesn't exist) and run column migrations.

        Args:
            table_name: SQL table name.
            schema: Pydantic model whose fields become columns.
            extra_columns: Additional column definitions not on the model.
            unique_constraint: Optional UNIQUE constraint clause.
        """
        all_extra = list(extra_columns or []) + ["ingested_at TEXT"]
        sql = schema_to_create_table(
            table_name, schema,
            extra_columns=all_extra,
            unique_constraint=unique_constraint,
        )
        self._exec(sql)
        self._migrate_new_columns(table_name, schema, all_extra)
        self._registered_tables[table_name] = schema

    def create_index(
        self,
        table_name: str,
        columns: Sequence[str],
        *,
        name: Optional[str] = None,
    ) -> None:
        """Create an index if it doesn't exist.

        Args:
            table_name: Target table.
            columns: Column names to index.
            name: Index name. Auto-generated from table and columns if omitted.
        """
        index_name = name or f"idx_{table_name}_{'_'.join(columns)}"
        _require_safe_identifier(table_name)
        for col in columns:
            _require_safe_identifier(col)
        _require_safe_identifier(index_name)
        cols = ", ".join(columns)
        sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({cols})"
        self._exec(sql)

    # ------------------------------------------------------------------
    # Data operations
    # ------------------------------------------------------------------

    def bulk_upsert(
        self,
        table_name: str,
        columns: Sequence[str],
        rows: Sequence[tuple],
        *,
        conflict_target: Optional[str] = None,
    ) -> int:
        """Upsert rows in bulk.

        Uses ON CONFLICT DO UPDATE when a conflict_target is provided,
        which updates in-place without deleting the row. Falls back to
        INSERT OR REPLACE when no conflict target is known.

        Args:
            table_name: Target table.
            columns: Column names matching the tuple positions.
            rows: Data tuples.
            conflict_target: Comma-separated conflict columns, e.g. 'id' or
                'fixture_id, identifier, element'. Auto-detected by upsert_models.

        Returns:
            Number of rows upserted.
        """
        if not rows:
            return 0
        _require_safe_identifier(table_name)
        for col in columns:
            _require_safe_identifier(col)
        placeholders = ", ".join("?" * len(columns))
        cols_str = ", ".join(columns)

        if conflict_target:
            conflict_cols = {c.strip() for c in conflict_target.split(",")}
            update_cols = [c for c in columns if c not in conflict_cols]
            if update_cols:
                update_clause = ", ".join(f"{c}=excluded.{c}" for c in update_cols)
                sql = (
                    f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})\n"
                    f"ON CONFLICT({conflict_target}) DO UPDATE SET {update_clause}"
                )
            else:
                sql = f"INSERT OR IGNORE INTO {table_name} ({cols_str}) VALUES ({placeholders})"
        else:
            sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) VALUES ({placeholders})"

        own_conn = self._active_conn is None
        effective = self._active_conn or self._get_connection()
        try:
            effective.executemany(sql, rows)
            if own_conn:
                effective.commit()
        finally:
            if own_conn:
                effective.close()
        return len(rows)

    def upsert_models(
        self,
        table_name: str,
        schema: Type[BaseModel],
        raw_dicts: Sequence[Dict[str, Any]],
        *,
        columns: Optional[Sequence[str]] = None,
        row_builder: Optional[Any] = None,
    ) -> Tuple[int, int]:
        """Validate raw dicts against a Pydantic schema and upsert into SQLite.

        In the default path (no row_builder), every schema field is persisted
        plus an auto-injected ingested_at timestamp.

        Args:
            table_name: Target table.
            schema: Pydantic model class for validation.
            raw_dicts: Raw JSON-like dicts (e.g. from FPL API).
            columns: Explicit column list (when you need extra/fewer cols).
            row_builder: callable(validated_model) -> tuple that builds the
                row tuple matching columns. Required when columns is provided.

        Returns:
            (upserted, skipped) counts.
        """
        rows: List[tuple] = []
        errors: List[Tuple[Any, str]] = []
        timestamp = datetime.now(timezone.utc).isoformat()
        is_custom = columns is not None and row_builder is not None
        column_names: Optional[List[str]] = list(columns) if columns else None
        data_column_names: Optional[List[str]] = None  # column_names minus ingested_at, cached after first row

        # column_names is derived from the first row and reused for all subsequent
        # rows. Derivation is deferred to avoid calling model_dump() twice on
        # the first row — once for columns, once for values.
        for raw in raw_dicts:
            try:
                model = schema.model_validate(raw)
                if is_custom:
                    rows.append(row_builder(model))
                else:
                    dumped = model.model_dump()
                    if column_names is None:
                        column_names = list(dumped.keys()) + ["ingested_at"]
                        data_column_names = column_names[:-1]
                    rows.append(tuple(dumped[c] for c in data_column_names) + (timestamp,))
            except ValidationError as exc:
                errors.append((raw.get("id", "unknown"), str(exc)))

        for entity_id, error_message in errors:
            logger.warning(
                "Skipped invalid %s row (id=%s): %s",
                table_name, entity_id, error_message,
            )

        if column_names is None:
            return (0, len(errors))

        conflict_target: Optional[str] = None
        if not is_custom:
            if hasattr(schema, "GRAIN_CONSTRAINT"):
                conflict_target = _parse_conflict_columns(schema.GRAIN_CONSTRAINT)
            elif "id" in column_names:
                conflict_target = "id"

        self.bulk_upsert(table_name, column_names, rows, conflict_target=conflict_target)
        return (len(rows), len(errors))

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def query(
        self,
        sql: str,
        params: tuple = (),
    ) -> List[Dict[str, Any]]:
        """Execute a read-only query and return results as a list of dicts.

        Args:
            sql: SQL SELECT statement.
            params: Positional parameters for the query.

        Returns:
            List of row dicts.
        """
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _migrate_new_columns(
        self,
        table_name: str,
        schema: Type[BaseModel],
        extra_columns: List[str],
    ) -> None:
        # Add columns present in the schema but absent from the live table.
        # ALTER TABLE ADD COLUMN cannot set NOT NULL without a default, so new
        # fields should be nullable in the Pydantic model. Type changes and
        # column removals are not handled; those require a manual migration.
        #
        # Use _active_conn when inside a transaction so the PRAGMA read sees
        # the same schema state as the preceding CREATE TABLE, without opening
        # a second connection that could observe stale table_info.
        own_conn = self._active_conn is None
        conn = self._active_conn or self._get_connection()
        try:
            existing = {
                row[1]
                for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            }
        finally:
            if own_conn:
                conn.close()

        expected: dict[str, str] = {
            field_name: pydantic_to_sqlite_column(field_name, field_info)
            for field_name, field_info in schema.model_fields.items()
        }
        for col_def in extra_columns:
            col_name = col_def.split()[0]
            expected[col_name] = col_def

        for col_name, col_def in expected.items():
            if col_name not in existing:
                logger.info("Migrating %s: adding column %s", table_name, col_name)
                self._exec(f"ALTER TABLE {table_name} ADD COLUMN {col_def}")

    def _exec(self, sql: str) -> None:
        own_conn = self._active_conn is None
        effective = self._active_conn or self._get_connection()
        try:
            effective.execute(sql)
            if own_conn:
                effective.commit()
        finally:
            if own_conn:
                effective.close()

    # ------------------------------------------------------------------
    # Run audit table
    # ------------------------------------------------------------------

    _RUNS_DDL = (
        "CREATE TABLE IF NOT EXISTS _runs ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  started_at TEXT NOT NULL,"
        "  stage TEXT NOT NULL,"
        "  fetched INTEGER NOT NULL DEFAULT 0,"
        "  upserted INTEGER NOT NULL DEFAULT 0,"
        "  skipped INTEGER NOT NULL DEFAULT 0,"
        "  errors INTEGER NOT NULL DEFAULT 0"
        ")"
    )

    def setup_runs_table(self) -> None:
        """Create the _runs audit table if it does not exist."""
        self._exec(self._RUNS_DDL)

    def record_run(
        self,
        started_at: str,
        stage: str,
        fetched: int,
        upserted: int,
        skipped: int,
        errors: int,
    ) -> None:
        """Insert one audit row for a completed pipeline stage.

        Args:
            started_at: ISO 8601 timestamp of when the run started.
            stage: Pipeline stage name (e.g. 'core', 'fixtures').
            fetched: Number of records fetched from the API.
            upserted: Number of rows successfully written to SQLite.
            skipped: Number of rows skipped due to validation errors.
            errors: Number of fetch or fatal errors.
        """
        sql = (
            "INSERT INTO _runs (started_at, stage, fetched, upserted, skipped, errors) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        own_conn = self._active_conn is None
        effective = self._active_conn or self._get_connection()
        try:
            effective.execute(sql, (started_at, stage, fetched, upserted, skipped, errors))
            if own_conn:
                effective.commit()
        finally:
            if own_conn:
                effective.close()

    # ------------------------------------------------------------------
    # Freshness metadata table
    # ------------------------------------------------------------------

    _METADATA_DDL = (
        "CREATE TABLE IF NOT EXISTS _metadata ("
        "  key TEXT PRIMARY KEY,"
        "  value TEXT,"
        "  updated_at TEXT NOT NULL"
        ")"
    )

    def setup_metadata_table(self) -> None:
        """Create the _metadata key-value table if it does not exist."""
        self._exec(self._METADATA_DDL)

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value pair with the current UTC timestamp.

        Args:
            key: Metadata key (e.g. 'last_successful_run_at').
            value: String value to store.
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        sql = "INSERT OR REPLACE INTO _metadata (key, value, updated_at) VALUES (?, ?, ?)"
        own_conn = self._active_conn is None
        effective = self._active_conn or self._get_connection()
        try:
            effective.execute(sql, (key, value, timestamp))
            if own_conn:
                effective.commit()
        finally:
            if own_conn:
                effective.close()
