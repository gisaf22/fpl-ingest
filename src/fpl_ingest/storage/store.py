"""SQLite persistence layer for compiled public contract tables."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Sequence, Tuple, Type

from pydantic import BaseModel, ValidationError

from fpl_ingest.contract.compiler import CompiledTable
from fpl_ingest.domain.execution_state import PipelineExecutionState
from fpl_ingest.domain.run_status import classify_run

if TYPE_CHECKING:
    from fpl_ingest.pipeline.stage_result import StageResult

logger = logging.getLogger(__name__)


class SQLiteStore:
    """SQLite store that persists FPL Pydantic models.

    Manages a single shared connection within a transaction block. Outside
    a transaction, each operation opens and closes its own connection.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        execution_state: PipelineExecutionState | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._registered_tables: Dict[str, CompiledTable] = {}
        self._active_conn: Optional[sqlite3.Connection] = None
        self._execution_state = execution_state

    def _writes_allowed(self) -> bool:
        return self._execution_state is None or not self._execution_state.is_failed

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

    def register_contract_table(self, table: CompiledTable) -> None:
        """Create or migrate a table from the compiled public contract."""
        self._exec(table.create_table_sql)
        self._migrate_contract_columns(table)
        for statement in table.index_sql:
            self._exec(statement)
        self._registered_tables[table.name] = table

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
        if not self._writes_allowed():
            return 0
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
        if not self._writes_allowed():
            return (0, 0)
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
                if row_builder is not None:
                    rows.append(row_builder(model))
                else:
                    dumped = model.model_dump()
                    if column_names is None:
                        column_names = list(dumped.keys()) + ["ingested_at"]
                        data_column_names = column_names[:-1]
                    assert data_column_names is not None
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
            contract_table = self._registered_tables.get(table_name)
            if contract_table is not None:
                if contract_table.unique_key:
                    conflict_target = ", ".join(contract_table.unique_key)
                elif contract_table.primary_key:
                    conflict_target = ", ".join(contract_table.primary_key)
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

    def _migrate_contract_columns(self, table: CompiledTable) -> None:
        # Add contract columns missing from an existing table. Phase 2 treats
        # nullability as authoritative, so any missing NOT NULL or PRIMARY KEY
        # column requires a manual migration instead of a silent nullable add.
        own_conn = self._active_conn is None
        conn = self._active_conn or self._get_connection()
        try:
            existing = {
                row[1]
                for row in conn.execute(f"PRAGMA table_info({table.name})").fetchall()
            }
        finally:
            if own_conn:
                conn.close()

        for column in table.columns:
            if column.name in existing:
                continue
            if column.primary_key or not column.nullable:
                raise RuntimeError(
                    f"Manual migration required for {table.name}.{column.name}: "
                    "compiled contract added a non-nullable or primary-key column."
                )
            logger.info("Migrating %s: adding column %s", table.name, column.name)
            self._exec(f"ALTER TABLE {table.name} ADD COLUMN {column.alter_sql}")

    def _exec(self, sql: str) -> None:
        if not self._writes_allowed():
            return
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
        "  validated INTEGER NOT NULL DEFAULT 0,"
        "  written INTEGER NOT NULL DEFAULT 0,"
        "  skipped INTEGER NOT NULL DEFAULT 0,"
        "  errors INTEGER NOT NULL DEFAULT 0,"
        "  status TEXT"
        ")"
    )

    def setup_runs_table(self) -> None:
        """Create the _runs audit table if it does not exist."""
        self._exec(self._RUNS_DDL)
        self._migrate_run_columns()

    def record_run(
        self,
        started_at: str,
        stage: str,
        fetched: int,
        validated: int,
        written: int,
        skipped: int,
        errors: int,
    ) -> None:
        """Insert one audit row for a completed pipeline stage.

        Args:
            started_at: ISO 8601 timestamp of when the run started.
            stage: Pipeline stage name (e.g. 'core', 'fixtures').
            fetched: Number of records fetched from the API.
            validated: Number of records that passed schema validation.
            written: Number of validated rows successfully written to SQLite.
            skipped: Number of rows skipped due to validation errors.
            errors: Number of fetch or fatal errors.
        """
        if not self._writes_allowed():
            return
        sql = (
            "INSERT INTO _runs (started_at, stage, fetched, validated, written, skipped, errors, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        own_conn = self._active_conn is None
        effective = self._active_conn or self._get_connection()
        try:
            effective.execute(sql, (started_at, stage, fetched, validated, written, skipped, errors, None))
            if own_conn:
                effective.commit()
        finally:
            if own_conn:
                effective.close()

    def record_stage_result(self, started_at: str, result: StageResult) -> None:
        """Persist one completed stage result into the _runs audit table."""
        self.record_run(
            started_at,
            result.stage,
            result.fetched,
            result.validated,
            result.written,
            result.skipped,
            result.errors,
        )

    def finalize_run(
        self,
        started_at: str,
        status: str | None = None,
        *,
        errors: int = 0,
        skipped: int = 0,
        strict_mode: bool = False,
        metadata_updates: Optional[Dict[str, str]] = None,
    ) -> str:
        """Atomically persist the terminal run status and any success metadata."""
        own_conn = self._active_conn is None
        if own_conn:
            with self.transaction():
                return self.finalize_run(
                    started_at,
                    status,
                    errors=errors,
                    skipped=skipped,
                    strict_mode=strict_mode,
                    metadata_updates=metadata_updates,
                )

        resolved_status = status or classify_run(errors=errors, skipped=skipped, strict_mode=strict_mode)
        self._update_run_status(started_at, resolved_status)
        for key, value in (metadata_updates or {}).items():
            self.set_metadata(key, value)
        return resolved_status

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

    def _migrate_run_columns(self) -> None:
        """Add any missing audit columns on existing _runs tables."""
        own_conn = self._active_conn is None
        conn = self._active_conn or self._get_connection()
        try:
            existing = {
                row[1]
                for row in conn.execute("PRAGMA table_info(_runs)").fetchall()
            }
        finally:
            if own_conn:
                conn.close()

        if "status" not in existing:
            logger.info("Migrating _runs: adding column status")
            self._exec("ALTER TABLE _runs ADD COLUMN status TEXT")
        if "validated" not in existing:
            logger.info("Migrating _runs: adding column validated")
            self._exec("ALTER TABLE _runs ADD COLUMN validated INTEGER NOT NULL DEFAULT 0")
            if "upserted" in existing:
                self._exec("UPDATE _runs SET validated = fetched - skipped WHERE validated = 0")
        if "written" not in existing:
            logger.info("Migrating _runs: adding column written")
            if "upserted" in existing:
                self._exec("ALTER TABLE _runs ADD COLUMN written INTEGER NOT NULL DEFAULT 0")
                self._exec("UPDATE _runs SET written = upserted WHERE written = 0")
            else:
                self._exec("ALTER TABLE _runs ADD COLUMN written INTEGER NOT NULL DEFAULT 0")

    def _update_run_status(self, started_at: str, status: str) -> None:
        """Persist the resolved terminal status for every stage row in a run."""
        sql = "UPDATE _runs SET status = ? WHERE started_at = ?"
        own_conn = self._active_conn is None
        effective = self._active_conn or self._get_connection()
        try:
            effective.execute(sql, (status, started_at))
            if own_conn:
                effective.commit()
        finally:
            if own_conn:
                effective.close()

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value pair with the current UTC timestamp.

        Args:
            key: Metadata key (e.g. 'last_successful_run_at').
            value: String value to store.
        """
        if not self._writes_allowed():
            return
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
