"""Generic SQLite storage for FPL Pydantic models.

Project-agnostic persistence layer. Creates tables from Pydantic schemas,
validates incoming data, and bulk-upserts rows.

Usage:
    from fpl_ingest import SQLiteStore, PlayerModel

    store = SQLiteStore("fpl.db")
    store.register_table("players", PlayerModel)
    store.upsert_models("players", PlayerModel, raw_dicts)
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

from pydantic import BaseModel, ValidationError

from fpl_ingest.models import schema_to_create_table

logger = logging.getLogger(__name__)


def _parse_conflict_columns(unique_constraint: str) -> str:
    """Extract column list from 'UNIQUE(col1, col2)' → 'col1, col2'."""
    m = re.search(r"UNIQUE\s*\(([^)]+)\)", unique_constraint, re.IGNORECASE)
    return m.group(1).strip() if m else unique_constraint


class SQLiteStore:
    """Generic SQLite store that persists FPL Pydantic models."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._registered_tables: Dict[str, Type[BaseModel]] = {}

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def get_connection(self) -> sqlite3.Connection:
        """Get a new database connection."""
        return sqlite3.connect(self.db_path)

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
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        """Create a table from a Pydantic schema (if it doesn't exist).

        Args:
            table_name: SQL table name.
            schema: Pydantic model whose fields become columns.
            extra_columns: Additional column definitions not on the model.
            unique_constraint: Optional UNIQUE constraint clause.
            conn: Reuse an existing connection. If None, opens and closes one.
        """
        all_extra = list(extra_columns or []) + ["ingested_at TEXT"]
        sql = schema_to_create_table(
            table_name, schema,
            extra_columns=all_extra,
            unique_constraint=unique_constraint,
        )
        self._exec(sql, conn=conn)
        self._registered_tables[table_name] = schema

    def create_index(
        self,
        table_name: str,
        columns: Sequence[str],
        *,
        name: Optional[str] = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        """Create an index if it doesn't exist.

        Args:
            table_name: Target table.
            columns: Column names to index.
            name: Index name. Auto-generated if omitted.
            conn: Reuse an existing connection.
        """
        idx_name = name or f"idx_{table_name}_{'_'.join(columns)}"
        cols = ", ".join(columns)
        sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({cols})"
        self._exec(sql, conn=conn)

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
        conn: Optional[sqlite3.Connection] = None,
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
            conn: Reuse an existing connection. Caller is responsible for commit.

        Returns:
            Number of rows upserted.
        """
        if not rows:
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

        own_conn = conn is None
        if own_conn:
            conn = self.get_connection()
        try:
            conn.executemany(sql, rows)
            if own_conn:
                conn.commit()
        finally:
            if own_conn:
                conn.close()
        return len(rows)

    def upsert_models(
        self,
        table_name: str,
        schema: Type[BaseModel],
        raw_dicts: Sequence[Dict[str, Any]],
        *,
        columns: Optional[Sequence[str]] = None,
        row_builder: Optional[Any] = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> Tuple[int, int]:
        """Validate raw dicts against a Pydantic schema and upsert.

        In the default path (no row_builder), every schema field is persisted
        plus an auto-injected ingested_at timestamp.

        Args:
            table_name: Target table.
            schema: Pydantic model class for validation.
            raw_dicts: Raw JSON-like dicts (e.g. from FPL API).
            columns: Explicit column list (when you need extra/fewer cols).
            row_builder: ``callable(validated_model) -> tuple`` that builds
                the row tuple matching *columns*. Required when *columns*
                is provided.
            conn: Reuse an existing connection. Caller commits.

        Returns:
            ``(inserted, skipped)`` counts.
        """
        rows: List[tuple] = []
        errors: List[Tuple[Any, str]] = []
        ts = datetime.now(timezone.utc).isoformat()
        use_custom = columns is not None and row_builder is not None
        _cols: Optional[List[str]] = list(columns) if columns else None
        _data_cols: Optional[List[str]] = None  # _cols minus ingested_at, cached after first row

        for raw in raw_dicts:
            try:
                model = schema.model_validate(raw)
                if use_custom:
                    rows.append(row_builder(model))
                else:
                    d = model.model_dump()
                    if _cols is None:
                        _cols = list(d.keys()) + ["ingested_at"]
                        _data_cols = _cols[:-1]
                    rows.append(tuple(d[c] for c in _data_cols) + (ts,))
            except ValidationError as e:
                errors.append((raw.get("id", "unknown"), str(e)))

        if errors:
            logger.warning(
                "Skipped %d %s rows with invalid schema: %s...",
                len(errors), table_name, errors[:3],
            )

        if _cols is None:
            return (0, len(errors))

        conflict_target: Optional[str] = None
        if not use_custom:
            if hasattr(schema, "DEFAULT_UNIQUE"):
                conflict_target = _parse_conflict_columns(schema.DEFAULT_UNIQUE)
            elif "id" in _cols:
                conflict_target = "id"

        self.bulk_upsert(table_name, _cols, rows, conflict_target=conflict_target, conn=conn)
        return (len(rows), len(errors))

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def query(
        self,
        sql: str,
        params: tuple = (),
    ) -> List[Dict[str, Any]]:
        """Execute a read query and return list of dicts."""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _exec(
        self,
        sql: str,
        *,
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        own_conn = conn is None
        if own_conn:
            conn = self.get_connection()
        try:
            conn.execute(sql)
            if own_conn:
                conn.commit()
        finally:
            if own_conn:
                conn.close()
