"""Generic SQLite storage for FPL Pydantic models.

Project-agnostic persistence layer. Creates tables from Pydantic schemas,
validates incoming data, and bulk-upserts rows.

Usage:
    from fpl_ingest import SQLiteStore, PlayerModel

    store = SQLiteStore("fpl.db")
    store.register_table("players", PlayerModel)
    store.upsert_from_api("players", PlayerModel, raw_dicts)
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

from pydantic import BaseModel, ValidationError

from fpl_ingest.models import schema_to_create_table

logger = logging.getLogger(__name__)


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
        sql = schema_to_create_table(
            table_name, schema,
            extra_columns=extra_columns,
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
        conn: Optional[sqlite3.Connection] = None,
    ) -> int:
        """INSERT OR REPLACE rows in bulk.

        Args:
            table_name: Target table.
            columns: Column names matching the tuple positions.
            rows: Data tuples.
            conn: Reuse an existing connection. Caller is responsible for commit.

        Returns:
            Number of rows upserted.
        """
        if not rows:
            return 0
        placeholders = ", ".join("?" * len(columns))
        cols = ", ".join(columns)
        sql = f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"

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

        If *columns* and *row_builder* are omitted, every schema field is
        persisted using ``model.model_dump()``.

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

        use_custom = columns is not None and row_builder is not None

        for raw in raw_dicts:
            try:
                model = schema.model_validate(raw)
                if use_custom:
                    rows.append(row_builder(model))
                else:
                    d = model.model_dump()
                    if columns is None:
                        columns = list(d.keys())
                    rows.append(tuple(d[c] for c in columns))
            except ValidationError as e:
                errors.append((raw.get("id", "unknown"), str(e)))

        if errors:
            logger.warning(
                f"Skipped {len(errors)} {table_name} rows with invalid schema: "
                f"{errors[:3]}..."
            )

        if columns is None:
            return (0, len(errors))

        self.bulk_upsert(table_name, columns, rows, conn=conn)
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
