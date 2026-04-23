from __future__ import annotations

import json
import sqlite3

import pytest

from fpl_ingest.contract import (
    DDL_CONTRACT_ARTIFACT_PATH,
    SCHEMA_CONTRACT_ARTIFACT_PATH,
    VALIDATION_CONTRACT_ARTIFACT_PATH,
    compile_contract,
)
from fpl_ingest.pipeline import setup_store
from fpl_ingest.domain.schema import validate_contract
from fpl_ingest.storage.store import SQLiteStore

pytestmark = pytest.mark.unit


def _build_contract_db(path):
    store = SQLiteStore(path)
    with store.transaction():
        setup_store(store)
    return path


def test_checked_in_contract_artifacts_match_compiled_outputs():
    contract = compile_contract()

    assert json.loads(SCHEMA_CONTRACT_ARTIFACT_PATH.read_text(encoding="utf-8")) == contract.schema_contract
    assert json.loads(VALIDATION_CONTRACT_ARTIFACT_PATH.read_text(encoding="utf-8")) == contract.validation_contract
    assert DDL_CONTRACT_ARTIFACT_PATH.read_text(encoding="utf-8") == contract.ddl_contract


def test_validate_contract_reports_extra_column_drift(tmp_path):
    db_path = _build_contract_db(tmp_path / "drift.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("ALTER TABLE players ADD COLUMN drift_col INTEGER")

    result = validate_contract(db_path)

    assert result.status == "drift"
    assert result.extra_columns["players"] == ["drift_col"]


def test_validate_contract_fails_when_required_table_missing(tmp_path):
    db_path = _build_contract_db(tmp_path / "missing_table.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE teams")

    result = validate_contract(db_path)

    assert result.status == "invalid"
    assert "teams" in result.missing_tables


def test_validate_contract_fails_when_required_column_type_drifts(tmp_path):
    contract = compile_contract()
    db_path = tmp_path / "type_drift.db"

    with sqlite3.connect(db_path) as conn:
        for table_name, table in contract.tables.items():
            columns_sql = []
            for column in table.columns:
                sqlite_type = "TEXT" if table_name == "teams" and column.name == "strength" else column.sqlite_type
                parts = [column.name, sqlite_type]
                if not column.nullable:
                    parts.append("NOT NULL")
                if column.primary_key:
                    parts.append("PRIMARY KEY")
                columns_sql.append(" ".join(parts))
            if table.unique_key:
                columns_sql.append(f"UNIQUE({', '.join(table.unique_key)})")
            conn.execute(f"CREATE TABLE {table_name} ({', '.join(columns_sql)})")
            for statement in table.index_sql:
                conn.execute(statement)

    result = validate_contract(db_path)

    assert result.status == "invalid"
    assert result.type_mismatches["teams"][0].column == "strength"
