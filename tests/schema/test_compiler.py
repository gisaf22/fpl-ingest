"""Tests for schema compiler outputs and contract alignment."""

from __future__ import annotations

import json

import pytest

from fpl_ingest.schema import compile_contract
from fpl_ingest.schema.definition import validate_contract

pytestmark = pytest.mark.integration


def test_compiled_contract_outputs_share_the_same_table_surface(tmp_path):
    contract = compile_contract()

    assert set(contract.schema_contract["tables"]) == set(contract.validation_contract["tables"])
    assert set(contract.schema_contract["tables"]) == set(contract.test_contracts["tables"])
    assert contract.schema_contract["schema_version"] == contract.validation_contract["schema_version"]
    assert contract.schema_contract["schema_version"] == contract.test_contracts["schema_version"]


def test_validate_contract_passes_for_db_created_from_compiled_contract(contract_db):

    result = validate_contract(contract_db)

    assert result.status == "valid"
    assert result.missing_tables == []
    assert result.missing_columns == {}
    assert result.extra_columns == {}
    assert result.type_mismatches == {}
    assert result.nullability_mismatches == {}
    assert result.primary_key_mismatches == {}
    assert result.unique_constraint_mismatches == {}
    assert result.index_mismatches == {}


def test_compiled_contract_payloads_are_json_serialisable():
    contract = compile_contract()

    json.dumps(contract.schema_contract)
    json.dumps(contract.validation_contract)
    json.dumps(contract.test_contracts)


def test_checked_in_contract_artifacts_match_compiled_outputs():
    from fpl_ingest.schema.compiler import (
        DDL_CONTRACT_ARTIFACT_PATH,
        SCHEMA_CONTRACT_ARTIFACT_PATH,
        VALIDATION_CONTRACT_ARTIFACT_PATH,
    )
    contract = compile_contract()

    assert json.loads(SCHEMA_CONTRACT_ARTIFACT_PATH.read_text(encoding="utf-8")) == contract.schema_contract
    assert json.loads(VALIDATION_CONTRACT_ARTIFACT_PATH.read_text(encoding="utf-8")) == contract.validation_contract
    assert DDL_CONTRACT_ARTIFACT_PATH.read_text(encoding="utf-8") == contract.ddl_contract


def test_validate_contract_reports_extra_column_drift(contract_db):
    import sqlite3
    with sqlite3.connect(contract_db) as conn:
        conn.execute("ALTER TABLE players ADD COLUMN drift_col INTEGER")

    result = validate_contract(contract_db)

    assert result.status == "drift"
    assert result.extra_columns["players"] == ["drift_col"]


def test_validate_contract_fails_when_required_table_missing(contract_db):
    import sqlite3
    with sqlite3.connect(contract_db) as conn:
        conn.execute("DROP TABLE teams")

    result = validate_contract(contract_db)

    assert result.status == "invalid"
    assert "teams" in result.missing_tables


def test_validate_contract_fails_when_required_column_type_drifts(tmp_path):
    import sqlite3
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
