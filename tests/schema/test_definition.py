"""Tests for schema version mismatch detection, nullability, and uniqueness constraints."""

from __future__ import annotations

import json
import sqlite3

import pytest

from fpl_ingest.schema import compile_contract
from fpl_ingest.schema.definition import SCHEMA_VERSION, ValidationResult, validate_contract

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Version mismatch detection
# ---------------------------------------------------------------------------


class TestVersionMismatchDetection:

    def _stub_result(self) -> ValidationResult:
        return ValidationResult(
            status="invalid",
            schema_version=SCHEMA_VERSION,
            db_path="",
            checked_at="",
        )

    def test_matching_versions_produce_no_mismatch(self, tmp_path):
        artifact = tmp_path / "schema_contract.json"
        artifact.write_text(
            json.dumps({"schema_version": SCHEMA_VERSION, "tables": {}}),
            encoding="utf-8",
        )
        from unittest.mock import patch
        with patch("fpl_ingest.schema.definition.CONTRACT_ARTIFACT_PATH", artifact):
            with patch("fpl_ingest.schema.validation.validate_contract_db", return_value=self._stub_result()):
                result = validate_contract(None)

        assert result.version_mismatch is False
        assert result.artifact_version is None

    def test_stale_artifact_version_sets_mismatch_flag(self, tmp_path):
        artifact = tmp_path / "schema_contract.json"
        artifact.write_text(
            json.dumps({"schema_version": "0.9.0", "tables": {}}),
            encoding="utf-8",
        )
        from unittest.mock import patch
        with patch("fpl_ingest.schema.definition.CONTRACT_ARTIFACT_PATH", artifact):
            with patch("fpl_ingest.schema.validation.validate_contract_db", return_value=self._stub_result()):
                result = validate_contract(None)

        assert result.version_mismatch is True
        assert result.artifact_version == "0.9.0"

    def test_missing_artifact_does_not_set_mismatch(self, tmp_path):
        missing = tmp_path / "does_not_exist.json"
        from unittest.mock import patch
        with patch("fpl_ingest.schema.definition.CONTRACT_ARTIFACT_PATH", missing):
            with patch("fpl_ingest.schema.validation.validate_contract_db", return_value=self._stub_result()):
                result = validate_contract(None)

        assert result.version_mismatch is False

# ---------------------------------------------------------------------------
# Nullability constraints
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.parametrize("probe", compile_contract().test_contracts["nullability_probes"])
def test_compiled_contract_marks_non_nullable_columns_as_not_null_in_sqlite(contract_db, probe):
    with sqlite3.connect(contract_db) as conn:
        columns = {
            row[1]: row
            for row in conn.execute(f"PRAGMA table_info({probe['table']})").fetchall()
        }

    assert columns[probe["column"]][3] == 1


@pytest.mark.integration
def test_validate_contract_detects_nullability_drift(tmp_path):
    contract = compile_contract()
    probe = contract.test_contracts["nullability_probes"][0]
    db_path = tmp_path / "nullability_drift.db"

    with sqlite3.connect(db_path) as conn:
        for table_name, table in contract.tables.items():
            columns_sql = []
            for column in table.columns:
                if table_name == probe["table"] and column.name == probe["column"]:
                    columns_sql.append(f"{column.name} {column.sqlite_type}")
                else:
                    columns_sql.append(column.column_sql)
            if table.unique_key:
                columns_sql.append(f"UNIQUE({', '.join(table.unique_key)})")
            conn.execute(f"CREATE TABLE {table_name} ({', '.join(columns_sql)})")
            for statement in table.index_sql:
                conn.execute(statement)

    result = validate_contract(db_path)

    assert result.status == "invalid"
    assert probe["table"] in result.nullability_mismatches
    assert result.nullability_mismatches[probe["table"]][0].name == probe["column"]


# ---------------------------------------------------------------------------
# Uniqueness constraints
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.parametrize("probe", compile_contract().test_contracts["uniqueness_probes"])
def test_compiled_unique_constraints_exist_in_sqlite(contract_db, probe):
    with sqlite3.connect(contract_db) as conn:
        indexes = conn.execute(f"PRAGMA index_list({probe['table']})").fetchall()
        unique_indexes = []
        for row in indexes:
            if row[2]:
                info = conn.execute(f'PRAGMA index_info("{row[1]}")').fetchall()
                unique_indexes.append(tuple(column[2] for column in sorted(info, key=lambda item: item[0])))

    assert tuple(probe["columns"]) in unique_indexes


@pytest.mark.integration
def test_compiled_primary_keys_exist_in_sqlite(contract_db):
    contract = compile_contract()

    with sqlite3.connect(contract_db) as conn:
        for table_name, table in contract.tables.items():
            if not table.primary_key:
                continue
            info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            actual = tuple(row[1] for row in sorted(info, key=lambda row: row[5]) if row[5])
            assert actual == table.primary_key


@pytest.mark.integration
def test_validate_contract_detects_missing_unique_constraint(tmp_path):
    contract = compile_contract()
    probe = contract.test_contracts["uniqueness_probes"][0]
    db_path = tmp_path / "unique_drift.db"

    with sqlite3.connect(db_path) as conn:
        for table_name, table in contract.tables.items():
            columns_sql = [column.column_sql for column in table.columns]
            if table_name != probe["table"] and table.unique_key:
                columns_sql.append(f"UNIQUE({', '.join(table.unique_key)})")
            conn.execute(f"CREATE TABLE {table_name} ({', '.join(columns_sql)})")
            for statement in table.index_sql:
                conn.execute(statement)

    result = validate_contract(db_path)

    assert result.status == "invalid"
    assert probe["table"] in result.unique_constraint_mismatches
