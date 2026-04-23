from __future__ import annotations

import sqlite3

import pytest

from fpl_ingest.contract import compile_contract
from fpl_ingest.pipeline import setup_store
from fpl_ingest.domain.schema import validate_contract
from fpl_ingest.storage.store import SQLiteStore

pytestmark = pytest.mark.unit


def _build_contract_db(path):
    store = SQLiteStore(path)
    with store.transaction():
        setup_store(store)
    return path


@pytest.mark.parametrize("probe", compile_contract().test_contracts["nullability_probes"])
def test_compiled_contract_marks_non_nullable_columns_as_not_null_in_sqlite(tmp_path, probe):
    db_path = _build_contract_db(tmp_path / "contract.db")

    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]: row
            for row in conn.execute(f"PRAGMA table_info({probe['table']})").fetchall()
        }

    assert columns[probe["column"]][3] == 1


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
