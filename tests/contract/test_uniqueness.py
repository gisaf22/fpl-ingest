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


@pytest.mark.parametrize("probe", compile_contract().test_contracts["uniqueness_probes"])
def test_compiled_unique_constraints_exist_in_sqlite(tmp_path, probe):
    db_path = _build_contract_db(tmp_path / "contract.db")

    with sqlite3.connect(db_path) as conn:
        indexes = conn.execute(f"PRAGMA index_list({probe['table']})").fetchall()
        unique_indexes = []
        for row in indexes:
            if row[2]:
                info = conn.execute(f'PRAGMA index_info("{row[1]}")').fetchall()
                unique_indexes.append(tuple(column[2] for column in sorted(info, key=lambda item: item[0])))

    assert tuple(probe["columns"]) in unique_indexes


def test_compiled_primary_keys_exist_in_sqlite(tmp_path):
    db_path = _build_contract_db(tmp_path / "contract.db")
    contract = compile_contract()

    with sqlite3.connect(db_path) as conn:
        for table_name, table in contract.tables.items():
            if not table.primary_key:
                continue
            info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            actual = tuple(row[1] for row in sorted(info, key=lambda row: row[5]) if row[5])
            assert actual == table.primary_key


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
