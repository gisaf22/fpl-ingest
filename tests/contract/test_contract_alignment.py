from __future__ import annotations

import json

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


def test_compiled_contract_outputs_share_the_same_table_surface(tmp_path):
    contract = compile_contract()

    assert set(contract.schema_contract["tables"]) == set(contract.validation_contract["tables"])
    assert set(contract.schema_contract["tables"]) == set(contract.test_contracts["tables"])
    assert contract.schema_contract["schema_version"] == contract.validation_contract["schema_version"]
    assert contract.schema_contract["schema_version"] == contract.test_contracts["schema_version"]


def test_validate_contract_passes_for_db_created_from_compiled_contract(tmp_path):
    db_path = _build_contract_db(tmp_path / "contract.db")

    result = validate_contract(db_path)

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
