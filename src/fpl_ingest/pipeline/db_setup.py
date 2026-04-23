"""Database setup — registers all pipeline tables, indexes, and constraints."""

from __future__ import annotations

from fpl_ingest.contract import compile_contract
from fpl_ingest.storage.store import SQLiteStore


def setup_store(store: SQLiteStore) -> None:
    """Register all pipeline tables from the compiled public contract."""
    contract = compile_contract()
    for table in contract.tables.values():
        store.register_contract_table(table)

    store.setup_runs_table()
    store.setup_metadata_table()
