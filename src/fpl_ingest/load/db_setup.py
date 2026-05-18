"""Database schema setup for the start of each ingest run.

Compiles the public contract and registers every table, index, and constraint
with the SQLiteStore before any stage writes data. Also creates the internal
``_runs`` audit table and ``_metadata`` key-value table. Called once per run
inside a transaction before the core stage begins.
"""

from __future__ import annotations

from fpl_ingest.schema import compile_contract
from fpl_ingest.load.store import SQLiteStore


def setup_store(store: SQLiteStore) -> None:
    """Register all public tables from the compiled public contract."""
    contract = compile_contract()
    for table in contract.tables.values():
        store.register_contract_table(table)

    store.setup_runs_table()
    store.setup_metadata_table()
