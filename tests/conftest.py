"""Shared pytest fixtures for the fpl-ingest test suite."""

from __future__ import annotations

import sqlite3

import pytest

from fpl_ingest.load.db_setup import setup_store
from fpl_ingest.load.store import SQLiteStore


@pytest.fixture
def contract_db(tmp_path):
    """Build and return a fully initialised contract database path.

    Creates a SQLiteStore, runs setup_store inside a transaction, and returns
    the path. Replaces the triplicated _build_contract_db helper previously
    inlined in tests/schema/test_definition.py, test_compiler.py, and
    test_drift.py.
    """
    path = tmp_path / "contract.db"
    store = SQLiteStore(path)
    with store.transaction():
        setup_store(store)
    return path


@pytest.fixture
def in_memory_conn():
    """Create an in-memory SQLite connection with the 4-table integrity schema.

    Used by tests/load/test_integrity.py. Each test gets a fresh connection.
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE players (id INTEGER PRIMARY KEY, team INTEGER NOT NULL);
        CREATE TABLE teams   (id INTEGER PRIMARY KEY);
        CREATE TABLE fixtures(id INTEGER PRIMARY KEY);
        CREATE TABLE player_histories (
            element_id INTEGER NOT NULL,
            fixture    INTEGER NOT NULL,
            round      INTEGER NOT NULL
        );
        """
    )
    conn.commit()
    yield conn
    conn.close()
