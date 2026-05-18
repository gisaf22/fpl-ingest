"""Tests for cross-table integrity checks.

Each named check function is tested in isolation against an in-memory
SQLite database, and the orchestrating run_integrity_checks is tested
for its raise / warn / pass behaviour.
"""

from __future__ import annotations

import logging

import pytest

pytestmark = pytest.mark.integration

from fpl_ingest.load.integrity import (
    IntegrityViolation,
    check_player_count_matches,
    check_player_histories_elements_exist,
    check_player_histories_fixtures_exist,
    check_players_teams_exist,
    run_integrity_checks,
)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def _insert(conn: sqlite3.Connection, table: str, rows: list[tuple]) -> None:
    cols = {
        "players": "(id, team)",
        "teams": "(id)",
        "fixtures": "(id)",
        "player_histories": "(element_id, fixture, round)",
    }[table]
    placeholders = ",".join("?" * len(rows[0]))
    conn.executemany(f"INSERT INTO {table} {cols} VALUES ({placeholders})", rows)
    conn.commit()


# ---------------------------------------------------------------------------
# check_player_histories_elements_exist
# ---------------------------------------------------------------------------


class TestCheckPlayerHistoriesElementsExist:

    def test_no_violations_returns_empty(self, in_memory_conn):
        _insert(in_memory_conn, "players", [(1, 10)])
        _insert(in_memory_conn, "player_histories", [(1, 101, 1)])
        assert check_player_histories_elements_exist(in_memory_conn) == []

    def test_orphan_element_id_returned(self, in_memory_conn):
        _insert(in_memory_conn, "players", [(1, 10)])
        _insert(in_memory_conn, "player_histories", [(99, 101, 1)])  # 99 not in players
        result = check_player_histories_elements_exist(in_memory_conn)
        assert 99 in result

    def test_empty_tables_returns_empty(self, in_memory_conn):
        assert check_player_histories_elements_exist(in_memory_conn) == []


# ---------------------------------------------------------------------------
# check_player_histories_fixtures_exist
# ---------------------------------------------------------------------------


class TestCheckPlayerHistoriesFixturesExist:

    def test_no_violations_returns_empty(self, in_memory_conn):
        _insert(in_memory_conn, "fixtures", [(101,)])
        _insert(in_memory_conn, "player_histories", [(1, 101, 1)])
        assert check_player_histories_fixtures_exist(in_memory_conn) == []

    def test_orphan_fixture_id_returned(self, in_memory_conn):
        _insert(in_memory_conn, "player_histories", [(1, 999, 1)])  # 999 not in fixtures
        result = check_player_histories_fixtures_exist(in_memory_conn)
        assert 999 in result

    def test_empty_tables_returns_empty(self, in_memory_conn):
        assert check_player_histories_fixtures_exist(in_memory_conn) == []


# ---------------------------------------------------------------------------
# check_players_teams_exist
# ---------------------------------------------------------------------------


class TestCheckPlayersTeamsExist:

    def test_no_violations_returns_empty(self, in_memory_conn):
        _insert(in_memory_conn, "teams", [(10,)])
        _insert(in_memory_conn, "players", [(1, 10)])
        assert check_players_teams_exist(in_memory_conn) == []

    def test_orphan_team_id_returned(self, in_memory_conn):
        _insert(in_memory_conn, "players", [(1, 77)])  # 77 not in teams
        result = check_players_teams_exist(in_memory_conn)
        assert 77 in result

    def test_empty_tables_returns_empty(self, in_memory_conn):
        assert check_players_teams_exist(in_memory_conn) == []


# ---------------------------------------------------------------------------
# check_player_count_matches
# ---------------------------------------------------------------------------


class TestCheckPlayerCountMatches:

    def test_counts_match(self, in_memory_conn):
        _insert(in_memory_conn, "players", [(1, 10), (2, 10)])
        _insert(in_memory_conn, "player_histories", [(1, 101, 1), (2, 102, 1)])
        hist, players = check_player_count_matches(in_memory_conn)
        assert hist == 2
        assert players == 2

    def test_mismatch_detected(self, in_memory_conn):
        _insert(in_memory_conn, "players", [(1, 10), (2, 10), (3, 10)])
        _insert(in_memory_conn, "player_histories", [(1, 101, 1)])
        hist, players = check_player_count_matches(in_memory_conn)
        assert hist == 1
        assert players == 3

    def test_empty_tables(self, in_memory_conn):
        hist, players = check_player_count_matches(in_memory_conn)
        assert hist == 0
        assert players == 0


# ---------------------------------------------------------------------------
# run_integrity_checks orchestrator
# ---------------------------------------------------------------------------


class TestRunIntegrityChecks:

    def test_clean_db_passes(self, in_memory_conn):
        _insert(in_memory_conn, "teams", [(10,)])
        _insert(in_memory_conn, "players", [(1, 10)])
        _insert(in_memory_conn, "fixtures", [(101,)])
        _insert(in_memory_conn, "player_histories", [(1, 101, 1)])
        run_integrity_checks(in_memory_conn)  # should not raise

    def test_orphan_element_raises(self, in_memory_conn):
        _insert(in_memory_conn, "player_histories", [(99, 101, 1)])  # no matching player
        with pytest.raises(IntegrityViolation, match="element_id"):
            run_integrity_checks(in_memory_conn)

    def test_orphan_fixture_raises(self, in_memory_conn):
        _insert(in_memory_conn, "teams", [(10,)])
        _insert(in_memory_conn, "players", [(1, 10)])
        _insert(in_memory_conn, "player_histories", [(1, 999, 1)])  # no matching fixture
        with pytest.raises(IntegrityViolation, match="fixture"):
            run_integrity_checks(in_memory_conn)

    def test_orphan_team_raises(self, in_memory_conn):
        _insert(in_memory_conn, "players", [(1, 77)])  # no matching team
        with pytest.raises(IntegrityViolation, match="team"):
            run_integrity_checks(in_memory_conn)

    def test_count_mismatch_warns_not_raises(self, in_memory_conn, caplog):
        _insert(in_memory_conn, "teams", [(10,)])
        _insert(in_memory_conn, "players", [(1, 10), (2, 10)])
        _insert(in_memory_conn, "fixtures", [(101,)])
        _insert(in_memory_conn, "player_histories", [(1, 101, 1)])  # only player 1 has history
        with caplog.at_level(logging.WARNING, logger="fpl_ingest.load.integrity"):
            run_integrity_checks(in_memory_conn)  # must not raise
        assert any("count" in r.message.lower() or "mismatch" in r.message.lower() for r in caplog.records)

    def test_empty_db_passes(self, in_memory_conn):
        run_integrity_checks(in_memory_conn)  # all empty — vacuously correct
