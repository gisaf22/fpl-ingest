"""Cross-table integrity assertions for the fpl-ingest SQLite schema.

Each public function takes a sqlite3.Connection and returns the set of
violating IDs (empty = no violation).  The orchestrating function
``run_integrity_checks`` calls them all and raises IntegrityViolation on
hard failures; a row-count mismatch is logged as a WARNING and does not raise.

These checks run only after a fully successful ingest run and are called from
the runner's finalization path.  They are deliberately pure (no side effects
beyond logging) so each one is independently unit-testable.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)


class IntegrityViolation(RuntimeError):
    """Raised when a cross-table referential integrity check fails."""


# ---------------------------------------------------------------------------
# Named check functions
# ---------------------------------------------------------------------------


def check_player_histories_elements_exist(conn: sqlite3.Connection) -> list[int]:
    """Return element_ids in player_histories that have no matching row in players.

    An empty list means the check passes.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT ph.element_id
        FROM player_histories ph
        LEFT JOIN players p ON ph.element_id = p.id
        WHERE p.id IS NULL
        ORDER BY ph.element_id
        """
    ).fetchall()
    return [r[0] for r in rows]


def check_player_histories_fixtures_exist(conn: sqlite3.Connection) -> list[int]:
    """Return fixture ids in player_histories that have no matching row in fixtures.

    An empty list means the check passes.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT ph.fixture
        FROM player_histories ph
        LEFT JOIN fixtures f ON ph.fixture = f.id
        WHERE f.id IS NULL
        ORDER BY ph.fixture
        """
    ).fetchall()
    return [r[0] for r in rows]


def check_players_teams_exist(conn: sqlite3.Connection) -> list[int]:
    """Return team ids referenced by players that have no matching row in teams.

    An empty list means the check passes.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT p.team
        FROM players p
        LEFT JOIN teams t ON p.team = t.id
        WHERE t.id IS NULL
        ORDER BY p.team
        """
    ).fetchall()
    return [r[0] for r in rows]


def check_player_count_matches(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (distinct element_ids in player_histories, row count in players).

    A mismatch is a warning, not a hard failure — it is normal when
    player_histories has not yet been populated for the current season.
    """
    hist_count: int = conn.execute(
        "SELECT COUNT(DISTINCT element_id) FROM player_histories"
    ).fetchone()[0]
    player_count: int = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    return hist_count, player_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_integrity_checks(conn: sqlite3.Connection) -> None:
    """Run all cross-table integrity checks against an open connection.

    Hard failures raise IntegrityViolation with the violating IDs.
    The player-count mismatch is logged at WARNING and does not raise.

    Args:
        conn: Open sqlite3 connection (read-only use; no writes performed).

    Raises:
        IntegrityViolation: If any referential integrity check finds violations.
    """
    orphan_elements = check_player_histories_elements_exist(conn)
    if orphan_elements:
        sample = orphan_elements[:20]
        raise IntegrityViolation(
            f"player_histories references {len(orphan_elements)} element_id(s) "
            f"not present in players. Sample: {sample}"
        )

    orphan_fixtures = check_player_histories_fixtures_exist(conn)
    if orphan_fixtures:
        sample = orphan_fixtures[:20]
        raise IntegrityViolation(
            f"player_histories references {len(orphan_fixtures)} fixture id(s) "
            f"not present in fixtures. Sample: {sample}"
        )

    orphan_teams = check_players_teams_exist(conn)
    if orphan_teams:
        sample = orphan_teams[:20]
        raise IntegrityViolation(
            f"players references {len(orphan_teams)} team id(s) "
            f"not present in teams. Sample: {sample}"
        )

    hist_count, player_count = check_player_count_matches(conn)
    if hist_count != player_count:
        logger.warning(
            "Player count mismatch: distinct element_ids in player_histories=%d "
            "does not equal players row count=%d",
            hist_count,
            player_count,
        )
