#!/usr/bin/env python3
"""Example script showing how to connect to the FPL database from another project."""

import sqlite3
from pathlib import Path

def connect_to_fpl_db(db_path: str = "~/.fpl/fpl.db"):
    """Connect to the FPL SQLite database."""
    expanded_path = Path(db_path).expanduser()
    if not expanded_path.exists():
        raise FileNotFoundError(f"Database not found at {expanded_path}")

    conn = sqlite3.connect(str(expanded_path))
    conn.row_factory = sqlite3.Row  # Enable column access by name
    return conn

def get_top_scorers(limit: int = 10):
    """Get top goal scorers from fixture stats."""
    conn = connect_to_fpl_db()

    query = """
    SELECT
        p.web_name as player_name,
        t.name as team_name,
        SUM(fs.value) as goals
    FROM fixture_stats fs
    JOIN players p ON fs.element = p.id
    JOIN teams t ON p.team = t.id
    WHERE fs.identifier = 'goals_scored'
    GROUP BY fs.element
    ORDER BY goals DESC
    LIMIT ?
    """

    cursor = conn.execute(query, (limit,))
    results = cursor.fetchall()

    print(f"Top {limit} Goal Scorers:")
    print("-" * 40)
    for row in results:
        print(f"{row['player_name']} ({row['team_name']}): {row['goals']} goals")

    conn.close()

def get_player_info(player_name: str):
    """Get detailed info for a specific player."""
    conn = connect_to_fpl_db()

    query = """
    SELECT
        p.*,
        t.name as team_name,
        et.singular_name_short as position
    FROM players p
    JOIN teams t ON p.team = t.id
    JOIN element_types et ON p.element_type = et.id
    WHERE LOWER(p.web_name) LIKE LOWER(?)
    """

    cursor = conn.execute(query, (f"%{player_name}%",))
    player = cursor.fetchone()

    if player:
        print(f"Player: {player['web_name']}")
        print(f"Team: {player['team_name']}")
        print(f"Position: {player['position']}")
        print(f"Cost: £{player['now_cost'] / 10:.1f}M")
        print(f"Total Points: {player['total_points']}")
        print(f"Goals: {player['goals_scored']}")
        print(f"Assists: {player['assists']}")
    else:
        print(f"Player '{player_name}' not found")

    conn.close()

def get_upcoming_fixtures():
    """Get upcoming fixtures."""
    conn = connect_to_fpl_db()

    query = """
    SELECT
        f.id,
        ht.name as home_team,
        at.name as away_team,
        f.kickoff_time,
        e.name as gameweek
    FROM fixtures f
    JOIN teams ht ON f.team_h = ht.id
    JOIN teams at ON f.team_a = at.id
    JOIN events e ON f.event = e.id
    WHERE f.finished = 0
    ORDER BY f.kickoff_time
    LIMIT 10
    """

    cursor = conn.execute(query)
    fixtures = cursor.fetchall()

    print("Upcoming Fixtures:")
    print("-" * 50)
    for fixture in fixtures:
        print(f"{fixture['home_team']} vs {fixture['away_team']} - {fixture['kickoff_time']}")

    conn.close()

if __name__ == "__main__":
    # Example usage
    try:
        get_top_scorers(5)
        print("\n" + "="*50 + "\n")

        get_player_info("salah")
        print("\n" + "="*50 + "\n")

        get_upcoming_fixtures()

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Make sure to run 'fpl-ingest' first to create the database.")