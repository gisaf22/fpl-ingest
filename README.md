# fpl-ingest

A lightweight Python library for pulling data from the [Fantasy Premier League API](https://fantasy.premierleague.com/api/bootstrap-static/) into a local SQLite database.

## What it does

- **Fetches** players, teams, fixtures, and live gameweek data from the FPL API
- **Validates** everything through typed Pydantic models
- **Stores** it in SQLite with a single `SQLiteStore` class

## Install

```bash
pip install fpl-ingest
```

Or as a git dependency:

```bash
pip install git+https://github.com/gisaf22/fpl-ingest.git@v1.0.0
```

## Quick start

```python
from fpl_ingest import FPLClient, SQLiteStore, PlayerModel

# Pull players from the API
client = FPLClient()
players = client.get_players()

# Store them locally
store = SQLiteStore("fpl.db")
store.register_table("players", PlayerModel)
store.upsert_models("players", PlayerModel, players)
```

## What's inside

| Module | Purpose |
|--------|---------|
| `FPLClient` | HTTP client with rate limiting and retry |
| `SQLiteStore` | Generic SQLite persistence for Pydantic models |
| `PlayerModel` `TeamModel` `FixtureModel` `GameweekModel` | Typed schemas for FPL data |
| `cost_to_millions` `get_season_id` | Utility helpers |
| `flatten_live_elements` | Transform live gameweek data into history rows |

## Requirements

- Python 3.10+
- `requests`
- `pydantic >= 2.0`