# fpl-ingest

A lightweight Python library for pulling data from the [Fantasy Premier League API](https://fantasy.premierleague.com/api/bootstrap-static/) into a local SQLite database.

## What it does

- Fetches players, teams, fixtures, live gameweek data, and per-player history from the FPL API
- Validates everything through typed Pydantic models
- Stores it in SQLite with a single `SQLiteStore` class
- Includes a `fpl-ingest` CLI for fully automated ingestion

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

## Getting started

**1. Clone the repo:**

```bash
git clone https://github.com/gisaf22/fpl-ingest.git
cd fpl-ingest
```

**2. Install dependencies:**

```bash
uv sync
```

**3. Set where data should be stored:**

Open `~/.zshrc` (or `~/.bashrc` on Linux) and add these two lines, replacing the paths with wherever you want the data to live:

```bash
export FPL_DB_PATH=~/data/fpl.db
export FPL_RAW_DIR=~/data/raw
```

Then reload your shell:

```bash
source ~/.zshrc
```

**4. Run the ingestion:**

```bash
uv run fpl-ingest
```

The SQLite database will contain these tables:

| Table | Contents |
|---|---|
| `players` | All players in the current season (name, team, position, price, stats) |
| `teams` | All 20 Premier League teams |
| `fixtures` | Every match in the season with scores and status |
| `fixture_stats` | Per-player stats per fixture (goals, assists, cards, etc.) |
| `gameweeks` | Per-player points and stats for each gameweek |
| `explain_stats` | Points breakdown per player per fixture (how each point was earned) |
| `events` | Gameweek metadata (deadlines, average score, top scorer) |
| `element_types` | Position definitions (GKP, DEF, MID, FWD) |
| `phases` | Season phase definitions |
| `player_history` | Each player's stats aggregated by past season |

Raw JSON responses are also saved to `FPL_RAW_DIR` for inspection or reprocessing.

## CLI reference

```bash
fpl-ingest [--db PATH] [--raw-dir PATH] [--force] [--skip-history] [--verbose]
```

| Option | Description |
|---|---|
| `--db` | SQLite database path. Overrides `FPL_DB_PATH`, defaults to `~/.fpl/fpl.db` if neither is set. |
| `--raw-dir` | Directory for raw JSON cache. Overrides `FPL_RAW_DIR`, defaults to `~/.fpl/raw` if neither is set. |
| `--force` | Re-fetch finished gameweeks even if already cached. |
| `--skip-history` | Skip per-player element-summary history. |
| `--verbose` | Enable debug logging. |

## What gets re-fetched each run

| Data | Default run | With `--force` |
|---|---|---|
| Players, teams, fixtures, events | Always re-fetched | Always re-fetched |
| Current gameweek | Always re-fetched | Always re-fetched |
| Player history (all seasons) | Always re-fetched | Always re-fetched |
| Finished gameweeks | Skipped if JSON file exists in `FPL_RAW_DIR` | Re-fetched |

Finished gameweeks are skipped on re-runs because their data never changes. Use `--force` if you suspect a result was corrected after the fact.
