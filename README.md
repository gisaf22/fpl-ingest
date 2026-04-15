# fpl-ingest

A lightweight Python library for pulling data from the [Fantasy Premier League API](https://fantasy.premierleague.com/api/bootstrap-static/) into a local SQLite database.

## What it does

- Fetches players, teams, fixtures, live gameweek data, and per-player history from the FPL API
- Validates everything through typed Pydantic models
- Stores it in SQLite with a single `SQLiteStore` class
- Includes a `fpl-ingest` CLI for fully automated ingestion

For the table grain and source-to-table contract, see [docs/data-contract.md](docs/data-contract.md). Persisted tables store API-provided values, with only minimal structural flattening for storage.

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

Set the paths in your shell profile:

```bash
export FPL_DB_PATH=~/data/fpl.db
export FPL_RAW_DIR=~/data/raw
export FPL_HISTORY_WORKERS=20
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
| `gameweeks` | Live endpoint rows at player-per-gameweek grain |
| `player_histories` | `element-summary/history` rows at player-per-fixture grain |
| `events` | Gameweek metadata (deadlines, average score, top scorer) |
| `element_types` | Position definitions (GKP, DEF, MID, FWD) |

Raw JSON responses are also saved to `FPL_RAW_DIR` for inspection or reprocessing. `gameweeks` stores live player-per-gameweek rows, while `player_histories` preserves player-per-fixture history rows without collapsing multiple fixtures from the same round.

## CLI reference

```bash
fpl-ingest [--db PATH] [--raw-dir PATH] [--force] [--history-workers N] [--verbose]
```

| Option | Description |
|---|---|
| `--db` | SQLite database path. Overrides `FPL_DB_PATH`, defaults to `~/.fpl/fpl.db` if neither is set. |
| `--raw-dir` | Directory for raw JSON cache. Overrides `FPL_RAW_DIR`, defaults to `~/.fpl/raw` if neither is set. |
| `--force` | Re-fetch finished gameweeks even if already cached. |
| `--history-workers` | Worker count for concurrent player history fetches. Overrides `FPL_HISTORY_WORKERS`. |
| `--verbose` | Enable debug logging. |

## What gets re-fetched each run

| Data | Default run | With `--force` |
|---|---|---|
| Players, teams, fixtures, events | Always re-fetched | Always re-fetched |
| Current gameweek | Always re-fetched | Always re-fetched |
| Player history (current season) | Always re-fetched | Always re-fetched |
| Finished gameweeks | Skipped if JSON file exists in `FPL_RAW_DIR` | Re-fetched |

Finished gameweeks are skipped on re-runs because their data never changes. Use `--force` if you suspect a result was corrected after the fact.

## Performance

| Scenario | Approx. time |
|---|---|
| First run (32 GWs + 826 players, no cache) | ~2 minutes |
| Re-run (GWs and player histories cached) | ~5 seconds |

The pipeline uses `aiohttp` with a token bucket rate limiter (default: 10 req/s, 10
concurrent). All gameweek and player history fetches run concurrently under that cap,
giving roughly 826 / 10 = ~83 seconds for player histories.

On re-runs, finished gameweeks and player histories are served from the local JSON
cache in `FPL_RAW_DIR`. Only the current gameweek is re-fetched from the API.

The `--rate` flag adjusts the request rate if needed:

```bash
fpl-ingest --rate 4   # more conservative: ~4 req/s
fpl-ingest --rate 10  # default
```

See [docs/performance-review.md](docs/performance-review.md) for the full analysis.
