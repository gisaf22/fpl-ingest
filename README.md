# fpl-ingest

A lightweight Python library for pulling data from the [Fantasy Premier League API](https://fantasy.premierleague.com/api/bootstrap-static/) into a local SQLite database.

## What it does

- Fetches players, teams, fixtures, live gameweek data, and per-player history from the FPL API
- Validates everything through typed Pydantic models
- Stores it in SQLite with a single `SQLiteStore` class
- Includes a `fpl-ingest` CLI for fully automated ingestion

For the table grain and source-to-table contract, see [docs/data-contract.md](docs/data-contract.md). Persisted tables store API-provided values, with only minimal structural flattening for storage.

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | Layer stack, data flow, and how to extend the pipeline |
| [Data contract](docs/data-contract.md) | Table grain and source-to-table mapping |
| [Guarantees](docs/guarantees.md) | Operational guarantees and error handling |
| [Performance](docs/performance-review.md) | Throughput analysis and rate limiter design |
| [Production readiness](docs/production-readiness.md) | Stability assessment |
| [Governance](docs/governance.md) | Versioning and compatibility policy |

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

**3. (Optional) Set where data should be stored:**

By default, data is written to `~/.fpl/fpl.db` and `~/.fpl/raw`. To use a different location, set these in your shell profile and reload it:

```bash
export FPL_DB_PATH=~/your/custom/path/fpl.db
export FPL_RAW_DIR=~/your/custom/path/raw
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
fpl-ingest [--db PATH] [--raw-dir PATH] [--force] [--rate RATE] [--strict] [--verbose]
```

| Option | Description |
|---|---|
| `--db` | SQLite database path. Overrides `FPL_DB_PATH`, defaults to `~/.fpl/fpl.db` if neither is set. |
| `--raw-dir` | Directory for raw JSON cache. Overrides `FPL_RAW_DIR`, defaults to `~/.fpl/raw` if neither is set. |
| `--force` | Re-fetch finished gameweeks even if already cached. |
| `--rate RATE` | Max API requests per second (default: 10.0). |
| `--strict` | Abort the run if any stage reports skipped rows or fetch errors. |
| `--verbose` | Enable debug logging. |

## What gets re-fetched each run

| Data | Default run | With `--force` |
|---|---|---|
| Players, teams, fixtures, events | Always re-fetched | Always re-fetched |
| Current gameweek | Always re-fetched | Always re-fetched |
| Player history | Fetched on first run; served from cache on re-runs | Re-fetched |
| Finished gameweeks | Skipped if JSON file exists in `FPL_RAW_DIR` | Re-fetched |

Finished gameweeks are skipped on re-runs because the data is stable once FPL has settled bonus points and score corrections, typically within 24-48 hours of the final whistle. Use `--force` if running the pipeline shortly after a gameweek closes or if a late correction is suspected.

Use `--strict` when running the pipeline in a scheduled or automated context. Without it, stages that encounter fetch errors or validation failures exit with code 0 and log warnings. With `--strict`, the first stage that reports any skipped rows or errors raises an error immediately and halts the run, making failures visible to the scheduler.

## Performance

For throughput numbers, rate limiter design, and cache behavior, see [docs/performance-review.md](docs/performance-review.md).
