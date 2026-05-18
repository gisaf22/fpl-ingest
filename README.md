# fpl-ingest

`fpl-ingest` is a contract-driven ingestion system that turns public Fantasy Premier League API data into local SQLite tables with raw source artifacts and run audit metadata.

## Key Capabilities

- Ingests FPL metadata, fixtures, live gameweek player stats, and player history.
- Stores current-season FPL data in structured SQLite tables.
- Preserves raw API responses for inspection.
- Validates payloads before persistence.
- Maintains generated schema contract artifacts.
- Records run and stage audit metadata.
- Uses grain-aligned upserts for repeatable loads.

## Documentation

- [System Purpose](docs/architecture/system-purpose.md) - system identity, guarantees, replay philosophy, trust boundary, and non-goals.
- [Architecture](docs/architecture/architecture.md) - execution flow, orchestration structure, and package ownership.
- [Data Contract](docs/data-contract.md) - table grain, schema definitions, constraints, and field meaning.
- [Navigation Map](docs/navigation-map.md) - reading path, command entry points, and where to go next.
- [ADRs](docs/adr/) - historical architecture decisions.

## License

[MIT](LICENSE)
