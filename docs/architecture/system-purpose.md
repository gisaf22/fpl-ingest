# System Purpose

This document defines the architectural intent of `fpl-ingest`. It is not an onboarding guide, runbook, schema reference, execution walkthrough, or historical decision record.

## System Overview

`fpl-ingest` is a contract-driven ingestion system for current-season Fantasy Premier League API data.

The system exists to make public API data usable as a trusted ingestion boundary before downstream analytics begin. It preserves source evidence, validates incoming payloads, writes source-shaped local tables, and records audit metadata that makes each run explainable.

The system is intentionally narrow. It is production-style in its contracts, failure visibility, and audit surface, but it avoids infrastructure responsibilities that are outside local ingestion.

## Primary Goals

The system prioritizes:

- reliable ingestion from the FPL API into stable local tables;
- deterministic transforms from upstream payloads to persisted rows;
- explicit table grain and schema visibility for downstream consumers;
- replayability from raw artifacts without additional network calls;
- operational traceability through run, freshness, and lineage metadata;
- reproducible persistence through idempotent upserts and checked-in schema artifacts;
- lightweight operability through a single-process CLI and SQLite database.

These are core goals. Future capabilities such as warehouse publishing, versioned history, downstream marts, feature engineering, or external orchestration should build on this foundation rather than blur the ingest boundary.

## Explicit Non-Goals

`fpl-ingest` intentionally does not attempt to be:

- a distributed ingestion platform;
- a data warehouse;
- a feature store;
- a real-time streaming system;
- a generalized orchestration framework;
- a versioned historical warehouse;
- a low-latency serving system;
- a semantic analytics layer;
- a scheduler;
- a replacement for downstream modeling.

The system persists current source-shaped state and audit metadata. It does not promise warehouse-style versioned history, dimensional models, sub-second freshness, multi-tenant serving, or distributed execution.

## Architectural Philosophy

The architecture favors explicit contracts over implicit behavior. Stage results, table grain, schema artifacts, run status, and lineage are represented directly because silent contract drift is the most dangerous failure mode for an ingestion system.

The trust boundary is the ingestion output: downstream consumers should trust the local tables only in combination with the recorded run and freshness metadata. A table existing in SQLite is not, by itself, proof that the latest run was complete or clean.

The system guarantees:

- successful status only after all stages complete with zero errors and zero skipped rows;
- visible accounting for skipped rows, network errors, and failed stages;
- freshness metadata updates only after a clean run;
- stage-level transactional writes;
- idempotent upserts aligned to documented table grain;
- rejection of unknown upstream fields rather than silent acceptance;
- replay through the same validation, transform, and load path used by live ingestion;
- generated schema artifacts checked against the expected public shape;
- durable run status that can be queried without reconstructing health from logs.

Operational simplicity is treated as a design constraint. A small CLI, SQLite, local raw cache, and scheduler-compatible commands are easier to inspect, replay, and recover than a larger framework whose control plane would exceed the needs of the workload.

Lineage is intentionally lightweight. The system records the relationship between executed stages, raw artifacts, and output tables, but it does not introduce a full external lineage platform.

## Why Replay Exists

Replay exists to separate source acquisition from deterministic processing.

Live ingestion owns network fetches and raw cache writes. Replay reads the existing raw cache, runs the same validation and normalization paths, and upserts the resulting rows into SQLite without calling the FPL API.

This provides:

- deterministic recovery after failed or interrupted runs;
- debugging from the exact payloads that produced a prior outcome;
- reproducible local investigation without depending on live API availability;
- validation of contract and transform changes against retained raw artifacts;
- operational trust that raw source evidence can be reprocessed.

Replay is part of the system philosophy: the source payload is preserved first, and persistence can be regenerated from that evidence. It is not a historical warehouse mechanism and does not create versioned history by itself.

## Latest-State Philosophy

The persisted public tables form a latest-state dataset keyed by documented grain.

Re-ingesting the same source row updates the existing record for that grain. The `ingested_at` column reflects when that row was last written, not when the upstream event originally occurred. Audit tables record stage outcomes and run status, but public data tables do not retain every previous version of a row.

Replay and latest-state persistence work together: replay can regenerate the current database state from retained raw artifacts, but replaying a cache still writes into the latest-state dataset. Historical guarantees require an additional versioned-storage layer outside the current scope.

## Operational Assumptions

The design intentionally relies on:

- single-process scheduling for writes;
- SQLite in WAL mode with bounded lock waiting;
- persistent raw cache directories;
- bounded request concurrency and a capped API request rate;
- scheduler-driven execution rather than an embedded scheduler;
- moderate current-season data volumes;
- local filesystem durability for the database and raw artifacts;
- stage-level transactions rather than all-or-nothing full-run transactions;
- downstream consumers checking metadata before trusting freshness.

These are constraints, not omissions. They keep the system inspectable and reproducible while matching the expected workload.

## Future Evolution Boundaries

Architectural expansion is justified when requirements exceed the current assumptions.

Distributed execution, shared multi-user access, strict recovery-point objectives, managed backups, or multiple concurrent writers would justify replacing SQLite or adding a service-backed persistence layer.

Versioned storage would be justified if downstream users need point-in-time reconstruction, slowly changing dimensions, or season-long warehouse history beyond the latest-state dataset and run metadata.

External orchestration tooling would be justified if dependencies, retries, schedules, alerting, and backfills become more complex than a CLI invoked by a scheduler.

Warehouse integration, downstream marts, or feature engineering layers would be justified when consumers need semantic models, aggregations, or serving-oriented datasets. Those layers should consume the contract-driven ingest output rather than move business semantics into ingestion.

The current architecture is intentionally constrained: it should expand only when new requirements change the system boundary.
