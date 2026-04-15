# Governance

This document defines the project boundary and the change policy for `fpl-ingest`.

## Scope

`fpl-ingest` is responsible for:

- fetching data from the FPL API
- validating and normalizing source payloads into local models
- persisting stable ingest tables in SQLite
- exposing a small CLI for repeatable ingestion runs

`fpl-ingest` is not responsible for owning the final analytical model layer. Its responsibility is to persist stable ingest contracts and keep source-grain data available without forcing one downstream aggregation policy.

## Data Boundary

The governing rule for persisted data is:

- persist only values provided by the upstream API
- allow only minimal structural flattening needed for storage
- do not derive analytics metrics
- do not aggregate records across source rows

Convenience helpers may exist in code, but persisted table contracts should remain aligned to API-provided values and source grain.

## Stability Surface

The main public contract of this repository is:

- persisted table names and grain
- documented CLI flags
- documented environment variables
- documented data contract behavior in `docs/data-contract.md`

Changes to those surfaces should be treated as more significant than internal refactors.

## Breaking Changes

Examples of breaking changes include:

- renaming or removing a persisted table
- changing table grain or uniqueness rules
- removing or renaming a documented CLI flag
- removing or renaming a documented environment variable
- changing the meaning of an existing persisted column

Breaking changes should be called out explicitly in docs, reviews, and release notes or change summaries.

## Change Policy

When changing a public contract:

1. Update the implementation and tests together.
2. Update `README.md` if user-facing usage changes.
3. Update `docs/data-contract.md` if table grain, mappings, or contract semantics change.
4. Document downstream impact if external consumers may be affected.

## Separation Of Concerns

The preferred architecture is:

- ingest preserves source fidelity where practical
- ingest stores data at the source grain it receives
- ingest persists API-provided values rather than derived analytics fields
- downstream systems decide how to aggregate and model that data for analytics use cases

Example:

- `player_histories` preserves `element-summary/history[]` at player-per-fixture grain
- downstream gameweek aggregation belongs outside `fpl-ingest`

## Decision Principle

When there is tension between convenience and contract safety, prefer:

- clearer grain definitions
- source-preserving storage
- explicit documentation
- tests that lock in behavior

This keeps `fpl-ingest` reliable as a reusable ingestion boundary rather than a partially implicit analytics layer.
