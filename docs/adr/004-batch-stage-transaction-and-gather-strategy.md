# ADR 004 — Batch Stage Transaction and Gather Strategy

Date: 2026-05-18
Status: Accepted

## Context

The `player_histories` stage must fetch one API response per player
(~600–700 calls per run), transform rows, and upsert them into SQLite. Two
design decisions shape its runtime profile and failure semantics:

**Fetch aggregation with `asyncio.gather`**
All player fetch coroutines are submitted in one `gather` call. The event loop
dispatches them concurrently (bounded by the token-bucket rate limiter at
10 req/s and a semaphore at 10 simultaneous connections). `gather` acts as a
full barrier: control does not return until every response is received or an
exception propagates. No write begins until the fetch window closes.

**Single stage-level SQLite transaction**
The runner wraps each stage call in one `store.transaction()`. For the
player-history stage this means the entire fetch + write sequence either
commits or rolls back as a unit. No partial player-history rows appear in the
database from a failed run.

Together these decisions produce a two-phase sequential model:
`network_window` → `write_window`, rather than overlapping them.

## Decision

Retain `asyncio.gather` for fetch aggregation and the single stage-level
transaction boundary. Do not pipeline writes alongside in-flight fetches.

## Consequences

**Positive**
- Simple orchestration: the fetch/write boundary is explicit and auditable.
- Deterministic stage boundaries: each stage either fully commits or does not
  commit at all.
- Clean replay semantics: a failed run leaves no partial data; replaying from
  cached JSON produces the same result every time.
- Simple failure handling: strict mode aborts cleanly at the gather barrier
  before any writes occur.
- Strong transactional guarantee: the audit table, lineage records, and
  player-history rows are always consistent after a run.

**Negative**
- Fetch and write windows cannot overlap: total stage runtime is
  `fetch_duration + write_duration` rather than `max(fetch, write)`.
- The player-history stage is the longest-running stage (~75–80 s at 10 req/s
  for ~700 players); it is not easily parallelised within a single run.
- Throughput ceiling: adding more players increases the network window linearly;
  there is no mechanical way to reduce it below `player_count / rate`.

## Triggers for Reconsideration

- Player count grows materially (e.g. multi-competition support, bulk backfill).
- A sub-minute or near-real-time refresh SLA is introduced.
- The write phase grows to a significant fraction of total runtime.
- The storage backend introduces meaningful write latency (e.g. remote database).

If any of these conditions arise, `asyncio.as_completed` batching with a
write queue could pipeline the two phases. This would require moving the
transaction boundary inside the stage and reworking strict-mode abort semantics.
See `docs/architecture/performance-assessment.md` for the quantitative tradeoff.
