# Player-History Stage: Performance Assessment

Date: 2026-05-18

## Purpose

Assess where wall-clock time is spent in the `player_histories` stage, explain
why the current design produces that profile, and decide whether it is
acceptable for this project's operational goals.

---

## Current Behavior

The stage executes in two sequential phases:

1. **Fetch phase** — all player history requests fire concurrently via
   `asyncio.gather`. The event loop dispatches up to 10 simultaneous
   connections; the token-bucket rate limiter holds throughput at ≤ 10 req/s.
   `gather` blocks until every response is received before control returns.

2. **Write phase** — results are iterated sequentially. For each player: raw
   JSON is written to disk, rows are validated, and a batched SQLite upsert is
   executed. No overlap with the fetch phase occurs.

The runner wraps the entire stage in a single `store.transaction()`, so the
SQLite transaction spans both phases.

---

## Observed Timing Breakdown

At 10 req/s with ~700 players and 10-wide concurrency, directional estimates are:

| Phase | Estimate | Notes |
|---|---|---|
| Fetch (network window) | ~70 s | 700 players ÷ 10 req/s; concurrency means this is rate-limited, not latency-bound |
| Write (sequential upserts) | ~5–10 s | ~700 players × ~38 rows each; SQLite is local, no network hop |
| Total stage | ~75–80 s | Dominated by the network window |

Instrumentation added in `extract/stages/element_summary.py` logs these at
runtime:

```
[player_histories] timing: players=N fetch=Xs write=Xs total=Xs
```

The fetch phase dominates. The write phase is a small fraction of total
runtime.

---

## Root Cause

`asyncio.gather` is a full-barrier primitive: it collects all results before
returning. This means:

- All network I/O is overlapped (good — this is intentional).
- No write begins until the last player fetch completes (the barrier cost).
- The write phase runs entirely after the network window closes.

The barrier is deliberate: it preserves a clean all-or-nothing fetch boundary
before any data is committed, which simplifies strict-mode failure semantics and
replay.

---

## Architectural Tradeoff

The alternative — `asyncio.as_completed` batching — would pipeline writes
alongside in-flight fetches, reducing the write window from sequential to
overlapping. Directional savings:

- Write phase is ~5–10 s of ~75–80 s total.
- Realistic savings from pipelining: ~5–8 s (the write window shrinks but does
  not disappear; SQLite cannot be written from concurrent coroutines without
  additional serialisation).
- Net improvement: roughly 6–10%.

This gain is small relative to the complexity introduced:
- Partial commits within a stage complicate replay semantics.
- `as_completed` interleaves exceptions with partial results, making strict-mode
  abort harder to reason about.
- The single `store.transaction()` in the runner would need to be broken up or
  moved inside the stage.

---

## Why Current Design Is Acceptable

- **Scheduled batch cadence.** The pipeline runs on a daily or per-gameweek
  schedule. A ~75–80 s player-history stage is operationally acceptable; there
  is no real-time or sub-minute SLA.
- **Clear failure semantics.** The gather barrier ensures either all fetches
  succeed or none are committed; strict mode works cleanly.
- **Replay correctness.** Because writes only begin after all fetches complete,
  a failed run leaves no partial player-history data in the database. A replay
  from cached JSON is clean and deterministic.
- **Transactional simplicity.** The runner's single-transaction boundary per
  stage is easy to audit, debug, and reason about.
- **Fetch dominates.** Optimising the ~10% write tail without addressing the
  rate-limited network window delivers minimal end-to-end benefit.

---

## Conditions That Would Justify Optimization Later

- Player count grows materially beyond ~700 (e.g. second competition added,
  bulk historical backfill).
- A tighter refresh SLA emerges (e.g. post-match near-real-time ingestion).
- Write phase grows to a significant fraction of total runtime (e.g. history
  rows per player multiply due to schema changes).
- The SQLite backend is replaced with a remote store that has its own latency
  profile, making write overlap more valuable.

---

## Recommendation

Retain the current gather + stage-transaction design. The player-history stage
runs in approximately 75–80 s under normal conditions, dominated by the
rate-limited network window. Clarity, replayability, and transactional
simplicity currently outweigh the ~6–10% wall-time saving from fetch/write
pipelining. Revisit if cadence, scale, or latency requirements change.
