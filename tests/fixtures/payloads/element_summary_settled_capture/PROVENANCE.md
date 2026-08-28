# Provenance: element_summary_settled_capture

**Source**: live capture of `GET /api/element-summary/1/` from the real FPL
API, on 2026-08-28, using `AsyncFPLClient.get_element_summary_raw` +
`LocalRawWriter` directly (the same client/writer classes the `fpl-ingest`
CLI's `run` command uses) rather than the full CLI `run` command — a targeted
script fetched bootstrap-static, fixtures, event-status, and 5 players'
element-summary (8 requests total) instead of all ~600 players `run` would
fetch, to keep this to a small, deliberate set of live GETs.

**Edits**: none. `payload.json` is byte-identical (modulo `json.dump(...,
indent=2)` pretty-printing for reviewability) to the response body captured
under `fpl/element-summary/1/2026-08-28/<run_id>/payload.json`.

**Scenario**: player 1's `element-summary` as of 2026-08-28, when gameweek 1
is the current gameweek and is finished/settled (see `event_status_settled`
fixture, captured in the same run: `bonus_added: true` for every date in
event 1). Represents "a player already captured, current gameweek settled" —
the input state for `test_second_run_after_settlement_refetches_no_players`.
