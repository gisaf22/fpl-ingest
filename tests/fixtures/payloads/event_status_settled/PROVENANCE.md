# Provenance: event_status_settled

**Source**: live capture of `GET /api/event-status/` from the real FPL API,
on 2026-08-28 — same capture run as `element_summary_settled_capture`.

**Edits**: none. `payload.json` is byte-identical (modulo pretty-printing) to
the captured response body.

**Scenario**: the real event-status window on 2026-08-28, where gameweek 1
(the season's current gameweek) is fully settled — every dated entry for
event 1 carries `bonus_added: true`. Parsing this with
`event_status._parse_finality` yields a non-empty `Finality` map whose entry
for event 1 has `bonus_added: True`. Used as the "settled" finality input for
`test_second_run_after_settlement_refetches_no_players`.
