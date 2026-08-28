# Provenance: event_status_empty_map

**Source**: derived from `event_status_settled/payload.json` (the real
event-status capture from 2026-08-28 — see that fixture's PROVENANCE.md).

**Edits**: `status` was replaced with an empty list (`[]`). `leagues` is
unchanged. This is not a shape a real event-status response is expected to
send while a season is in progress — it stands in for the case where
event-status genuinely has no per-date entries at all (e.g. a stage-level
data gap, not "every date rolled past the window").

**Scenario**: pins the specific risk in
`element_summary._latest_gameweek_settled`: parsing this payload via
`event_status._parse_finality` yields `Finality == {}` (not `None`). Passed
as `event_finality={}` against a current gameweek with no entry in the map,
`_latest_gameweek_settled` returns `True` (`info is None` is read as "settled
— rolled out of the window"), which is correct for an *old, already-finished*
gameweek but wrong for the *current* one, where an empty map more plausibly
means "unknown," not "settled." Used by
`test_empty_finality_map_does_not_skip_every_player`.
