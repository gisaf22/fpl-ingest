# Provenance: fixtures_dgw

**Source**: live capture of `GET /api/fixtures/` from the real FPL API, on
2026-08-28 — same capture run as `element_summary_settled_capture`. The
capture held all 380 fixtures for the season; this payload keeps only
gameweek 1 (10 real, finished fixtures — kept for context) and gameweek 2
(10 real, unplayed fixtures), with each fixture's bulky `stats` array
(per-player BPS/bonus/defensive-contribution breakdowns, irrelevant to
DGW/BGW shape) zeroed to `[]` for readability. Not extracted from the legacy
SQLite DB, which stores this data flattened and in the wrong shape for a
raw-capture fixture.

**Edits**: one synthetic fixture was appended to the real gameweek-2 set —
`id: 9001`, `code: 9990001`, `event: 2`, `team_h: 8`, `team_a: 20`,
unplayed (`finished: false`, `started: false`, scores `null`), kickoff
`2026-08-30T19:00:00Z`. All other fields were copied from team 8's real
single gameweek-2 fixture (`id: 11`) and adjusted for the new opponent/id.
This gives both team 8 and team 20 two fixtures in event 2 — a double
gameweek — while every other team keeps its single real gameweek-2 fixture
unchanged.

**Scenario**: a double-gameweek shape for gameweek 2, for tests that need to
assert DGW handling downstream of the fixtures endpoint. Marked
`@pytest.mark.dgw` wherever consumed.
