# Provenance: fixtures_bgw

**Source**: live capture of `GET /api/fixtures/` from the real FPL API, on
2026-08-28 — same capture run as `element_summary_settled_capture` and the
same base data as `fixtures_dgw` (see that fixture's PROVENANCE.md for the
capture method and the `stats`-stripping done for readability). Not
extracted from the legacy SQLite DB (flattened, wrong shape).

**Edits**: one real gameweek-2 fixture was removed — `id: 20`
(`team_h: 2`, `team_a: 1`, kickoff `2026-08-31T19:00:00Z`). All other real
gameweek-1 and gameweek-2 fixtures are unchanged.

**Scenario**: a blank-gameweek shape for gameweek 2 — teams 1 and 2 have no
fixture entry in event 2 at all, while every other team keeps its real
single gameweek-2 fixture. Marked `@pytest.mark.bgw` wherever consumed.
