# Provenance: element_summary_new_player

**Source**: live capture of `GET /api/element-summary/5/` from the real FPL
API, on 2026-08-28 — same capture run as `element_summary_settled_capture`
(see that fixture's PROVENANCE.md for the capture method).

**Edits**: none. `payload.json` is byte-identical (modulo pretty-printing) to
the captured response body.

**Scenario**: a player whose element-summary payload is real and well-formed,
used to stand in for "the response a new player would get" in
`test_new_player_absent_from_prior_run_is_always_fetched`. The "new player"
condition in that test comes from the *absence* of a prior
`element-summary/{player_id}` capture directory on disk — this fixture only
supplies the fetch response once the stage decides to fetch; it plays no role
in the skip/fetch decision itself.
