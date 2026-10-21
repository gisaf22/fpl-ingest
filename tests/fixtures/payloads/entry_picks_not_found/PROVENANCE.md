# Provenance: entry_picks_not_found

**Source**: live `GET /api/entry/10000000/event/1/picks/` from the real FPL API on
2026-10-20 (curl, no auth, HTTP 404), for #116. An id that did not exist at gameweek 1, the pre-join case a random-cohort manager hits.

**Edits**: none; the body is `{"detail":"Not found."}` as received (re-indented).
