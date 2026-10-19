# Provenance: entry_history_not_found

**Source**: live `GET /api/entry/99999999/history/` from the real FPL API on
2026-10-19 (curl, no auth), for #102.

**Edits**: an id beyond `total_players` (11,125,330 at capture); the API answered HTTP 404 with this body; untrimmed. Values are otherwise as received. The history response
carries no manager or team names, so none were removed.
