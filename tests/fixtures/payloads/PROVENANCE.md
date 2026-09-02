# Fixture provenance

`element_summary_gw2_provisional` / `element_summary_gw2_ratified`
: Real 2026/27 GW2 captures for element 426, trimmed to the GW2 history row
  plus one `fixtures`/`history_past` entry each. The provisional one is the
  verbatim capture from run `20260901T071823Z` (the last run before GW2
  ratified); the ratified one is the live `element-summary` response for the
  same player after ratification. They differ in exactly the four
  ratification-only fields — `influence`, `creativity`, `threat`,
  `ict_index` — all `"0.0"` before, all populated after, with `bonus`, `bps`
  and `total_points` identical in both. This is the shape of the staleness
  the settlement-transition forced re-fetch exists to prevent.
