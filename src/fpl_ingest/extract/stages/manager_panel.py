"""Manager panel draw and one-off history capture (#102).

Draws a seeded, reproducible panel of real managers (D1: uniformly random ids
plus a self cohort held outside them) and captures each kept manager's
``entry/{id}/history`` once, verbatim, under endpoint
``entry-history/{season}/{entry_id}``. The panel file lands at
``fpl/_panels/{season}/manager-panel.json`` (D3).

The history response is also the keep test: an id is kept iff ``current`` is
non-empty, i.e. it has picks this season (P1). A 404 is skipped, not a failure.

Resume after a failed run (P3, P6): any non-404 failure leaves no panel file,
so the next run redraws. The seed defaults to a function of the season, so the
redraw is the same ids, and an id that already has a capture under
``fpl/entry-history/{season}/{entry_id}`` is counted as kept without a fetch
(``exists_prefix``, no content read) — sound because only kept ids are ever
stored. The panel file is the one thing read back (D5), via
``RawStorageBackend.read_panel_bytes``, and only by this module.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
from typing import Any

from fpl_ingest.extract.http.client import _ENDPOINTS, AsyncFPLClient, RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter, RawStorageBackend
from fpl_ingest.extract.http.raw_keys import panel_key
from fpl_ingest.orchestration.stage_result import StageLineage, StageMetadata, StageOutcome, StageResult

logger = logging.getLogger(__name__)

RAW_SOURCE = "fpl"
HISTORY_FAMILY = "entry-history"

#: Random ids in the panel (D1 update), before the self cohort.
PANEL_SIZE = 2000
#: The self cohort (D1): kept outside the random draw.
SELF_COHORT = (8450051, 4685107)
#: Every id in the first draw joins at GW1, where backfill starts (P2).
INITIAL_JOINED_GW = 1

COHORT_RANDOM = "random"
COHORT_SELF = "self"

#: The stage writes raw objects and the panel file, no tables.
MANAGER_PANEL_STAGE = StageMetadata(
    name="manager_panel", dependencies=("core",), raw_artifacts=(), output_tables=()
)


def raw_endpoint(season: str, entry_id: int) -> str:
    """Return the raw-contract endpoint identity for one manager's history."""
    return f"{HISTORY_FAMILY}/{season}/{entry_id}"


def default_seed(season: str) -> int:
    """Return the season's default seed, stable across runs and processes (P5).

    Derived from a digest, not ``hash()``, which is salted per process.
    """
    return int.from_bytes(hashlib.sha256(season.encode("utf-8")).digest()[:8], "big")


def draw_ids(seed: int, total_players: int, panel_size: int, self_cohort: tuple[int, ...]) -> list[int]:
    """Return the random panel ids: seeded, sorted, never a self id.

    Over-draws by the number of self ids in range and drops them, so the
    population is never materialised (it is ~11 million ids).
    """
    excluded = {i for i in self_cohort if 1 <= i <= total_players}
    wanted = min(panel_size, total_players - len(excluded))
    sample = random.Random(seed).sample(range(1, total_players + 1), wanted + len(excluded))
    return sorted([i for i in sample if i not in excluded][:wanted])


def read_panel(backend: RawStorageBackend, season: str) -> dict[str, Any] | None:
    """Return the season's panel file, or None if it has not been drawn."""
    data = backend.read_panel_bytes(panel_key(RAW_SOURCE, season))
    return None if data is None else json.loads(data)


async def ingest_manager_panel(
    client: AsyncFPLClient,
    raw_writer: LocalRawWriter,
    *,
    season: str,
    total_players: int,
    seed: int,
    panel_size: int = PANEL_SIZE,
    self_cohort: tuple[int, ...] = SELF_COHORT,
) -> StageOutcome[dict[str, Any]]:
    """Draw the season's panel once and capture each kept manager's history.

    Returns the existing panel untouched, fetching nothing, when one has
    already been drawn (D5). Otherwise fetches every drawn id, writes history
    for the kept ones, and writes the panel file only if nothing failed (P3).
    """
    backend = raw_writer.backend
    existing = read_panel(backend, season)
    if existing is not None:
        logger.info("Manager panel for %s already drawn; nothing to do", season)
        return StageOutcome(result=StageResult(stage=MANAGER_PANEL_STAGE.name), output=existing)

    drawn = draw_ids(seed, total_players, panel_size, self_cohort)
    jobs = [(i, COHORT_RANDOM) for i in drawn] + [(i, COHORT_SELF) for i in self_cohort]
    results = await asyncio.gather(*[_capture_one(client, raw_writer, season, i, c) for i, c in jobs])

    tally = {"fetched": 0, "errors": 0, "written": 0, "not_found": 0, "no_picks": 0}
    entries: list[dict[str, Any]] = []
    for (entry_id, cohort), verdict in zip(jobs, results):
        if verdict == "error":
            tally["errors"] += 1
            continue
        if verdict != "resumed":
            tally["fetched"] += 1
        if verdict in ("not_found", "no_picks"):
            tally[verdict] += 1
            if cohort == COHORT_RANDOM:
                continue
        if verdict == "kept":
            tally["written"] += 1
        entries.append({"entry_id": entry_id, "cohort": cohort, "joined_gw": INITIAL_JOINED_GW})

    result = StageResult(
        stage=MANAGER_PANEL_STAGE.name,
        fetched=tally["fetched"], validated=tally["fetched"], written=tally["written"],
        errors=tally["errors"],
    )
    if tally["errors"]:
        logger.error("Manager panel not written: %d history capture(s) failed; a rerun resumes", tally["errors"])
        return StageOutcome(result=result)

    panel = {
        "season": season,
        "seed": seed,
        "total_players": total_players,
        "drawn": len(drawn),
        "kept": sum(1 for e in entries if e["cohort"] == COHORT_RANDOM),
        "drawn_not_kept": {"not_found": tally["not_found"], "no_picks": tally["no_picks"]},
        "entries": entries,
    }
    backend.put_bytes(panel_key(RAW_SOURCE, season), json.dumps(panel, sort_keys=True, indent=2).encode("utf-8") + b"\n")
    logger.info("Manager panel %s drawn: kept %d of %d", season, panel["kept"], panel["drawn"])
    return StageOutcome(
        result=result, output=panel,
        lineage=StageLineage.from_metadata(MANAGER_PANEL_STAGE, raw_artifacts=(panel_key(RAW_SOURCE, season),)),
    )


async def _capture_one(
    client: AsyncFPLClient, raw_writer: LocalRawWriter, season: str, entry_id: int, cohort: str
) -> str:
    """Capture one id; return resumed, kept, no_picks, not_found or error.

    Failures are recorded in the manifest here, so the caller only counts them.
    """
    endpoint = raw_endpoint(season, entry_id)
    # Existence alone says kept: only kept ids are ever stored (P6).
    if raw_writer.backend.exists_prefix(f"{RAW_SOURCE}/{endpoint}"):
        return "resumed"
    url = _ENDPOINTS["entry_history"].format(entry_id=entry_id)
    try:
        raw = await client.get_entry_history_raw(entry_id)
    except Exception as exc:  # noqa: BLE001 - any fetch failure is recorded, never raised
        logger.error("Failed history fetch %d: %s", entry_id, exc)
        raw_writer.record_failure(endpoint, request_url=url, error_class=type(exc).__name__, message=str(exc))
        return "error"

    if raw.status == 404 and cohort == COHORT_RANDOM:
        return "not_found"
    shape = validate_history_shape(raw)
    if not shape["ok"]:
        # A self id that 404s lands here too: it is configured, so it must exist.
        raw_writer.record_failure(
            endpoint, request_url=url, error_class="UnexpectedResponse",
            http_status=raw.status, attempt_count=raw.attempt_count,
            message="; ".join(shape["failures"]),
        )
        return "error"
    payload = raw.json()
    if cohort == COHORT_RANDOM and isinstance(payload, dict) and not payload["current"]:
        return "no_picks"
    raw_writer.write_object(
        endpoint, raw.body, request_url=raw.url, requested_at=raw.requested_at,
        received_at=raw.received_at, http_status=raw.status,
        response_headers=raw.headers, attempt_count=raw.attempt_count, shape_validation=shape,
    )
    return "kept"


def validate_history_shape(raw: RawResponse) -> dict[str, Any]:
    """Return the raw-boundary structural verdict for an entry-history response.

    Checks only that the status is 2xx and the body is a JSON object carrying
    a ``current`` list; anything finer is warehouse work.
    """
    checks = ["http_status_2xx", "body_parses_as_json", "top_level_is_object", "current_is_list"]
    failures: list[str] = []
    payload = raw.json()
    if not 200 <= raw.status < 300:
        failures.append(f"http_status_2xx: got {raw.status}")
    elif payload is None:
        failures.append("body_parses_as_json: body is not valid JSON")
    elif not isinstance(payload, dict):
        failures.append(f"top_level_is_object: got {type(payload).__name__}")
    elif not isinstance(payload.get("current"), list):
        failures.append("current_is_list: missing or not a list")
    return {"ok": not failures, "checks": checks, "failures": failures, "record_count": None}
