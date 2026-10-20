"""Manager panel draw and one-off history capture (#102).

Draws a seeded, reproducible panel of real managers (D1: uniformly random ids
plus a self cohort held outside them) and captures each kept manager's
``entry/{id}/history`` once, verbatim, under endpoint
``entry-history/{season}/{entry_id}``. The panel file lands at
``fpl/_panels/{season}/manager-panel.json`` (D3).

The history response is also the keep test: an id is kept iff ``current`` is
non-empty, i.e. it has picks this season (P1). Every response goes through the
shared router (#115): a random-cohort 404 or empty ``current`` is stored under
``fpl/_invalid/`` and declared expected, so it is counted but is not a failure;
anything else that is not a valid capture fails the endpoint, and nothing is
stored at ``payload.json`` for an id that is not kept. The draw is dispatched
through the bounded loop, which stops after a streak of ``http_error`` responses.

Resume after a failed run (P3, P6): any failure that was not declared expected leaves no panel file,
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
import time
from functools import partial
from typing import Any

from fpl_ingest.extract.http.client import _ENDPOINTS, AsyncFPLClient, RawResponse
from fpl_ingest.extract.http.dispatch import dispatch_bounded
from fpl_ingest.extract.http.local_writer import LocalRawWriter, RawStorageBackend
from fpl_ingest.extract.http.rate_config import MAX_CONCURRENT_REQUESTS
from fpl_ingest.extract.http.raw_keys import panel_key
from fpl_ingest.extract.http.response_router import ResponseClass, fetch_and_route, record_no_response
from fpl_ingest.orchestration.stage_result import StageLineage, StageMetadata, StageOutcome, StageResult

logger = logging.getLogger(__name__)

RAW_SOURCE = "fpl"
HISTORY_FAMILY = "entry-history"

#: Random ids in the panel (D1 update), before the self cohort.
PANEL_SIZE = 2000
#: The self cohort (D1): kept outside the random draw.
SELF_COHORT = (8450051, 4685107)
#: Seconds between progress log lines while the draw runs.
PROGRESS_INTERVAL_SECONDS = 30.0
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
    progress_interval: float = PROGRESS_INTERVAL_SECONDS,
) -> StageOutcome[dict[str, Any]]:
    """Draw the season's panel once and capture each kept manager's history.

    Returns the existing panel untouched, fetching nothing, when one has
    already been drawn (D5). Otherwise fetches every drawn id, writes history
    for the kept ones, and writes the panel file only if nothing failed (P3).
    """
    backend = raw_writer.backend
    existing = await asyncio.to_thread(read_panel, backend, season)
    if existing is not None:
        logger.info("Manager panel for %s already drawn; nothing to do", season)
        return StageOutcome(result=StageResult(stage=MANAGER_PANEL_STAGE.name), output=existing)

    drawn = draw_ids(seed, total_players, panel_size, self_cohort)
    jobs = [(i, COHORT_RANDOM) for i in drawn] + [(i, COHORT_SELF) for i in self_cohort]

    io = _OffLoop()
    progress = {"attempted": 0, "stored": 0, "failed": 0}

    async def capture(job: tuple[int, str]) -> str:
        verdict = await _capture_one(client, raw_writer, io, season, *job)
        progress["attempted"] += 1
        if verdict == "kept":
            progress["stored"] += 1
        elif verdict == "error":
            progress["failed"] += 1
        return verdict

    reporter = asyncio.create_task(_report_progress(progress, len(jobs), progress_interval))
    try:
        dispatched = await dispatch_bounded(
            jobs, capture, aborted=lambda: raw_writer.aborted, limit=MAX_CONCURRENT_REQUESTS
        )
    finally:
        reporter.cancel()

    # A job the abort kept from being dispatched is an unresolved id: it blocks the panel file.
    tally = {"fetched": 0, "errors": len(jobs) - len(dispatched), "written": 0, "not_found": 0, "no_picks": 0}
    entries: list[dict[str, Any]] = []
    for (entry_id, cohort), verdict in dispatched:
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
    panel_bytes = json.dumps(panel, sort_keys=True, indent=2).encode("utf-8") + b"\n"
    await io(backend.put_bytes, panel_key(RAW_SOURCE, season), panel_bytes)
    logger.info("Manager panel %s drawn: kept %d of %d", season, panel["kept"], panel["drawn"])
    return StageOutcome(
        result=result, output=panel,
        lineage=StageLineage.from_metadata(MANAGER_PANEL_STAGE, raw_artifacts=(panel_key(RAW_SOURCE, season),)),
    )


class _OffLoop:
    """Runs the writer's and backend's blocking calls on a thread, one at a time.

    boto3 is synchronous: called on the loop it freezes every in-flight
    request, whose 30 s timers keep running (runs 39365465979, 39366583920).
    The writer is not thread-safe, so the lock keeps a single thread inside it.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def __call__(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            return await asyncio.to_thread(fn, *args, **kwargs)


async def _report_progress(progress: dict[str, int], total: int, interval: float) -> None:
    """Log attempted, stored and failed ids every ``interval`` seconds until cancelled."""
    started = time.monotonic()
    while True:
        await asyncio.sleep(interval)
        logger.info(
            "Manager panel progress: attempted %d of %d, stored %d, failed %d (%.0fs elapsed)",
            progress["attempted"], total, progress["stored"], progress["failed"], time.monotonic() - started,
        )


#: Outcomes a random-cohort id may legitimately end in: it may not exist yet, or have no picks.
_EXPECTED_DROPS = frozenset({"not_found", "no_picks"})


async def _capture_one(
    client: AsyncFPLClient, raw_writer: LocalRawWriter, io: _OffLoop, season: str, entry_id: int, cohort: str
) -> str:
    """Capture one id; return resumed, kept, no_picks, not_found or error.

    Failures are recorded in the manifest here, so the caller only counts them.
    """
    endpoint = raw_endpoint(season, entry_id)
    # Existence alone says kept: only kept ids are ever stored (P6).
    if await io(raw_writer.backend.exists_prefix, f"{RAW_SOURCE}/{endpoint}"):
        return "resumed"
    url = _ENDPOINTS["entry_history"].format(entry_id=entry_id)
    random_cohort = cohort == COHORT_RANDOM
    try:
        routed = await fetch_and_route(
            raw_writer, endpoint, partial(client.get_entry_history_raw, entry_id),
            partial(validate_history_shape, require_picks=random_cohort),
            request_url=url, label=f"history {entry_id}",
            # A self id is configured, so it must exist: its 404 is an http_error.
            absence_ok=random_cohort, expected=_EXPECTED_DROPS if random_cohort else frozenset(),
            run_blocking=io,
        )
    except Exception as exc:  # noqa: BLE001 - any fetch failure is recorded, never raised
        await io(record_no_response, raw_writer, endpoint, request_url=url, exc=exc, label=f"history {entry_id}")
        return "error"

    response_class = routed.response_class
    if response_class is ResponseClass.VALID:
        return "kept"
    if response_class is ResponseClass.NOT_FOUND:
        return "not_found"
    if response_class is ResponseClass.SHAPE_INVALID and (routed.shape or {}).get("outcome") == "no_picks":
        return "no_picks"
    return "error"


def validate_history_shape(raw: RawResponse, *, require_picks: bool = False) -> dict[str, Any]:
    """Return the raw-boundary structural verdict for an entry-history response.

    Checks only that the status is 2xx and the body is a JSON object carrying
    a ``current`` list; anything finer is warehouse work. With ``require_picks``
    an empty list also fails, naming the outcome ``no_picks`` so the caller can
    declare it expected (the random cohort); the self cohort keeps an empty
    history.
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
    elif require_picks and not payload["current"]:
        failures.append("current_is_empty: the manager has no picks this season")
        return {"ok": False, "checks": checks, "failures": failures, "record_count": None, "outcome": "no_picks"}
    return {"ok": not failures, "checks": checks, "failures": failures, "record_count": None}
