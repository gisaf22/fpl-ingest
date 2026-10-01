"""A small bucket-shaped tree for the capture-index backfill tests (#66).

Laid out exactly as the capture bucket is, under a local root::

    raw/fpl/_manifests/{date}/{run_id}/manifest.json
    raw/fpl/{endpoint}[/{id}]/{date}/{run_id}/payload.json + metadata.json
    history/2025-26/fpl/{endpoint}[/{id}]/{date}/{run_id}/payload.json + metadata.json

Modelled on what #63's step 0 measured in the bucket on 2026-09-30:

- ``RUN_1_0_0``: a 1.0.0 run with a usable bootstrap (season 2026-27), two
  element-summary payloads, one of them recorded shape-invalid by its sidecar.
- ``RUN_2_0_0``: a 2.0.0 run whose fixtures sidecar has no verdict, so it is
  revalidated.
- ``RUN_NO_BOOTSTRAP``: element-summary only, like the out-of-CI run
  ``20260902T163935Z-07eb06`` — its season is null.
- ``RUN_BAD_BOOTSTRAP``: has a bootstrap-static, but its sidecar records it
  shape-invalid, so it is not usable and the run's season is null.
- ``RUN_2_1_0``: already indexed by its own manifest, so out of scope.
- ``HISTORY_RUN``: the ported 2025-26 run. Sidecars carry no verdict, no
  ``received_at`` and no ``http_status``; one element-summary is malformed.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from tests.support.cli_fakes import MINIMAL_BOOTSTRAP, PLAYER_HISTORY_1

RUN_1_0_0 = "20260829T012300Z-aaaaaa"
RUN_2_0_0 = "20260910T191300Z-bbbbbb"
RUN_NO_BOOTSTRAP = "20260902T163935Z-07eb06"
RUN_BAD_BOOTSTRAP = "20260905T191200Z-dddddd"
RUN_2_1_0 = "20260930T191434Z-cccccc"
HISTORY_RUN = "20260526T034626Z-2a6b73"
HISTORY_RECEIVED_AT = "2026-05-26T03:46:26Z"
VALIDATOR_VERSION = "fpl-ingest/1.0.0+abc1234"

IN_SCOPE_RUNS = (RUN_1_0_0, RUN_2_0_0, RUN_NO_BOOTSTRAP, RUN_BAD_BOOTSTRAP, HISTORY_RUN)
NULL_SEASON_RUNS = (RUN_NO_BOOTSTRAP, RUN_BAD_BOOTSTRAP)

# Fields every backfill entry carries: the manifest captures[] fields (#63 D8),
# so C1 can union the two, plus where the shape verdict came from.
CAPTURE_FIELDS = (
    "key", "endpoint", "received_at", "content_sha256", "content_length",
    "http_status", "shape_ok", "usable", "season",
)
ENTRY_FIELDS = (*CAPTURE_FIELDS, "shape_source", "validator_version")

SHAPE_OK = {"ok": True, "checks": ["top_level_is_object"], "failures": [], "record_count": 1}
SHAPE_INVALID = {
    "ok": False,
    "checks": ["required_top_level_keys_present"],
    "failures": ["required_top_level_keys_present: missing history"],
    "record_count": None,
}

BOOTSTRAP_2026 = {
    **MINIMAL_BOOTSTRAP,
    "events": [{"id": 1, "finished": True, "is_current": False, "deadline_time": "2026-08-15T17:30:00Z"}],
}
BOOTSTRAP_2025 = {
    **MINIMAL_BOOTSTRAP,
    "events": [{"id": 1, "finished": True, "is_current": False, "deadline_time": "2025-08-15T17:30:00Z"}],
}
EVENT_LIVE = {"elements": [{"id": 1, "stats": {"minutes": 90}, "explain": []}]}
FIXTURES = [{"id": 1, "team_h": 11, "team_a": 13, "event": 1}]
MALFORMED = {"not": "an element summary"}


def _bytes(payload) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _date(run_id: str) -> str:
    return f"{run_id[0:4]}-{run_id[4:6]}-{run_id[6:8]}"


class BackfillTree:
    """Writes the tree and remembers every payload key it wrote, by run."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.payload_keys: dict[str, list[str]] = {}

    def _put(self, key: str, data: bytes) -> None:
        path = self.root.joinpath(*key.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def manifest(self, run_id: str, version: str, **extra) -> None:
        body = {"raw_contract_version": version, "run_id": run_id, "source": "fpl",
                "extraction_date": _date(run_id), "status": "SUCCESS", **extra}
        self._put(f"raw/fpl/_manifests/{_date(run_id)}/{run_id}/manifest.json", _bytes(body))

    def live(self, run_id: str, endpoint: str, payload, *, shape=SHAPE_OK, version="1.0.0") -> str:
        body = _bytes(payload)
        prefix = f"raw/fpl/{endpoint}/{_date(run_id)}/{run_id}"
        sidecar = {
            "raw_contract_version": version, "source": "fpl", "endpoint": endpoint,
            "run_id": run_id, "extraction_date": _date(run_id),
            "request_url": f"https://fantasy.premierleague.com/api/{endpoint}/",
            "requested_at": f"{_date(run_id)}T01:23:00Z", "received_at": f"{_date(run_id)}T01:23:01Z",
            "http_status": 200, "response_headers": {}, "content_length": len(body),
            "content_sha256": hashlib.sha256(body).hexdigest(), "attempt_count": 1,
            "payload_filename": "payload.json", "companion_files": [],
        }
        if shape is not None:
            sidecar["shape_validation"] = shape
        self._put(f"{prefix}/payload.json", body)
        self._put(f"{prefix}/metadata.json", _bytes(sidecar))
        self.payload_keys.setdefault(run_id, []).append(f"{prefix}/payload.json")
        return f"{prefix}/payload.json"

    def history(self, endpoint: str, payload) -> str:
        body = _bytes(payload)
        prefix = f"history/2025-26/fpl/{endpoint}/{_date(HISTORY_RUN)}/{HISTORY_RUN}"
        sidecar = {
            "raw_contract_version": "1.0.0", "source": "fpl", "endpoint": endpoint,
            "run_id": HISTORY_RUN, "extraction_date": _date(HISTORY_RUN), "season": "2025-26",
            "content_length": len(body), "content_sha256": hashlib.sha256(body).hexdigest(),
            "payload_filename": "payload.json", "synthetic": True,
            "archive_sha256": "0" * 64, "archive_source_uri": "archive/2025-26/raw/x.json",
            "rekeyed_at": "2026-09-20T00:00:00Z", "run_id_timestamp_meaning": "start of the fetching run",
            "synthetic_note": "re-keyed from the archive",
        }
        self._put(f"{prefix}/payload.json", body)
        self._put(f"{prefix}/metadata.json", _bytes(sidecar))
        self.payload_keys.setdefault(HISTORY_RUN, []).append(f"{prefix}/payload.json")
        return f"{prefix}/payload.json"


def build_tree(root: Path, *, shape_failures: bool = True) -> BackfillTree:
    """The fixture tree; ``shape_failures=False`` leaves out every capture that fails its shape check."""
    tree = BackfillTree(root)

    tree.manifest(RUN_1_0_0, "1.0.0")
    tree.live(RUN_1_0_0, "bootstrap-static", BOOTSTRAP_2026)
    tree.live(RUN_1_0_0, "element-summary/1", PLAYER_HISTORY_1)
    if shape_failures:
        tree.live(RUN_1_0_0, "element-summary/2", MALFORMED, shape=SHAPE_INVALID)

    tree.manifest(RUN_2_0_0, "2.0.0")
    tree.live(RUN_2_0_0, "bootstrap-static", BOOTSTRAP_2026, version="2.0.0")
    tree.live(RUN_2_0_0, "fixtures", FIXTURES, shape=None, version="2.0.0")

    tree.manifest(RUN_NO_BOOTSTRAP, "1.0.0")
    tree.live(RUN_NO_BOOTSTRAP, "element-summary/3", PLAYER_HISTORY_1)

    tree.manifest(RUN_BAD_BOOTSTRAP, "1.0.0")
    if shape_failures:
        tree.live(RUN_BAD_BOOTSTRAP, "bootstrap-static", MALFORMED, shape=SHAPE_INVALID)
    tree.live(RUN_BAD_BOOTSTRAP, "element-summary/4", PLAYER_HISTORY_1)

    tree.manifest(RUN_2_1_0, "2.1.0", captures=[])
    tree.live(RUN_2_1_0, "event-status", {"status": [], "leagues": ""}, version="2.1.0")
    # RUN_2_1_0 is out of scope: it is indexed by its own manifest.
    tree.payload_keys.pop(RUN_2_1_0)

    tree.history("bootstrap-static", BOOTSTRAP_2025)
    tree.history("fixtures", FIXTURES)
    tree.history("event-live/01", EVENT_LIVE)
    tree.history("element-summary/10", PLAYER_HISTORY_1)
    if shape_failures:
        tree.history("element-summary/11", MALFORMED)
    return tree


def catalog_key(run_id: str) -> str:
    return f"raw/fpl/_catalog/backfill/{run_id}.json"


def read_catalog(root: Path, run_id: str) -> dict:
    return json.loads(root.joinpath(*catalog_key(run_id).split("/")).read_text(encoding="utf-8"))
