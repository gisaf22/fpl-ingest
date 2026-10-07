"""Payload drift aggregated into the run manifest, raw contract 2.4.0 (#85).

Drives ``fpl_ingest.cli.main`` with only the FPL client faked, reusing #82's
clean payloads and baselines, and reads ``endpoints[...].drift`` from the
finalized manifest.
"""

from __future__ import annotations

import copy
import dataclasses
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.sync_http import FPLClientError
from tests.integration.test_payload_drift_capture import (  # noqa: F401 - baselines is a fixture
    CLEAN,
    ENDPOINTS,
    baselines,
)
from tests.support import raw_contract_schema
from tests.support.cli_fakes import _raw_response
from tests.support.run_helpers import _manifest

PLAYERS = (1, 2)


def _summary(**extra) -> dict:
    payload = copy.deepcopy(CLEAN["element-summary"])
    for row in payload["history"]:
        row.update(extra)
    return payload


def _client(payloads: dict, *, per_player: dict | None = None, failing: tuple[str, ...] = ()):
    per_player = per_player or {}

    def raw(name, url_tail, payload=None):
        if name in failing:
            raise FPLClientError(f"{name} unreachable")
        body = payloads[name] if payload is None else payload
        url = f"https://fantasy.premierleague.com/api/{url_tail}/"
        response = _raw_response(url, {} if isinstance(body, bytes) else body)
        if isinstance(body, bytes):  # a raw, possibly non-JSON, body
            response = dataclasses.replace(response, body=body)
        return response

    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_bootstrap_raw = AsyncMock(side_effect=lambda: raw("bootstrap-static", "bootstrap-static"))
    client.get_fixtures_raw = AsyncMock(side_effect=lambda: raw("fixtures", "fixtures"))
    client.get_event_status_raw = AsyncMock(side_effect=lambda: raw("event-status", "event-status"))
    client.get_gameweek_live_raw = AsyncMock(side_effect=lambda gw: raw("event-live", f"event/{gw}/live"))
    client.get_element_summary_raw = AsyncMock(
        side_effect=lambda pid: raw("element-summary", f"element-summary/{pid}", per_player.get(pid))
    )
    return client


def _run(tmp_path: Path, client, name: str = "raw") -> tuple[int, dict]:
    raw = tmp_path / name
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        try:
            main(["--raw-dir", str(raw)])
        except SystemExit as exc:
            return int(exc.code or 0), _manifest(raw)
    return 0, _manifest(raw)


def _drift(manifest: dict, endpoint: str) -> dict:
    return manifest["endpoints"][endpoint]["drift"]


# ---------------------------------------------------------------------------
# AC1 — per-endpoint drift aggregated in the finalized manifest
# ---------------------------------------------------------------------------


@pytest.mark.covers("#85 AC1")
def test_a_clean_run_records_ok_with_the_number_of_payloads_checked(tmp_path, baselines):
    _, manifest = _run(tmp_path, _client(copy.deepcopy(CLEAN)))

    for endpoint in ENDPOINTS:
        checked = len(PLAYERS) if endpoint == "element-summary" else 1
        assert _drift(manifest, endpoint) == {
            "status": "ok", "checked": checked, "reasons": [], "entries": []
        }, endpoint


@pytest.mark.covers("#85 AC1")
def test_one_drift_shared_by_every_payload_is_one_entry_counting_them(tmp_path, baselines):
    payloads = copy.deepcopy(CLEAN)
    payloads["element-summary"] = _summary(brand_new="x")

    _, manifest = _run(tmp_path, _client(payloads))

    assert _drift(manifest, "element-summary") == {
        "status": "drift",
        "checked": 2,
        "reasons": [],
        "entries": [
            {
                "path": "$.history[].brand_new",
                "kind": "added",
                "baseline_types": [],
                "observed_types": ["string"],
                "payloads": 2,
            }
        ],
    }


@pytest.mark.covers("#85 AC1")
def test_drift_in_one_payload_of_several_counts_one(tmp_path, baselines):
    client = _client(copy.deepcopy(CLEAN), per_player={2: _summary(brand_new="x")})

    _, manifest = _run(tmp_path, client)

    (entry,) = _drift(manifest, "element-summary")["entries"]
    assert entry["payloads"] == 1


@pytest.mark.covers("#85 AC1")
def test_different_observed_types_are_separate_entries(tmp_path, baselines):
    client = _client(
        copy.deepcopy(CLEAN), per_player={1: _summary(metric="1"), 2: _summary(metric=[1])}
    )

    _, manifest = _run(tmp_path, client)

    entries = _drift(manifest, "element-summary")["entries"]
    assert [(e["path"], e["kind"], e["observed_types"], e["payloads"]) for e in entries] == [
        ("$.history[].metric", "type_changed", ["array"], 1),
        ("$.history[].metric", "type_changed", ["string"], 1),
    ]


@pytest.mark.covers("#85 AC1")
def test_an_unavailable_check_records_its_reason(tmp_path, baselines):
    (baselines / "fixtures.json").unlink()

    _, manifest = _run(tmp_path, _client(copy.deepcopy(CLEAN)))

    drift = _drift(manifest, "fixtures")
    assert drift["status"] == "unavailable"
    assert drift["checked"] == 1
    assert len(drift["reasons"]) == 1 and "fixtures.json" in drift["reasons"][0]
    assert drift["entries"] == []


@pytest.mark.covers("#85 AC1")
def test_mixed_ok_and_unavailable_is_unavailable_with_the_entries_that_ran(tmp_path, baselines):
    client = _client(copy.deepcopy(CLEAN), per_player={1: _summary(brand_new="x"), 2: b"<html>"})

    _, manifest = _run(tmp_path, client)

    drift = _drift(manifest, "element-summary")
    assert drift["status"] == "unavailable"
    assert drift["reasons"] == ["payload is not JSON"]
    assert [(e["path"], e["payloads"]) for e in drift["entries"]] == [("$.history[].brand_new", 1)]


@pytest.mark.covers("#85 AC1")
def test_an_endpoint_with_only_fetch_failures_has_no_drift_block(tmp_path, baselines):
    _, manifest = _run(tmp_path, _client(copy.deepcopy(CLEAN), failing=("fixtures",)))

    assert manifest["endpoints"]["fixtures"]["usable"] == 0
    assert "drift" not in manifest["endpoints"]["fixtures"]


@pytest.mark.covers("#85 AC1")
def test_an_endpoint_skipped_by_policy_is_absent(tmp_path, baselines):
    payloads = copy.deepcopy(CLEAN)
    for row in payloads["event-status"]["status"]:
        row.update(points="p", bonus_added=False)  # provisional: event-live is not fetched

    _, manifest = _run(tmp_path, _client(payloads))

    assert "event-live" not in manifest["endpoints"]


@pytest.mark.covers("#85 AC1")
def test_an_in_progress_manifest_has_no_drift_block(tmp_path, baselines):
    writer = LocalRawWriter(tmp_path / "raw", "fpl", baseline_dir=baselines)
    at = datetime(2026, 10, 7, tzinfo=timezone.utc)
    writer.write_object(
        "fixtures", json.dumps(CLEAN["fixtures"]).encode(), request_url="u",
        requested_at=at, received_at=at, http_status=200,
        shape_validation={"ok": True, "checks": [], "failures": []},
    )

    (path,) = (tmp_path / "raw" / "fpl" / "_manifests").rglob("manifest.json")
    manifest = json.loads(path.read_text())
    assert manifest["status"] == "IN_PROGRESS"
    assert "drift" not in manifest["endpoints"]["fixtures"]


# ---------------------------------------------------------------------------
# AC2 — written manifests and sidecars validate against contract 2.4.0
# ---------------------------------------------------------------------------


@pytest.mark.covers("#85 AC2")
@pytest.mark.parametrize("case", ["ok", "drift", "unavailable"])
def test_written_manifests_and_sidecars_validate_against_2_4_0(tmp_path, baselines, case):
    payloads = copy.deepcopy(CLEAN)
    if case == "drift":
        payloads["element-summary"] = _summary(brand_new="x")
    if case == "unavailable":
        (baselines / "fixtures.json").unlink()

    _, manifest = _run(tmp_path, _client(payloads))

    assert manifest["raw_contract_version"] == "2.4.0"
    assert raw_contract_schema.errors("manifest", manifest) == []
    for sidecar_path in (tmp_path / "raw" / "fpl").rglob("metadata.json"):
        sidecar = json.loads(sidecar_path.read_text())
        assert sidecar["raw_contract_version"] == "2.4.0"
        assert raw_contract_schema.errors("sidecar", sidecar) == [], sidecar_path
