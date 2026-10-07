"""Payload drift recorded at capture time, end to end (#82).

Drives ``fpl_ingest.cli.main`` with only the FPL client faked, so the real
stages, runner and writer run. The baselines are built from this file's clean
payloads into ``tmp_path``. Each test then captures a payload with one kind of
drift and reads the sidecar the writer produced.
"""

from __future__ import annotations

import copy
import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.schema.payload_baseline import build_baseline, render_baseline
from tests.factories import event_row, fixture_row, player_row, team_row
from tests.support.cli_fakes import _raw_response
from tests.support import raw_contract_schema
from tests.support.run_helpers import _manifest

GW = 1
ICT = {"influence": "20.6", "creativity": "3.2", "threat": "6.0", "ict_index": "3.0"}
EXTRA = {"metric": 1, "nested": {"x": 1}}  # a non-identifying number and a nested object on every record

CLEAN = {
    "bootstrap-static": {
        "events": [event_row(id=GW, finished=True, is_current=True)],
        "elements": [
            {**player_row(id=1, team=11, element_type=3), **EXTRA},
            {**player_row(id=2, team=13, element_type=4), **EXTRA},
        ],
        "teams": [team_row(id=11), team_row(id=13, name="Man City", short_name="MCI")],
        "element_types": [],
        "phases": [],
    },
    "fixtures": [
        {**fixture_row(id=1, event=GW, team_h=11, team_a=13), **EXTRA},
        {**fixture_row(id=2, event=GW, team_h=13, team_a=11), **EXTRA},
    ],
    "event-status": {
        "status": [
            {"event": GW, "points": "r", "bonus_added": True, "date": "2026-08-16", **EXTRA},
            {"event": GW, "points": "r", "bonus_added": True, "date": "2026-08-17", **EXTRA},
        ],
        "leagues": "Updated",
    },
    "event-live": {
        "elements": [
            {"id": pid, "stats": {"minutes": 90, **ICT}, "explain": [], **EXTRA} for pid in (1, 2)
        ]
    },
    "element-summary": {
        "history": [
            {"element": 1, "round": GW, "fixture": 1, "minutes": 90, "total_points": 2, **ICT, **EXTRA},
            {"element": 1, "round": GW, "fixture": 2, "minutes": 45, "total_points": 1, **ICT, **EXTRA},
        ],
        "fixtures": [],
        "history_past": [],
    },
}

#: Where each endpoint's records live, and the baseline path prefix for them.
RECORDS = {
    "bootstrap-static": (lambda p: p["elements"], "$.elements[]"),
    "fixtures": (lambda p: p, "$[]"),
    "event-status": (lambda p: p["status"], "$.status[]"),
    "event-live": (lambda p: p["elements"], "$.elements[]"),
    "element-summary": (lambda p: p["history"], "$.history[]"),
}

#: Each drift kind as (mutation of one record, expected (path suffix, kind, baseline, observed) entries).
KINDS = {
    "new_field": (
        lambda r: r.update(brand_new="x"),
        {(".brand_new", "added", (), ("string",))},
    ),
    "removed_field": (
        lambda r: r.pop("metric"),
        {(".metric", "removed", ("number",), ())},
    ),
    "rename": (
        lambda r: r.update(renamed=r.pop("metric")),
        {(".metric", "removed", ("number",), ()), (".renamed", "added", (), ("number",))},
    ),
    "type_change": (
        lambda r: r.update(metric="1"),
        {(".metric", "type_changed", ("number",), ("string",))},
    ),
    "nested_change": (
        lambda r: r.update(nested={"y": 1}),
        {(".nested.x", "removed", ("number",), ()), (".nested.y", "added", (), ("number",))},
    ),
}

ENDPOINTS = list(CLEAN)


@pytest.fixture
def baselines(tmp_path, monkeypatch) -> Path:
    directory = tmp_path / "baselines"
    directory.mkdir()
    for endpoint, payload in CLEAN.items():
        (directory / f"{endpoint}.json").write_text(render_baseline(build_baseline(endpoint, [payload])))
    monkeypatch.setenv("FPL_BASELINE_DIR", str(directory))
    return directory


def _payloads(**overrides) -> dict:
    payloads = copy.deepcopy(CLEAN)
    payloads.update(overrides)
    return payloads


def _with_drift(endpoint: str, kind: str, *, records: str = "all") -> dict:
    payloads = _payloads()
    get_records, _ = RECORDS[endpoint]
    rows = get_records(payloads[endpoint])
    targets = rows if records == "all" else rows[-1:]
    mutate, _ = KINDS[kind]
    for row in targets:
        mutate(row)
    return payloads


def _client(payloads: dict):
    def raw(name, url_tail):
        return _raw_response(f"https://fantasy.premierleague.com/api/{url_tail}/", payloads[name])

    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_bootstrap_raw = AsyncMock(return_value=raw("bootstrap-static", "bootstrap-static"))
    client.get_fixtures_raw = AsyncMock(return_value=raw("fixtures", "fixtures"))
    client.get_event_status_raw = AsyncMock(return_value=raw("event-status", "event-status"))
    client.get_gameweek_live_raw = AsyncMock(side_effect=lambda gw: raw("event-live", f"event/{gw}/live"))
    client.get_element_summary_raw = AsyncMock(
        side_effect=lambda pid: raw("element-summary", f"element-summary/{pid}")
    )
    return client


def _run(tmp_path: Path, payloads: dict, name: str = "raw") -> tuple[int, Path]:
    raw = tmp_path / name
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=_client(payloads)):
        try:
            main(["--raw-dir", str(raw)])
        except SystemExit as exc:
            return int(exc.code or 0), raw
    return 0, raw


def _sidecars(raw: Path, endpoint: str) -> list[dict]:
    paths = sorted((raw / "fpl" / endpoint).rglob("metadata.json"))
    assert paths, f"no {endpoint} capture under {raw}"
    return [json.loads(p.read_text()) for p in paths]


def _entries(sidecar: dict) -> set:
    return {
        (e["path"], e["kind"], tuple(e["baseline_types"]), tuple(e["observed_types"]))
        for e in sidecar["drift"]["entries"]
    }


def _expected(endpoint: str, kind: str) -> set:
    _, prefix = RECORDS[endpoint]
    return {(prefix + suffix, k, b, o) for suffix, k, b, o in KINDS[kind][1]}


def _outcome(raw: Path, rc: int) -> dict:
    manifest = _manifest(raw)
    return {
        "exit": rc,
        "status": manifest["status"],
        "usable": {name: e["usable"] for name, e in manifest["endpoints"].items()},
        "markers": sorted(
            str(p.relative_to(raw)).split("/")[2] for p in (raw / "fpl" / "_settlement").rglob("marker.json")
        ),
    }


# ---------------------------------------------------------------------------
# AC1 — matching payloads record status ok, no entries
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC1")
def test_a_clean_run_records_no_drift_on_any_endpoint(tmp_path, baselines):
    rc, raw = _run(tmp_path, _payloads())

    assert rc == 0
    for endpoint in ENDPOINTS:
        for sidecar in _sidecars(raw, endpoint):
            assert sidecar["drift"] == {"status": "ok", "reason": None, "entries": []}, endpoint


# ---------------------------------------------------------------------------
# AC2 / AC9 — each drift kind on each endpoint, through the real writer path
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC2")
@pytest.mark.covers("#82 AC9")
@pytest.mark.parametrize("kind", list(KINDS))
@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_each_drift_kind_is_recorded_in_the_sidecar_of_each_endpoint(tmp_path, baselines, endpoint, kind):
    _, raw = _run(tmp_path, _with_drift(endpoint, kind))

    for sidecar in _sidecars(raw, endpoint):
        assert sidecar["drift"]["status"] == "drift"
        assert _entries(sidecar) == _expected(endpoint, kind)
        assert {e["endpoint"] for e in sidecar["drift"]["entries"]} == {endpoint}
    for other in set(ENDPOINTS) - {endpoint}:
        for sidecar in _sidecars(raw, other):
            assert sidecar["drift"]["status"] == "ok", other


# ---------------------------------------------------------------------------
# AC3 — drift only in a non-first record is detected
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC3")
@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_drift_only_in_the_last_record_is_recorded(tmp_path, baselines, endpoint):
    _, raw = _run(tmp_path, _with_drift(endpoint, "type_change", records="last"))

    for sidecar in _sidecars(raw, endpoint):
        _, prefix = RECORDS[endpoint]
        assert (prefix + ".metric", "type_changed", ("number",), ("number", "string")) in _entries(sidecar)


# ---------------------------------------------------------------------------
# AC4 — null where the baseline has a type is not drift
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC4")
@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_null_values_record_no_drift(tmp_path, baselines, endpoint):
    payloads = _payloads()
    for row in RECORDS[endpoint][0](payloads[endpoint]):
        row["metric"] = None
        row["nested"] = None

    _, raw = _run(tmp_path, payloads)

    for sidecar in _sidecars(raw, endpoint):
        assert sidecar["drift"]["status"] == "ok"


# ---------------------------------------------------------------------------
# AC5 — empty payloads and lists: no false drift, no crash
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC5")
def test_empty_lists_record_no_drift(tmp_path, baselines):
    payloads = _payloads(fixtures=[])
    payloads["element-summary"] = {"history": [], "fixtures": [], "history_past": []}

    _, raw = _run(tmp_path, payloads)

    for endpoint in ("fixtures", "element-summary"):
        for sidecar in _sidecars(raw, endpoint):
            assert sidecar["drift"] == {"status": "ok", "reason": None, "entries": []}, endpoint


# ---------------------------------------------------------------------------
# AC6 — drift never changes usable, status, exit code or the markers
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC6")
@pytest.mark.parametrize("kind", list(KINDS))
@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_drift_leaves_the_run_outcome_unchanged(tmp_path, baselines, endpoint, kind):
    clean_rc, clean_raw = _run(tmp_path, _payloads(), "clean")
    drift_rc, drift_raw = _run(tmp_path, _with_drift(endpoint, kind), "drift")

    clean = _outcome(clean_raw, clean_rc)
    assert clean["markers"] == ["element-summary", "event-live"], "clean run must write both markers"
    assert _outcome(drift_raw, drift_rc) == clean


# ---------------------------------------------------------------------------
# AC7 — a missing identifying field is still red
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC7")
def test_a_missing_identifying_field_still_fails_the_endpoint(tmp_path, baselines):
    payloads = _payloads()
    for row in payloads["fixtures"]:
        row.pop("team_h")

    rc, raw = _run(tmp_path, payloads)

    manifest = _manifest(raw)
    assert rc != 0
    assert manifest["status"] != "SUCCESS"
    assert manifest["endpoints"]["fixtures"]["usable"] == 0


# ---------------------------------------------------------------------------
# AC8 — missing/corrupt baseline or internal error: capture unaffected
# ---------------------------------------------------------------------------


def _break_missing(baselines: Path, monkeypatch) -> None:
    (baselines / "fixtures.json").unlink()


def _break_corrupt(baselines: Path, monkeypatch) -> None:
    (baselines / "fixtures.json").write_text("{not json")


def _break_internal(baselines: Path, monkeypatch) -> None:
    def boom(*_args, **_kwargs):
        raise RuntimeError("diff exploded")

    monkeypatch.setattr("fpl_ingest.schema.payload_drift.diff_payload", boom)


@pytest.mark.covers("#82 AC8")
@pytest.mark.parametrize("breakage", [_break_missing, _break_corrupt, _break_internal], ids=["missing", "corrupt", "internal"])
def test_a_broken_check_records_unavailable_and_leaves_the_capture_alone(
    tmp_path, baselines, monkeypatch, caplog, breakage
):
    clean_rc, clean_raw = _run(tmp_path, _payloads(), "clean")
    breakage(baselines, monkeypatch)

    with caplog.at_level(logging.WARNING):
        rc, raw = _run(tmp_path, _payloads(), "broken")

    assert _outcome(raw, rc) == _outcome(clean_raw, clean_rc)
    (sidecar,) = _sidecars(raw, "fixtures")
    assert sidecar["drift"]["status"] == "unavailable"
    assert sidecar["drift"]["reason"]
    assert any(
        r.levelno == logging.WARNING and "drift check unavailable" in r.getMessage() for r in caplog.records
    ), caplog.text


# ---------------------------------------------------------------------------
# AC9 — the sidecar block's fields
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC9")
def test_the_sidecar_block_carries_status_reason_and_full_entries(tmp_path, baselines):
    _, raw = _run(tmp_path, _with_drift("fixtures", "new_field"))

    (sidecar,) = _sidecars(raw, "fixtures")
    assert set(sidecar["drift"]) == {"status", "reason", "entries"}
    (entry,) = sidecar["drift"]["entries"]
    assert entry == {
        "endpoint": "fixtures",
        "path": "$[].brand_new",
        "kind": "added",
        "baseline_types": [],
        "observed_types": ["string"],
        "count": 2,
    }


# ---------------------------------------------------------------------------
# AC10 — written sidecars validate against the current contract (2.3.0's
# sidecar shape, unchanged in 2.4.0 by #85)
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC10")
@pytest.mark.parametrize("case", ["ok", "drift", "unavailable"])
def test_written_sidecars_validate_against_the_current_schema(tmp_path, baselines, case):
    payloads = _with_drift("fixtures", "rename") if case == "drift" else _payloads()
    if case == "unavailable":
        (baselines / "fixtures.json").unlink()

    rc, raw = _run(tmp_path, payloads)

    for endpoint in ENDPOINTS:
        for sidecar in _sidecars(raw, endpoint):
            assert sidecar["raw_contract_version"] == "2.4.0"
            assert raw_contract_schema.errors("sidecar", sidecar) == [], endpoint
    assert raw_contract_schema.errors("manifest", _manifest(raw)) == []


# ---------------------------------------------------------------------------
# AC8 — no configured baseline directory is never a silent skip
# ---------------------------------------------------------------------------


@pytest.mark.covers("#82 AC8")
def test_a_run_with_no_baseline_directory_records_unavailable_on_every_fpl_capture(tmp_path, caplog):
    # No `baselines` fixture: the config resolves no baseline directory.
    with caplog.at_level(logging.WARNING):
        rc, raw = _run(tmp_path, _payloads())

    assert rc == 0
    for endpoint in ENDPOINTS:
        for sidecar in _sidecars(raw, endpoint):
            assert sidecar["drift"]["status"] == "unavailable", endpoint
            assert "no baseline directory" in sidecar["drift"]["reason"]
    assert "drift check unavailable" in caplog.text


@pytest.mark.covers("#82 AC8")
def test_a_config_without_a_baseline_dir_attribute_records_unavailable(tmp_path):
    from types import SimpleNamespace

    config = SimpleNamespace(raw_dir=tmp_path / "raw", storage_backend="local", s3_bucket=None)
    with patch("fpl_ingest.cli.resolve_config", return_value=config):
        rc, raw = _run(tmp_path, _payloads())

    (sidecar,) = _sidecars(raw, "fixtures")
    assert sidecar["drift"]["status"] == "unavailable"
