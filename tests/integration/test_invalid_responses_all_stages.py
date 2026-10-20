"""bootstrap-static, element-summary, event-status and event-live keep invalid responses apart (#55).

Drives ``fpl_ingest.cli.main`` with only the FPL client faked, so the real stages, router,
writer and runner run, as in ``test_invalid_response_storage``. A clean run captures every
endpoint and writes both settlement markers, so each case changes one response.
"""

from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.cli import main
from tests.integration.test_payload_drift_capture import CLEAN
from tests.support.cli_fakes import _raw_response
from tests.support.run_helpers import _manifest

API = "https://fantasy.premierleague.com/api/"
BOOTSTRAP, EVENT_STATUS, EVENT_LIVE, ELEMENT_SUMMARY = (
    "bootstrap-static", "event-status", "event-live", "element-summary",
)
GW = 1
ERROR_BODY = json.dumps({"detail": "forbidden"}).encode()
LIST_BODY = b"[]"
NO_HISTORY_BODY = json.dumps({"detail": "no history here"}).encode()

#: (status, body) invalid for every endpoint: a 2xx list, and a 4xx error body.
INVALID = [
    pytest.param(200, LIST_BODY, "shape_invalid", id="2xx_list"),
    pytest.param(403, ERROR_BODY, "http_error", id="4xx_error_body"),
]


def _response(url: str, payload, status: int = 200, body: bytes | None = None):
    response = _raw_response(url, payload)
    if body is not None:
        response = dataclasses.replace(response, body=body)
    return dataclasses.replace(response, status=status)


def _client(**overrides):
    """A client serving CLEAN. ``overrides`` maps an endpoint to (status, body), or for
    element-summary to ``{player_id: (status, body)}``."""

    def answer(endpoint: str, url_tail: str):
        status, body = overrides.get(endpoint, (200, None))
        return _response(f"{API}{url_tail}/", copy.deepcopy(CLEAN[endpoint]), status, body)

    def element_summary(pid: int):
        status, body = overrides.get(ELEMENT_SUMMARY, {}).get(pid, (200, None))
        return _response(f"{API}element-summary/{pid}/", copy.deepcopy(CLEAN[ELEMENT_SUMMARY]), status, body)

    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_bootstrap_raw = AsyncMock(side_effect=lambda: answer(BOOTSTRAP, "bootstrap-static"))
    client.get_fixtures_raw = AsyncMock(side_effect=lambda: answer("fixtures", "fixtures"))
    client.get_event_status_raw = AsyncMock(side_effect=lambda: answer(EVENT_STATUS, "event-status"))
    client.get_gameweek_live_raw = AsyncMock(side_effect=lambda gw: answer(EVENT_LIVE, f"event/{gw}/live"))
    client.get_element_summary_raw = AsyncMock(side_effect=element_summary)
    return client


def _run(tmp_path: Path, client) -> tuple[int, Path, dict]:
    raw = tmp_path / "raw"
    code = 0
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        try:
            main(["--raw-dir", str(raw)])
        except SystemExit as exc:
            code = int(exc.code or 0)
    return code, raw, _manifest(raw)


def _files(raw: Path, prefix: str) -> list[Path]:
    root = raw / prefix
    return sorted(p for p in root.rglob("*") if p.is_file()) if root.exists() else []


def _payloads(raw: Path, endpoint: str) -> list[Path]:
    return [p for p in _files(raw, f"fpl/{endpoint}") if p.name == "payload.json"]


def _stored_invalid(raw: Path, response_class: str, endpoint: str) -> tuple[bytes, dict]:
    files = _files(raw, f"fpl/_invalid/{response_class}/{endpoint}")
    sidecars = [p for p in files if p.name == "metadata.json"]
    bodies = [p for p in files if p.name != "metadata.json"]
    assert len(sidecars) == 1 and len(bodies) == 1, files
    return bodies[0].read_bytes(), json.loads(sidecars[0].read_text())


def _marker(raw: Path, endpoint: str) -> list[Path]:
    return _files(raw, f"fpl/_settlement/{endpoint}/{GW}")


# ---------------------------------------------------------------------------
# AC1, AC4, AC7 - a top-level response kept apart, never as payload.json
# ---------------------------------------------------------------------------


@pytest.mark.covers("#55 AC1")
@pytest.mark.parametrize("status, body, response_class", INVALID)
def test_an_invalid_bootstrap_static_response_is_kept_apart_with_its_status(
    tmp_path, status, body, response_class
):
    _, raw, _ = _run(tmp_path, _client(**{BOOTSTRAP: (status, body)}))

    assert _payloads(raw, BOOTSTRAP) == []
    stored, sidecar = _stored_invalid(raw, response_class, BOOTSTRAP)
    assert stored == body
    assert sidecar["http_status"] == status


@pytest.mark.covers("#55 AC4")
@pytest.mark.parametrize("status, body, response_class", INVALID)
def test_an_invalid_event_status_response_is_kept_apart_with_its_status(
    tmp_path, status, body, response_class
):
    _, raw, _ = _run(tmp_path, _client(**{EVENT_STATUS: (status, body)}))

    assert _payloads(raw, EVENT_STATUS) == []
    stored, sidecar = _stored_invalid(raw, response_class, EVENT_STATUS)
    assert stored == body
    assert sidecar["http_status"] == status


@pytest.mark.covers("#55 AC7")
@pytest.mark.parametrize("status, body, response_class", INVALID)
def test_an_invalid_event_live_response_is_kept_apart_and_earns_no_marker(
    tmp_path, status, body, response_class
):
    _, raw, _ = _run(tmp_path, _client(**{EVENT_LIVE: (status, body)}))

    assert _payloads(raw, EVENT_LIVE) == []
    stored, sidecar = _stored_invalid(raw, response_class, f"{EVENT_LIVE}/{GW:02d}")
    assert stored == body
    assert sidecar["http_status"] == status
    assert _marker(raw, EVENT_LIVE) == []


@pytest.mark.covers("#55 AC7")
def test_a_valid_event_live_response_still_earns_its_marker(tmp_path):
    _, raw, _ = _run(tmp_path, _client())

    assert len(_marker(raw, EVENT_LIVE)) == 1


# ---------------------------------------------------------------------------
# AC2, AC3 - one player's invalid element-summary, with the rest unaffected
# ---------------------------------------------------------------------------

BAD_PLAYER = [
    pytest.param(200, NO_HISTORY_BODY, "shape_invalid", id="2xx_no_history_list"),
    pytest.param(403, ERROR_BODY, "http_error", id="4xx_error_body"),
]


@pytest.mark.covers("#55 AC2")
@pytest.mark.parametrize("status, body, response_class", BAD_PLAYER)
def test_an_invalid_element_summary_response_is_kept_apart_with_its_status(
    tmp_path, status, body, response_class
):
    _, raw, _ = _run(tmp_path, _client(**{ELEMENT_SUMMARY: {1: (status, body)}}))

    assert _payloads(raw, f"{ELEMENT_SUMMARY}/1") == []
    stored, sidecar = _stored_invalid(raw, response_class, f"{ELEMENT_SUMMARY}/1")
    assert stored == body
    assert sidecar["http_status"] == status


@pytest.mark.covers("#55 AC3")
@pytest.mark.parametrize("status, body, response_class", BAD_PLAYER)
def test_the_other_players_are_captured_exactly_as_without_the_bad_one(
    tmp_path, status, body, response_class
):
    _, clean_raw, _ = _run(tmp_path / "clean", _client())
    _, raw, _ = _run(tmp_path / "bad", _client(**{ELEMENT_SUMMARY: {1: (status, body)}}))

    (clean,) = _payloads(clean_raw, f"{ELEMENT_SUMMARY}/2")
    (captured,) = _payloads(raw, f"{ELEMENT_SUMMARY}/2")
    assert captured.read_bytes() == clean.read_bytes()
    assert json.loads((captured.parent / "metadata.json").read_text())["shape_validation"]["ok"] is True
    assert _payloads(raw, f"{ELEMENT_SUMMARY}/1") == []


# ---------------------------------------------------------------------------
# AC5 - a withheld payload never reports a usable endpoint or a SUCCESS run
# ---------------------------------------------------------------------------


@pytest.mark.covers("#55 AC5")
@pytest.mark.parametrize("status, body, response_class", INVALID)
@pytest.mark.parametrize("endpoint", [BOOTSTRAP, EVENT_STATUS, EVENT_LIVE])
def test_an_invalid_response_fails_its_endpoint_and_the_run_is_never_success(
    tmp_path, endpoint, status, body, response_class
):
    code, _, manifest = _run(tmp_path, _client(**{endpoint: (status, body)}))

    outcome = manifest["endpoints"][endpoint]
    assert outcome["outcome"] == "FAILED"
    assert outcome["usable"] == 0
    assert outcome["failures"] and outcome["failures"][0]["reason"]
    assert manifest["status"] in ("PARTIAL", "FAILED")
    assert code != 0


@pytest.mark.covers("#55 AC5")
def test_one_invalid_player_makes_element_summary_partial_and_the_run_partial(tmp_path):
    code, _, manifest = _run(tmp_path, _client(**{ELEMENT_SUMMARY: {1: (403, ERROR_BODY)}}))

    outcome = manifest["endpoints"][ELEMENT_SUMMARY]
    assert (outcome["outcome"], outcome["usable"], outcome["failed"]) == ("PARTIAL", 1, 1)
    assert manifest["status"] == "PARTIAL"
    assert code != 0


@pytest.mark.covers("#55 AC5")
def test_every_player_invalid_fails_element_summary(tmp_path):
    bad = {1: (403, ERROR_BODY), 2: (403, ERROR_BODY)}

    _, _, manifest = _run(tmp_path, _client(**{ELEMENT_SUMMARY: bad}))

    outcome = manifest["endpoints"][ELEMENT_SUMMARY]
    assert (outcome["outcome"], outcome["usable"], outcome["failed"]) == ("FAILED", 0, 2)
    assert manifest["status"] in ("PARTIAL", "FAILED")


# ---------------------------------------------------------------------------
# AC6 - valid captures are unchanged and nothing lands in the invalid location
# ---------------------------------------------------------------------------


@pytest.mark.covers("#55 AC6")
def test_valid_responses_are_written_as_payload_json_and_nothing_is_invalid(tmp_path):
    code, raw, manifest = _run(tmp_path, _client())

    assert code == 0
    assert not (raw / "fpl" / "_invalid").exists()
    for endpoint in (BOOTSTRAP, EVENT_STATUS, f"{EVENT_LIVE}/{GW:02d}", f"{ELEMENT_SUMMARY}/1", f"{ELEMENT_SUMMARY}/2"):
        (payload,) = _payloads(raw, endpoint)
        sidecar = json.loads((payload.parent / "metadata.json").read_text())
        assert sidecar["http_status"] == 200
        assert sidecar["shape_validation"]["ok"] is True
        assert "class" not in sidecar and "reason" not in sidecar
    for endpoint in (BOOTSTRAP, EVENT_STATUS, EVENT_LIVE, ELEMENT_SUMMARY):
        assert manifest["endpoints"][endpoint]["outcome"] == "SUCCESS"
    assert manifest["status"] == "SUCCESS"


# ---------------------------------------------------------------------------
# AC8 (proposed) - a 429 or 5xx that outlived the client's retries stores nothing
# ---------------------------------------------------------------------------


@pytest.mark.covers("#55 AC8")
@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize(
    "overrides, endpoint, family",
    [
        pytest.param({BOOTSTRAP: None}, f"{BOOTSTRAP}", BOOTSTRAP, id="bootstrap-static"),
        pytest.param({EVENT_STATUS: None}, f"{EVENT_STATUS}", EVENT_STATUS, id="event-status"),
        pytest.param({EVENT_LIVE: None}, f"{EVENT_LIVE}/{GW:02d}", EVENT_LIVE, id="event-live"),
        pytest.param({ELEMENT_SUMMARY: {1: None}}, f"{ELEMENT_SUMMARY}/1", ELEMENT_SUMMARY, id="element-summary"),
    ],
)
def test_a_failed_fetch_stores_nothing_and_is_recorded_as_a_failure(
    tmp_path, status, overrides, endpoint, family
):
    if isinstance(overrides[family], dict):
        overrides = {family: {1: (status, b"")}}
    else:
        overrides = {family: (status, b"")}

    code, raw, manifest = _run(tmp_path, _client(**overrides))

    assert _payloads(raw, endpoint) == []
    assert _files(raw, "fpl/_invalid") == []
    failures = manifest["endpoints"][family]["failures"]
    assert any(f["endpoint"] == endpoint for f in failures)
    assert manifest["status"] in ("PARTIAL", "FAILED")
    assert code != 0
    if family == EVENT_LIVE:
        assert _marker(raw, EVENT_LIVE) == []
