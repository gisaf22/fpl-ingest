"""Invalid fixtures responses are kept apart from ``payload.json`` (#54).

Drives ``fpl_ingest.cli.main`` with only the FPL client faked, so the real
fixtures stage, router, writer and runner run. Reads back what landed in raw
storage and in the finalized manifest.
"""

from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.cli import main
from tests.integration.test_payload_drift_capture import (  # noqa: F401 - baselines is a fixture
    CLEAN,
    baselines,
)
from tests.support.cli_fakes import _raw_response
from tests.support.run_helpers import _manifest

FIXTURES_URL = "https://fantasy.premierleague.com/api/fixtures/"
NOT_FOUND_BODY = b'{"detail": "Not found."}'
HTML_BODY = b"<html><body>403 Forbidden</body></html>"
OBJECT_BODY = b'{"unexpected": "an object, not a list"}'
FORBIDDEN_JSON_BODY = b'{"detail": "forbidden", "brand_new_field": 1}'
VALID_BODY = json.dumps(CLEAN["fixtures"]).encode()


def _client(*, status: int = 200, body: bytes = VALID_BODY, headers: dict | None = None):
    def raw(name: str, url_tail: str):
        return _raw_response(f"https://fantasy.premierleague.com/api/{url_tail}/", copy.deepcopy(CLEAN[name]))

    fixtures = dataclasses.replace(
        _raw_response(FIXTURES_URL, []),
        status=status,
        body=body,
        headers=headers or {"content-type": "application/json"},
    )
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get_bootstrap_raw = AsyncMock(side_effect=lambda: raw("bootstrap-static", "bootstrap-static"))
    client.get_fixtures_raw = AsyncMock(return_value=fixtures)
    client.get_event_status_raw = AsyncMock(side_effect=lambda: raw("event-status", "event-status"))
    client.get_gameweek_live_raw = AsyncMock(side_effect=lambda gw: raw("event-live", f"event/{gw}/live"))
    client.get_element_summary_raw = AsyncMock(
        side_effect=lambda pid: raw("element-summary", f"element-summary/{pid}")
    )
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
    return sorted(path for path in (raw / prefix).rglob("*") if path.is_file()) if (raw / prefix).exists() else []


def _fixtures_payloads(raw: Path) -> list[Path]:
    return _files(raw, "fpl/fixtures")


def _stored_invalid(raw: Path, response_class: str) -> tuple[bytes, dict]:
    """The one stored body and the sidecar of an invalid fixtures capture."""
    files = _files(raw, f"fpl/_invalid/{response_class}/fixtures")
    sidecars = [path for path in files if path.name == "metadata.json"]
    bodies = [path for path in files if path.name != "metadata.json"]
    assert len(sidecars) == 1 and len(bodies) == 1, files
    return bodies[0].read_bytes(), json.loads(sidecars[0].read_text())


def _other_invalid_classes(raw: Path, expected: str) -> list[str]:
    return [
        cls for cls in ("not_found", "shape_invalid", "http_error")
        if cls != expected and _files(raw, f"fpl/_invalid/{cls}")
    ]


# ---------------------------------------------------------------------------
# AC1-AC3 - an invalid fixtures response is stored apart, never as payload.json
# ---------------------------------------------------------------------------


@pytest.mark.covers("#54 AC1")
def test_a_404_json_error_body_is_kept_as_an_http_error_not_as_the_payload(tmp_path):
    _, raw, _ = _run(tmp_path, _client(status=404, body=NOT_FOUND_BODY))

    assert not any(path.name == "payload.json" for path in _fixtures_payloads(raw))
    body, sidecar = _stored_invalid(raw, "http_error")
    assert body == NOT_FOUND_BODY
    assert sidecar["http_status"] == 404
    assert _other_invalid_classes(raw, "http_error") == []


@pytest.mark.covers("#54 AC2")
def test_an_html_error_page_is_kept_as_an_http_error_not_as_the_payload(tmp_path):
    _, raw, _ = _run(
        tmp_path, _client(status=403, body=HTML_BODY, headers={"content-type": "text/html"})
    )

    assert not any(path.name == "payload.json" for path in _fixtures_payloads(raw))
    body, sidecar = _stored_invalid(raw, "http_error")
    assert body == HTML_BODY
    assert sidecar["http_status"] == 403
    assert _other_invalid_classes(raw, "http_error") == []


@pytest.mark.covers("#54 AC3")
def test_a_200_object_instead_of_a_list_is_kept_as_shape_invalid_with_its_failures(tmp_path):
    _, raw, _ = _run(tmp_path, _client(status=200, body=OBJECT_BODY))

    assert not any(path.name == "payload.json" for path in _fixtures_payloads(raw))
    body, sidecar = _stored_invalid(raw, "shape_invalid")
    assert body == OBJECT_BODY
    assert sidecar["http_status"] == 200
    assert any("top_level_is_list" in failure for failure in sidecar["shape_validation"]["failures"])
    assert _other_invalid_classes(raw, "shape_invalid") == []


# ---------------------------------------------------------------------------
# AC4 - a valid capture is unchanged and nothing lands in the invalid location
# ---------------------------------------------------------------------------


@pytest.mark.covers("#54 AC4")
def test_a_valid_list_is_written_as_payload_json_with_its_usual_sidecar(tmp_path):
    _, raw, manifest = _run(tmp_path, _client())

    (payload,) = [path for path in _fixtures_payloads(raw) if path.name == "payload.json"]
    assert payload.read_bytes() == VALID_BODY
    assert payload.relative_to(raw).parts[:2] == ("fpl", "fixtures")
    sidecar = json.loads((payload.parent / "metadata.json").read_text())
    assert sidecar["http_status"] == 200
    assert sidecar["shape_validation"]["ok"] is True
    assert "class" not in sidecar and "reason" not in sidecar
    assert not (raw / "fpl" / "_invalid").exists()
    assert manifest["endpoints"]["fixtures"]["outcome"] == "SUCCESS"


# ---------------------------------------------------------------------------
# AC6 - a withheld payload never reports a usable endpoint or a SUCCESS run
# ---------------------------------------------------------------------------

INVALID_RESPONSES = [
    pytest.param(404, NOT_FOUND_BODY, id="404_json_error"),
    pytest.param(403, HTML_BODY, id="403_html_page"),
    pytest.param(200, OBJECT_BODY, id="200_object_not_list"),
]


@pytest.mark.covers("#54 AC6")
@pytest.mark.parametrize("status, body", INVALID_RESPONSES)
def test_an_invalid_fixtures_response_fails_the_endpoint_and_makes_the_run_partial(tmp_path, status, body):
    code, raw, manifest = _run(tmp_path, _client(status=status, body=body))

    assert not any(path.name == "payload.json" for path in _fixtures_payloads(raw))
    fixtures = manifest["endpoints"]["fixtures"]
    assert (fixtures["outcome"], fixtures["usable"], fixtures["failed"]) == ("FAILED", 0, 1)
    assert fixtures["failures"] and fixtures["failures"][0]["reason"]
    assert manifest["endpoints"]["bootstrap-static"]["outcome"] == "SUCCESS"
    assert manifest["status"] == "PARTIAL"
    assert code != 0


# ---------------------------------------------------------------------------
# AC9 - the invalid capture's sidecar and captures[] entry; warn-only drift ignores it
# ---------------------------------------------------------------------------


@pytest.mark.covers("#54 AC9")
def test_a_403_json_response_has_a_class_and_reason_sidecar_and_an_unusable_manifest_entry(
    tmp_path, baselines
):
    _, raw, manifest = _run(tmp_path, _client(status=403, body=FORBIDDEN_JSON_BODY))

    _, sidecar = _stored_invalid(raw, "http_error")
    for field in ("received_at", "response_headers", "season", "http_status"):
        assert field in sidecar, field
    assert sidecar["http_status"] == 403
    assert sidecar["season"]
    assert sidecar["class"] == "http_error"
    assert sidecar["reason"]
    assert "drift" not in sidecar

    (entry,) = [c for c in manifest["captures"] if "/_invalid/" in c["key"]]
    assert entry["key"].startswith("raw/fpl/_invalid/http_error/fixtures/")
    assert entry["usable"] is False
    assert entry["class"] == "http_error"

    drift = manifest["endpoints"]["fixtures"].get("drift")
    observed_paths = [e["path"] for e in (drift or {}).get("entries", [])]
    assert not any("brand_new_field" in path or "detail" in path for path in observed_paths)
