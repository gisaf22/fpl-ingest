"""Raw contract 2.5.0: invalid responses are stored apart from ``payload.json`` (#54 AC5)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath

import pytest
from jsonschema import Draft202012Validator

from fpl_ingest.extract.http import raw_keys
from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.response_router import route_response

CONTRACT_DIR = Path(__file__).resolve().parents[3] / "schemas" / "raw-contract" / "2.5.0"
INVALID_CLASSES = ("not_found", "shape_invalid", "http_error")
FAILING = {"ok": False, "checks": ["http_status_2xx"], "failures": ["http_status_2xx: got 403"], "record_count": None}
PASSING = {"ok": True, "checks": [], "failures": [], "record_count": 0}


def _response(status: int, body: bytes) -> RawResponse:
    requested_at = datetime(2026, 10, 19, 8, 0, 0, tzinfo=timezone.utc)
    return RawResponse(
        url="https://fantasy.premierleague.com/api/fixtures/",
        status=status,
        headers={"content-type": "application/json"},
        body=body,
        requested_at=requested_at,
        received_at=requested_at + timedelta(seconds=1),
    )


def _errors(name: str, document: dict) -> list[str]:
    schema = json.loads((CONTRACT_DIR / f"{name}.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    return [e.message for e in Draft202012Validator(schema).iter_errors(document)]


@pytest.fixture
def run_with_one_valid_and_one_invalid_capture(tmp_path):
    """A run whose event-status is valid and whose fixtures response is a 403."""
    writer = LocalRawWriter(
        tmp_path / "raw", "fpl",
        origin={"kind": "local", "workflow": None, "ref": None, "github_run_id": None, "aws_principal": None},
    )
    route_response(writer, "event-status", _response(200, b"{}"), PASSING)
    route_response(writer, "fixtures", _response(403, b'{"detail": "forbidden"}'), FAILING)
    manifest = writer.finalize("PARTIAL").manifest
    return tmp_path / "raw", manifest


@pytest.mark.covers("#54 AC5")
def test_the_contract_version_is_a_minor_bump_to_2_5_0():
    assert raw_keys.RAW_CONTRACT_VERSION == "2.5.0"


@pytest.mark.covers("#54 AC5")
def test_the_invalid_response_location_and_its_classes_are_documented():
    text = "\n".join(
        path.read_text() for path in sorted(CONTRACT_DIR.iterdir()) if path.is_file()
    )

    assert "fpl/_invalid/" in text
    for response_class in INVALID_CLASSES:
        assert response_class in text, response_class


@pytest.mark.covers("#54 AC5")
def test_written_sidecars_and_manifest_validate_against_2_5_0(run_with_one_valid_and_one_invalid_capture):
    root, manifest = run_with_one_valid_and_one_invalid_capture

    assert manifest["raw_contract_version"] == "2.5.0"
    assert _errors("manifest", manifest) == []
    sidecars = sorted(root.rglob("metadata.json"))
    assert len(sidecars) == 2
    for sidecar in sidecars:
        assert _errors("sidecar", json.loads(sidecar.read_text())) == [], sidecar


@pytest.mark.covers("#54 AC5")
def test_a_payload_json_glob_cannot_match_an_invalid_response(run_with_one_valid_and_one_invalid_capture):
    root, _ = run_with_one_valid_and_one_invalid_capture

    invalid_keys = [
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and "_invalid" in path.parts
    ]

    assert invalid_keys, "the 403 must have been stored under _invalid"
    for key in invalid_keys:
        assert PurePosixPath(key).name != "payload.json", key
        assert not PurePosixPath(key).match("fpl/fixtures/**/payload.json"), key
