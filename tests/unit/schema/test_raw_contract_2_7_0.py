"""Raw contract 2.7.0: the additive ``expected`` field (#115 AC14)."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.raw_keys import RAW_CONTRACT_VERSION
from fpl_ingest.extract.http.response_router import route_response

SCHEMAS = Path(__file__).resolve().parents[3] / "schemas" / "raw-contract" / "2.7.0"
NO_PICKS = {"ok": False, "checks": [], "failures": ["current_is_empty"], "record_count": None, "outcome": "no_picks"}
ORIGIN = {"kind": "local", "workflow": None, "ref": None, "github_run_id": None, "aws_principal": None}
PASSING = {"ok": True, "checks": [], "failures": [], "record_count": 1}


def _validator(name: str) -> Draft202012Validator:
    return Draft202012Validator(json.loads((SCHEMAS / f"{name}.schema.json").read_text()))


def _errors(name: str, document: dict) -> list[str]:
    return [e.message for e in _validator(name).iter_errors(document)]


def _run(tmp_path, *, with_expected: bool):
    at = datetime(2026, 10, 19, 8, 0, 0, tzinfo=timezone.utc)
    raw = RawResponse(url="https://x/api/entry/1/history/", status=200, headers={}, body=b"{}",
                      requested_at=at, received_at=at + timedelta(seconds=1))
    writer = LocalRawWriter(tmp_path / "raw", "fpl", origin=ORIGIN)
    route_response(writer, "entry-history/2026-27/1", raw, PASSING)
    if with_expected:
        route_response(writer, "entry-history/2026-27/2", raw, NO_PICKS, expected=frozenset({"no_picks"}))
    manifest = writer.finalize("SUCCESS").manifest
    sidecars = [json.loads(p.read_text()) for p in (tmp_path / "raw").rglob("metadata.json")]
    return manifest, sidecars


@pytest.mark.covers("#115 AC14")
def test_contract_version_is_one_minor_above_2_6_0():
    assert RAW_CONTRACT_VERSION == "2.7.0"


@pytest.mark.covers("#115 AC14")
def test_a_manifest_and_sidecar_with_expected_validate(tmp_path):
    manifest, sidecars = _run(tmp_path, with_expected=True)

    assert _errors("manifest", manifest) == []
    assert all(_errors("sidecar", s) == [] for s in sidecars)
    assert any(s.get("expected") is True for s in sidecars)


@pytest.mark.covers("#115 AC14")
@pytest.mark.parametrize("bad", [False, "yes", 1])
def test_a_capture_entry_with_expected_false_or_non_boolean_is_rejected(tmp_path, bad):
    manifest, _ = _run(tmp_path, with_expected=True)
    broken = copy.deepcopy(manifest)
    next(c for c in broken["captures"] if c.get("expected"))["expected"] = bad

    assert _errors("manifest", broken)


@pytest.mark.covers("#115 AC14")
@pytest.mark.parametrize("bad", [False, "yes"])
def test_a_sidecar_with_expected_false_or_non_boolean_is_rejected(tmp_path, bad):
    _, sidecars = _run(tmp_path, with_expected=True)
    broken = copy.deepcopy(next(s for s in sidecars if s.get("expected")))
    broken["expected"] = bad

    assert _errors("sidecar", broken)


@pytest.mark.covers("#115 AC14")
def test_a_run_with_no_expected_outcomes_has_no_expected_key_and_validates(tmp_path):
    manifest, sidecars = _run(tmp_path, with_expected=False)

    assert "expected" not in json.dumps(manifest)
    assert all("expected" not in s for s in sidecars)
    assert _errors("manifest", manifest) == []
