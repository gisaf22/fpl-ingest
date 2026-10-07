"""Raw contract 2.3.0: the sidecar gains ``drift``, nothing else changes (#82 AC10)."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

CONTRACT = Path(__file__).resolve().parents[3] / "schemas" / "raw-contract"


def _schema(version: str, name: str) -> dict:
    return json.loads((CONTRACT / version / f"{name}.schema.json").read_text())


def _errors(version: str, name: str, document: dict) -> list[str]:
    schema = _schema(version, name)
    Draft202012Validator.check_schema(schema)
    return [e.message for e in Draft202012Validator(schema).iter_errors(document)]


def _without_version(schema: dict) -> str:
    return json.dumps(schema, sort_keys=True).replace("2.3.0", "X").replace("2.2.0", "X")


SIDECAR_2_2_0 = {
    "raw_contract_version": "2.2.0",
    "source": "fpl",
    "endpoint": "fixtures",
    "run_id": "20261006T120000Z-abcdef",
    "extraction_date": "2026-10-06",
    "season": "2026-27",
    "request_url": "https://fantasy.premierleague.com/api/fixtures/",
    "requested_at": "2026-10-06T12:00:00Z",
    "received_at": "2026-10-06T12:00:01Z",
    "http_status": 200,
    "response_headers": {},
    "content_length": 2,
    "content_sha256": "0" * 64,
    "attempt_count": 1,
    "payload_filename": "payload.json",
    "companion_files": [],
    "shape_validation": {"ok": True, "checks": [], "failures": []},
}

DRIFT = {
    "status": "drift",
    "reason": None,
    "entries": [
        {
            "endpoint": "fixtures",
            "path": "$[].new",
            "kind": "added",
            "baseline_types": [],
            "observed_types": ["number"],
            "count": 1,
        }
    ],
}


@pytest.mark.covers("#82 AC10")
@pytest.mark.parametrize("name", ["manifest", "backfill-catalog"])
def test_only_the_version_differs_from_2_2_0_outside_the_sidecar(name):
    assert _without_version(_schema("2.3.0", name)) == _without_version(_schema("2.2.0", name))


@pytest.mark.covers("#82 AC10")
@pytest.mark.parametrize(
    "drift",
    [DRIFT, {"status": "ok", "reason": None, "entries": []}, {"status": "unavailable", "reason": "x", "entries": []}],
    ids=["drift", "ok", "unavailable"],
)
def test_a_2_3_0_sidecar_with_a_drift_block_validates(drift):
    sidecar = {**SIDECAR_2_2_0, "raw_contract_version": "2.3.0", "drift": drift}
    assert _errors("2.3.0", "sidecar", sidecar) == []


@pytest.mark.covers("#82 AC10")
def test_a_2_3_0_sidecar_without_a_drift_block_validates():
    assert _errors("2.3.0", "sidecar", {**SIDECAR_2_2_0, "raw_contract_version": "2.3.0"}) == []


@pytest.mark.covers("#82 AC10")
@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(status="warn"),
        lambda d: d.pop("entries"),
        lambda d: d["entries"][0].update(kind="renamed"),
        lambda d: d["entries"][0].pop("observed_types"),
        lambda d: d.update(extra=1),
    ],
    ids=["bad_status", "no_entries", "bad_kind", "entry_missing_field", "extra_key"],
)
def test_a_malformed_drift_block_is_rejected(mutate):
    drift = copy.deepcopy(DRIFT)
    mutate(drift)
    sidecar = {**SIDECAR_2_2_0, "raw_contract_version": "2.3.0", "drift": drift}
    assert _errors("2.3.0", "sidecar", sidecar) != []


@pytest.mark.covers("#82 AC10")
def test_a_2_2_0_sidecar_still_validates_against_2_2_0_and_drift_is_not_backported():
    assert _errors("2.2.0", "sidecar", SIDECAR_2_2_0) == []
    assert _errors("2.2.0", "sidecar", {**SIDECAR_2_2_0, "drift": DRIFT}) != []
