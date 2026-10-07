"""Raw contract 2.4.0: the manifest gains per-endpoint ``drift``, nothing else changes (#85 AC2)."""

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
    return json.dumps(schema, sort_keys=True).replace("2.4.0", "X").replace("2.3.0", "X")


MANIFEST_2_3_0 = {
    "raw_contract_version": "2.3.0",
    "run_id": "20261007T070000Z-abcdef",
    "source": "fpl",
    "extraction_date": "2026-10-07",
    "started_at": "2026-10-07T07:00:00Z",
    "ended_at": "2026-10-07T07:05:00Z",
    "duration_seconds": 300.0,
    "status": "SUCCESS",
    "objects": {"fixtures": {"written": 1, "bytes": 2}},
    "totals": {"written": 1, "bytes": 2},
    "failures": [],
    "endpoints": {
        "fixtures": {"attempted": 1, "usable": 1, "failed": 0, "outcome": "SUCCESS", "failures": []}
    },
    "git_sha": "abc1234",
    "ingest_version": "0.1.0",
    "config": None,
    "trigger": "scheduled",
    "origin": {"kind": "local", "workflow": None, "ref": None, "github_run_id": None, "aws_principal": None},
    "captures": [],
}

DRIFT = {
    "status": "drift",
    "checked": 2,
    "reasons": [],
    "entries": [
        {
            "path": "$[].new",
            "kind": "added",
            "baseline_types": [],
            "observed_types": ["number"],
            "payloads": 2,
        }
    ],
}


def _manifest_2_4_0(drift=None) -> dict:
    manifest = copy.deepcopy(MANIFEST_2_3_0)
    manifest["raw_contract_version"] = "2.4.0"
    if drift is not None:
        manifest["endpoints"]["fixtures"]["drift"] = drift
    return manifest


@pytest.mark.covers("#85 AC2")
@pytest.mark.parametrize("name", ["sidecar", "backfill-catalog"])
def test_only_the_version_differs_from_2_3_0_outside_the_manifest(name):
    assert _without_version(_schema("2.4.0", name)) == _without_version(_schema("2.3.0", name))


@pytest.mark.covers("#85 AC2")
def test_the_2_3_0_manifest_fixture_is_valid_against_2_3_0():
    assert _errors("2.3.0", "manifest", MANIFEST_2_3_0) == []


@pytest.mark.covers("#85 AC2")
@pytest.mark.parametrize(
    "drift",
    [
        DRIFT,
        {"status": "ok", "checked": 1, "reasons": [], "entries": []},
        {"status": "unavailable", "checked": 1, "reasons": ["payload is not JSON"], "entries": []},
        None,
    ],
    ids=["drift", "ok", "unavailable", "absent"],
)
def test_a_2_4_0_manifest_validates_with_or_without_endpoint_drift(drift):
    assert _errors("2.4.0", "manifest", _manifest_2_4_0(drift)) == []


@pytest.mark.covers("#85 AC2")
@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(status="warn"),
        lambda d: d.pop("checked"),
        lambda d: d.update(checked=-1),
        lambda d: d.pop("reasons"),
        lambda d: d["entries"][0].update(kind="renamed"),
        lambda d: d["entries"][0].pop("payloads"),
        lambda d: d["entries"][0].update(payloads=0),
        lambda d: d.update(extra=1),
    ],
    ids=[
        "bad_status", "no_checked", "negative_checked", "no_reasons",
        "bad_kind", "entry_missing_payloads", "zero_payloads", "extra_key",
    ],
)
def test_a_malformed_endpoint_drift_block_is_rejected(mutate):
    drift = copy.deepcopy(DRIFT)
    mutate(drift)
    assert _errors("2.4.0", "manifest", _manifest_2_4_0(drift)) != []


@pytest.mark.covers("#85 AC2")
def test_earlier_manifests_still_validate_and_the_2_3_0_schema_is_unchanged():
    assert _errors("2.3.0", "manifest", MANIFEST_2_3_0) == []
    # 2.3.0 left endpoints free-form; 2.4.0 adds the drift shape without editing 2.3.0.
    assert _schema("2.3.0", "manifest")["properties"]["endpoints"] == {"type": "object"}
