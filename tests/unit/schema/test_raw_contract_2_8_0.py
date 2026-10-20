"""Raw contract 2.8.0: the entry-picks manifest fields (#116 AC4, AC5, AC11)."""

from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from jsonschema import Draft202012Validator

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from fpl_ingest.extract.http.raw_keys import RAW_CONTRACT_VERSION
from fpl_ingest.extract.stages.entry_picks import COHORT_RANDOM, COHORT_SELF, ingest_entry_picks
from fpl_ingest.orchestration.run_status import classify_run
from tests.support.fixture_payloads import payload_bytes

SCHEMAS = Path(__file__).resolve().parents[3] / "schemas" / "raw-contract" / "2.8.0"
ORIGIN = {"kind": "local", "workflow": None, "ref": None, "github_run_id": None, "aws_principal": None}
AT = datetime(2026, 10, 20, 7, 0, tzinfo=timezone.utc)
GWS = [{"id": 1, "finished": True, "data_checked": True}, {"id": 2, "finished": True, "data_checked": True}]


def _errors(name: str, document: dict) -> list[str]:
    schema = json.loads((SCHEMAS / f"{name}.schema.json").read_text())
    return [e.message for e in Draft202012Validator(schema).iter_errors(document)]


def _raw(status: int, body: bytes) -> RawResponse:
    return RawResponse(
        url="https://x/api/entry/1/event/1/picks/", status=status, headers={}, body=body,
        requested_at=AT, received_at=AT,
    )


def _run(tmp_path, *, budget: float | None = None):
    """A run with one valid, one 404 (expected) and one 403 picks response, then its manifest and sidecars."""

    async def _fetch(gw: int, entry_id: int) -> RawResponse:
        if entry_id == 12:
            return _raw(404, b'{"detail":"Not found."}')
        if entry_id == 13:
            return _raw(403, b'{"detail":"forbidden"}')
        return _raw(200, payload_bytes("entry_picks_self"))

    writer = LocalRawWriter(tmp_path / "raw", "fpl", origin=ORIGIN)
    client = MagicMock(get_entry_picks_raw=AsyncMock(side_effect=_fetch))
    entries = [(11, COHORT_RANDOM), (12, COHORT_RANDOM), (13, COHORT_RANDOM), (8450051, COHORT_SELF)]
    asyncio.run(ingest_entry_picks(client, writer, season="2026-27", entries=entries, gameweeks=GWS, budget_seconds=budget))
    manifest = writer.finalize(classify_run(writer.endpoint_outcomes)).manifest
    sidecars = [json.loads(p.read_text()) for p in (tmp_path / "raw").rglob("metadata.json")]
    return manifest, sidecars


@pytest.mark.covers("#116 AC11")
def test_contract_version_is_one_minor_above_2_7_0():
    assert RAW_CONTRACT_VERSION == "2.8.0"


@pytest.mark.covers("#116 AC11")
def test_a_picks_manifest_and_its_sidecars_validate(tmp_path):
    manifest, sidecars = _run(tmp_path)

    assert _errors("manifest", manifest) == []
    assert all(_errors("sidecar", s) == [] for s in sidecars)
    block = manifest["endpoints"]["entry-picks"]
    assert block["non_2xx_by_gameweek"] == {"1": 2}, "the 404 and the 403 of gameweek 1; gameweek 2 never starts"
    assert set(block["throughput"]) == {"requests", "total_seconds", "requests_per_second", "gameweeks_completed"}


@pytest.mark.covers("#116 AC4")
def test_a_budget_stopped_manifest_validates_and_names_the_reason(tmp_path):
    manifest, _ = _run(tmp_path, budget=1e-9)

    assert manifest["stop_reason"] == "stopped_at_budget"
    assert _errors("manifest", manifest) == []


@pytest.mark.covers("#116 AC11")
def test_a_run_without_picks_requests_carries_none_of_the_new_keys(tmp_path):
    writer = LocalRawWriter(tmp_path / "raw", "fpl", origin=ORIGIN)
    manifest = writer.finalize("SUCCESS").manifest

    assert not {"stop_reason"} & set(manifest)
    assert _errors("manifest", manifest) == []


@pytest.mark.covers("#116 AC4")
def test_an_unknown_stop_reason_is_rejected(tmp_path):
    manifest, _ = _run(tmp_path, budget=1e-9)
    broken = copy.deepcopy(manifest)
    broken["stop_reason"] = "ran_out_of_patience"

    assert _errors("manifest", broken)


@pytest.mark.covers("#116 AC11")
@pytest.mark.parametrize("bad", [{"0": 1}, {"one": 1}, {"1": -1}, {"1": "2"}])
def test_a_malformed_non_2xx_count_is_rejected(tmp_path, bad):
    manifest, _ = _run(tmp_path)
    broken = copy.deepcopy(manifest)
    broken["endpoints"]["entry-picks"]["non_2xx_by_gameweek"] = bad

    assert _errors("manifest", broken)


@pytest.mark.covers("#116 AC5")
@pytest.mark.parametrize("change", ["drop_total", "extra_key", "bad_gameweeks"])
def test_a_malformed_throughput_record_is_rejected(tmp_path, change):
    manifest, _ = _run(tmp_path)
    broken = copy.deepcopy(manifest)
    throughput = broken["endpoints"]["entry-picks"]["throughput"]
    if change == "drop_total":
        del throughput["total_seconds"]
    elif change == "extra_key":
        throughput["extra"] = 1
    else:
        throughput["gameweeks_completed"] = ["one"]

    assert _errors("manifest", broken)
