"""The capture index and per-capture season, end to end (#62).

Drives ``fpl_ingest.cli.main`` against ``tmp_path`` with the FPL client faked,
so the real stages, runner and writer produce the files under test.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.extract.http.sync_http import FPLClientError
from tests.support import raw_contract_schema
from tests.support.cli_fakes import MINIMAL_BOOTSTRAP, _make_async_client, _raw_response
from tests.support.run_helpers import _history_failing_for, _manifest

# A bootstrap whose lowest-id event has a 2026 deadline: season 2026-27.
BOOTSTRAP_2026 = {
    **MINIMAL_BOOTSTRAP,
    "events": [
        {"id": 2, "finished": False, "is_current": False, "deadline_time": "2026-08-22T17:30:00Z"},
        {"id": 1, "finished": False, "is_current": False, "deadline_time": "2026-08-15T17:30:00Z"},
    ],
}

# Fields a manifest entry and its sidecar must agree on (#62 AC2).
_SHARED_FIELDS = ("received_at", "content_sha256", "shape_ok", "season")


def _run(raw: Path, client) -> int:
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        try:
            main(["--raw-dir", str(raw)])
        except SystemExit as exc:
            return int(exc.code or 0)
    return 0


def _mixed_run(raw: Path) -> dict:
    """A run with a shape-invalid fixtures payload and one player fetch failing."""
    client = _make_async_client(bootstrap=BOOTSTRAP_2026, history_side_effect=_history_failing_for(2))
    client.get_fixtures_raw = AsyncMock(
        return_value=_raw_response("https://fantasy.premierleague.com/api/fixtures/", {"not": "a list"})
    )
    _run(raw, client)
    return _manifest(raw)


def _bootstrap_failing_run(raw: Path) -> dict:
    client = _make_async_client(bootstrap=BOOTSTRAP_2026)
    client.get_bootstrap_raw = AsyncMock(side_effect=FPLClientError("bootstrap unreachable"))
    _run(raw, client)
    return _manifest(raw)


def _payload_keys_on_disk(raw: Path) -> list[str]:
    """Every payload the run wrote, in the bucket-form key the manifest records."""
    return sorted(
        "raw/" + p.relative_to(raw).as_posix()
        for p in raw.rglob("payload.json")
    )


def _sidecar_for(raw: Path, key: str) -> dict:
    assert key.startswith("raw/"), key
    payload = raw / key.removeprefix("raw/")
    return json.loads((payload.parent / "metadata.json").read_text(encoding="utf-8"))


def _shape_ok(sidecar: dict) -> bool:
    verdict = sidecar.get("shape_validation")
    return verdict is None or bool(verdict.get("ok", True))


class TestOneCapturePerPayload:

    @pytest.mark.covers("#62 AC1")
    def test_manifest_has_one_capture_per_payload_written(self, tmp_path):
        raw = tmp_path / "raw"
        manifest = _mixed_run(raw)

        assert manifest["raw_contract_version"] == "2.1.0"
        assert manifest["status"] != "IN_PROGRESS"
        keys = [c["key"] for c in manifest["captures"]]
        assert len(keys) == len(set(keys)), "a payload is indexed twice"
        assert sorted(keys) == _payload_keys_on_disk(raw)
        assert len(keys) == manifest["totals"]["written"]

    @pytest.mark.covers("#62 AC1")
    def test_fetch_failure_has_no_capture_entry(self, tmp_path):
        raw = tmp_path / "raw"
        manifest = _mixed_run(raw)

        assert any(f["endpoint"] == "element-summary/2" for f in manifest["failures"])
        assert not any("/element-summary/2/" in c["key"] for c in manifest["captures"])

    @pytest.mark.covers("#62 AC1")
    def test_shape_invalid_payload_is_indexed_as_unusable(self, tmp_path):
        raw = tmp_path / "raw"
        manifest = _mixed_run(raw)

        (fixtures,) = [c for c in manifest["captures"] if c["endpoint"] == "fixtures"]
        assert fixtures["shape_ok"] is False
        assert fixtures["usable"] is False


class TestCapturesMatchSidecars:

    @pytest.mark.covers("#62 AC2")
    def test_manifest_captures_match_sidecars(self, tmp_path):
        raw = tmp_path / "raw"
        manifest = _mixed_run(raw)

        assert manifest["captures"]
        for entry in manifest["captures"]:
            sidecar = _sidecar_for(raw, entry["key"])
            assert sidecar["endpoint"] == entry["endpoint"]
            assert sidecar["received_at"] == entry["received_at"]
            assert sidecar["content_sha256"] == entry["content_sha256"]
            assert _shape_ok(sidecar) is entry["shape_ok"]
            assert sidecar["season"] == entry["season"]

    @pytest.mark.covers("#62 AC2")
    def test_event_status_sidecar_carries_the_run_season(self, tmp_path):
        """event-status is fetched before bootstrap but written after the season is known (D1)."""
        raw = tmp_path / "raw"
        manifest = _mixed_run(raw)

        (entry,) = [c for c in manifest["captures"] if c["endpoint"] == "event-status"]
        assert entry["season"] == "2026-27"
        assert _sidecar_for(raw, entry["key"])["season"] == "2026-27"

    @pytest.mark.covers("#62 AC2")
    def test_every_capture_in_a_run_has_the_same_season(self, tmp_path):
        raw = tmp_path / "raw"
        manifest = _mixed_run(raw)

        assert {c["season"] for c in manifest["captures"]} == {"2026-27"}


    @pytest.mark.covers("#62 AC2")
    def test_forced_pre_deadline_run_captures_match_sidecars(self, tmp_path):
        raw = tmp_path / "raw"
        client = _make_async_client(bootstrap=BOOTSTRAP_2026)
        with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
            with pytest.raises(SystemExit) as exc:
                main(["--raw-dir", str(raw), "pre-deadline", "--force"])
        assert int(exc.value.code or 0) == 0
        manifest = _manifest(raw)

        assert sorted(c["endpoint"] for c in manifest["captures"]) == ["bootstrap-static", "fixtures"]
        assert sorted(c["key"] for c in manifest["captures"]) == _payload_keys_on_disk(raw)
        for entry in manifest["captures"]:
            sidecar = _sidecar_for(raw, entry["key"])
            for field in _SHARED_FIELDS:
                if field == "shape_ok":
                    assert _shape_ok(sidecar) is entry["shape_ok"]
                else:
                    assert sidecar[field] == entry[field], field
            assert entry["season"] == "2026-27"


class TestEventStatusFetchedFirst:

    @pytest.mark.covers("#62 AC7")
    def test_event_status_is_fetched_before_bootstrap(self, tmp_path):
        """D1 moves event-status's write after bootstrap's fetch, never its fetch."""
        raw = tmp_path / "raw"
        calls: list[str] = []
        base = datetime(2026, 9, 30, 7, 0, tzinfo=timezone.utc)

        def _stamped(name: str, url: str, payload):
            def fetch(*_args):
                calls.append(name)
                response = _raw_response(url, payload)
                at = base + timedelta(seconds=len(calls))
                return dataclasses.replace(response, requested_at=at, received_at=at)
            return fetch

        client = _make_async_client(bootstrap=BOOTSTRAP_2026)
        client.get_event_status_raw = AsyncMock(side_effect=_stamped(
            "event-status", "https://fantasy.premierleague.com/api/event-status/",
            {"status": [], "leagues": ""},
        ))
        client.get_bootstrap_raw = AsyncMock(side_effect=_stamped(
            "bootstrap-static", "https://fantasy.premierleague.com/api/bootstrap-static/", BOOTSTRAP_2026,
        ))
        _run(raw, client)
        manifest = _manifest(raw)

        assert calls[:2] == ["event-status", "bootstrap-static"]
        received = {c["endpoint"]: c["received_at"] for c in manifest["captures"]}
        assert received["event-status"] < received["bootstrap-static"]


class TestSeasonWithoutUsableBootstrap:

    @pytest.mark.covers("#62 AC3")
    def test_capture_is_written_with_null_season_when_bootstrap_fails(self, tmp_path, caplog):
        caplog.set_level(logging.INFO)
        raw = tmp_path / "raw"
        manifest = _bootstrap_failing_run(raw)

        (entry,) = [c for c in manifest["captures"] if c["endpoint"] == "event-status"]
        assert entry["season"] is None
        assert _sidecar_for(raw, entry["key"])["season"] is None
        none_logs = [r for r in caplog.records if "season_source=none" in r.getMessage()]
        assert none_logs and all(r.levelno == logging.ERROR for r in none_logs)


class TestWrittenFilesValidateAgainstSchema:

    @pytest.mark.covers("#62 AC4")
    @pytest.mark.parametrize("scenario", [_mixed_run, _bootstrap_failing_run], ids=["mixed", "bootstrap-fails"])
    def test_written_files_validate_against_schema(self, tmp_path, scenario):
        raw = tmp_path / "raw"
        manifest = scenario(raw)

        assert raw_contract_schema.errors("manifest", manifest) == []
        sidecars = sorted(raw.rglob("metadata.json"))
        assert sidecars
        for path in sidecars:
            sidecar = json.loads(path.read_text(encoding="utf-8"))
            assert raw_contract_schema.errors("sidecar", sidecar) == [], path

    @pytest.mark.covers("#62 AC4")
    @pytest.mark.parametrize(
        ("name", "mutate"),
        [
            pytest.param("manifest", lambda d: d.update(unexpected=1), id="manifest-unknown-field"),
            pytest.param("manifest", lambda d: d["captures"][0].update(unexpected=1), id="capture-unknown-field"),
            pytest.param("manifest", lambda d: d["captures"][0].pop("season"), id="capture-missing-season"),
            pytest.param("sidecar", lambda d: d.update(unexpected=1), id="sidecar-unknown-field"),
            pytest.param("sidecar", lambda d: d.pop("season"), id="sidecar-missing-season"),
        ],
    )
    def test_schema_rejects_unknown_fields_and_missing_season(self, tmp_path, name, mutate):
        raw = tmp_path / "raw"
        manifest = _mixed_run(raw)
        document = (
            manifest
            if name == "manifest"
            else _sidecar_for(raw, manifest["captures"][0]["key"])
        )
        mutated = copy.deepcopy(document)
        mutate(mutated)

        assert raw_contract_schema.errors(name, document) == []
        assert raw_contract_schema.errors(name, mutated) != []
