"""The capture commands leave a drift report only beside a finalized manifest (#83 AC13).

Drives ``fpl_ingest.cli.main`` with only the FPL client faked. ``report-drift``
reads this file in a separate job; its absence must mean "not checked".
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from fpl_ingest.cli import main
from fpl_ingest.extract.http.local_writer import LocalRawWriter
from tests.integration.test_manifest_drift import _client
from tests.integration.test_payload_drift_capture import (  # noqa: F401 - baselines is a fixture
    CLEAN,
    baselines,
)
from tests.support.cli_fakes import _make_async_client
from tests.support.run_helpers import _bootstrap_with_deadline_in, _manifest


def _main(client, *argv: str) -> int:
    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        with pytest.raises(SystemExit) as exc:
            main(list(argv))
    return int(exc.value.code or 0)


@pytest.mark.covers("#83 AC13")
def test_a_finalized_run_writes_the_manifests_drift_to_the_report(tmp_path, baselines):
    raw, report = tmp_path / "raw", tmp_path / "out" / "drift-report.json"

    _main(_client(copy.deepcopy(CLEAN)), "--raw-dir", str(raw), "--drift-report", str(report))

    manifest = _manifest(raw)
    written = json.loads(report.read_text())
    assert written["run_id"] == manifest["run_id"]
    assert written["raw_contract_version"] == manifest["raw_contract_version"]
    assert written["endpoints"] == {
        endpoint: {"drift": outcome["drift"]}
        for endpoint, outcome in manifest["endpoints"].items()
        if "drift" in outcome
    }


@pytest.mark.covers("#83 AC13")
def test_a_run_whose_manifest_is_never_finalized_writes_no_report(tmp_path, baselines):
    raw, report = tmp_path / "raw", tmp_path / "drift-report.json"

    with patch.object(LocalRawWriter, "finalize", side_effect=OSError("bucket unreachable")):
        _main(_client(copy.deepcopy(CLEAN)), "--raw-dir", str(raw), "--drift-report", str(report))

    assert _manifest(raw)["status"] == "IN_PROGRESS"
    assert not report.exists()


@pytest.mark.covers("#83 AC13")
def test_a_pre_deadline_tick_outside_the_window_writes_no_report(tmp_path):
    raw, report = tmp_path / "raw", tmp_path / "drift-report.json"
    client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(180))

    assert _main(client, "--raw-dir", str(raw), "pre-deadline", "--drift-report", str(report)) == 0

    assert not report.exists()
