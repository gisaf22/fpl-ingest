"""The ``pre-deadline`` command's window gate, driven through the CLI.

Kept apart from ``tests/unit/orchestration/test_pre_deadline.py``, which tests
the pure gate with no client and no filesystem. Here the FPL client is faked
and output lands in ``tmp_path``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from fpl_ingest.cli import main
from tests.support.cli_fakes import MINIMAL_BOOTSTRAP, _make_async_client


def _bootstrap_with_deadline_in(minutes: float) -> dict:
    deadline = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    return {
        **MINIMAL_BOOTSTRAP,
        "events": [
            {"id": 6, "finished": False, "deadline_time": deadline.strftime("%Y-%m-%dT%H:%M:%SZ")}
        ],
    }


@pytest.mark.covers("#46 AC4")
@pytest.mark.parametrize("minutes", [180, -5], ids=["deadline-too-far", "deadline-passed"])
def test_out_of_window_run_captures_neither_endpoint(tmp_path, minutes):
    raw: Path = tmp_path / "raw"
    client = _make_async_client(bootstrap=_bootstrap_with_deadline_in(minutes))

    with patch("fpl_ingest.orchestration.runner.AsyncFPLClient", return_value=client):
        with pytest.raises(SystemExit) as exc:
            main(["--raw-dir", str(raw), "pre-deadline"])

    assert int(exc.value.code or 0) == 0
    assert not raw.exists() or not any(p.is_file() for p in raw.rglob("*"))
