"""The real config always points the drift check at the committed baselines (#82 AC8).

Run in a subprocess so the suite's autouse fixture, which turns the baseline
directory off for unrelated tests, cannot mask the real default.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
COMMITTED = REPO / "schemas" / "payload-baseline"


def _resolved(builder: str, **env: str) -> str:
    environ = {k: v for k, v in os.environ.items() if k != "FPL_BASELINE_DIR"}
    environ.update(env)
    code = f"from fpl_ingest.config import {builder}; print({builder}().baseline_dir)"
    return subprocess.run(
        [sys.executable, "-c", code], env=environ, capture_output=True, text=True, check=True
    ).stdout.strip()


@pytest.mark.covers("#82 AC8")
@pytest.mark.parametrize("builder", ["resolve_config", "default_config"])
def test_the_real_config_sets_the_committed_baseline_directory(builder):
    assert _resolved(builder) == str(COMMITTED)
    assert COMMITTED.is_dir()


@pytest.mark.covers("#82 AC8")
@pytest.mark.parametrize("builder", ["resolve_config", "default_config"])
def test_fpl_baseline_dir_overrides_the_default(builder, tmp_path):
    assert _resolved(builder, FPL_BASELINE_DIR=str(tmp_path)) == str(tmp_path.resolve())


@pytest.mark.covers("#82 AC8")
def test_the_config_has_no_silent_default_for_baseline_dir():
    from dataclasses import MISSING, fields

    from fpl_ingest.config import IngestConfig

    (field,) = [f for f in fields(IngestConfig) if f.name == "baseline_dir"]
    assert field.default is MISSING, "a config built without baseline_dir must fail, not disable drift"
