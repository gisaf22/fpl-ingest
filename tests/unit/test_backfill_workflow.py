"""The backfill workflow's safety settings (#67 AC2b; #63 D1, D3).

A dispatch with defaults must be a dry run, the job must run in the
``backfill`` environment (the only subject the backfill role trusts, and the
one gated by required review), and a concurrency group must keep two
backfills from running at once.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "backfill_catalog.yml"


def _workflow() -> dict:
    return yaml.safe_load(_WORKFLOW.read_text())


def _job(workflow: dict) -> dict:
    (job,) = workflow["jobs"].values()
    return job


@pytest.mark.covers("#67 AC2b")
def test_dispatch_defaults_to_a_dry_run():
    # PyYAML reads the bare key `on` as the boolean True.
    triggers = _workflow()[True]
    dry_run = triggers["workflow_dispatch"]["inputs"]["dry_run"]

    assert dry_run["type"] == "boolean"
    assert dry_run["default"] is True


@pytest.mark.covers("#67 AC2b")
def test_job_runs_in_the_backfill_environment():
    assert _job(_workflow())["environment"] == "backfill"


@pytest.mark.covers("#67 AC2b")
def test_concurrency_group_prevents_parallel_backfills():
    concurrency = _workflow()["concurrency"]

    assert concurrency["group"]
    assert concurrency["cancel-in-progress"] is False
