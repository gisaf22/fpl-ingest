"""Only the ``report-drift`` job may write issues, and it can never fail the run.

The capture job holds AWS credentials and runs FPL fetches, so it must never
hold an issue-writing token (#80 D5). A failed job turns the workflow red, so
``report-drift`` must be ``continue-on-error``. These tests parse both
scheduled workflows.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"
_CAPTURE_JOBS = {
    "scheduled_run_daily.yml": "ingest",
    "scheduled_run_pre_deadline.yml": "capture",
}


def _load(name: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / name).read_text())


def _grants_issue_write(permissions) -> bool:
    if permissions in ("write-all",):
        return True
    return isinstance(permissions, dict) and permissions.get("issues") == "write"


@pytest.mark.covers("#83 AC7")
@pytest.mark.parametrize("name", sorted(_CAPTURE_JOBS))
def test_only_the_report_drift_job_can_write_issues(name):
    workflow = _load(name)
    jobs = workflow["jobs"]

    assert not _grants_issue_write(workflow.get("permissions"))
    assert "report-drift" in jobs
    assert _grants_issue_write(jobs["report-drift"].get("permissions"))
    for job_name, job in jobs.items():
        if job_name != "report-drift":
            assert not _grants_issue_write(job.get("permissions")), job_name


@pytest.mark.covers("#83 AC5")
@pytest.mark.parametrize("name", sorted(_CAPTURE_JOBS))
def test_report_drift_cannot_change_the_workflow_conclusion(name):
    jobs = _load(name)["jobs"]
    report = jobs["report-drift"]

    assert report.get("continue-on-error") is True
    needs = report.get("needs")
    assert _CAPTURE_JOBS[name] in ([needs] if isinstance(needs, str) else needs)
    assert "always()" in str(report.get("if", ""))
