"""Every scheduled workflow must stamp its manifest with a ``trigger``.

The manifest's ``trigger`` field is the only record of which workflow produced
a run; nothing on the GitHub side survives a workflow's retirement. These
tests pin the invocation each workflow uses, so dropping the flag or the
subcommand fails CI instead of silently producing untagged manifests.
"""

from __future__ import annotations

from pathlib import Path

_WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def _run_lines(name: str) -> list[str]:
    text = (_WORKFLOWS / name).read_text()
    return [line.strip() for line in text.splitlines() if "uv run fpl-ingest" in line]


def test_daily_workflow_passes_a_trigger():
    lines = _run_lines("scheduled_run_daily.yml")
    assert len(lines) == 1
    assert "run --trigger" in lines[0]
    assert "github.event_name == 'schedule'" in lines[0]


def test_pre_deadline_workflow_runs_the_gated_subcommand():
    lines = _run_lines("scheduled_run_pre_deadline.yml")
    assert lines == ["run: uv run fpl-ingest pre-deadline"]
