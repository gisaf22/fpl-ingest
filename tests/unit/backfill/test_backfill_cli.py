"""The backfill CLI's exit code (#67 AC5; #63 D4).

B only reports: a shape failure is listed in the report and the command still
exits 0. An operational error, such as a payload that cannot be read, exits
non-zero so the workflow job fails.
"""

from __future__ import annotations

import json

import pytest

from fpl_ingest.cli import build_parser, run_backfill
from tests.support.backfill_tree import RUN_1_0_0, build_tree
from tests.support.fake_s3 import FakeS3Client


def _args(tmp_path, *extra):
    return build_parser().parse_args([
        "backfill", "--bucket", "test-bucket",
        "--report-json", str(tmp_path / "report.json"),
        "--summary", str(tmp_path / "summary.md"),
        *extra,
    ])


@pytest.mark.covers("#67 AC5")
def test_shape_failures_are_reported_and_exit_zero(tmp_path):
    root = tmp_path / "bucket"
    build_tree(root)

    rc = run_backfill(_args(tmp_path), client=FakeS3Client(root))

    assert rc == 0
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["shape_failure_count"] > 0
    assert report["shape_failures"]["live"] and report["shape_failures"]["history"]
    assert "Shape failures" in (tmp_path / "summary.md").read_text()


@pytest.mark.covers("#67 AC5")
def test_a_read_error_exits_non_zero(tmp_path):
    root = tmp_path / "bucket"
    tree = build_tree(root)
    unreadable = tree.payload_keys[RUN_1_0_0][0]

    rc = run_backfill(_args(tmp_path), client=FakeS3Client(root, failing_keys={unreadable}))

    assert rc != 0
