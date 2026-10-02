"""A dry run reads the bucket and writes nothing (#67 AC2a; E1).

``DryRunTree`` wraps the S3 tree: the builder runs in full, but every write is
recorded rather than sent, and the report says it is a dry run and which
catalog files it would have written.
"""

from __future__ import annotations

import pytest

from fpl_ingest.backfill import DryRunTree, S3Tree, build_backfill
from tests.support.backfill_tree import IN_SCOPE_RUNS, VALIDATOR_VERSION, build_tree, catalog_key
from tests.support.fake_s3 import FakeS3Client


@pytest.mark.covers("#67 AC2a")
def test_dry_run_sends_no_put_and_lists_what_it_would_write(tmp_path):
    build_tree(tmp_path)
    client = FakeS3Client(tmp_path)

    report = build_backfill(DryRunTree(S3Tree("test-bucket", client=client)), validator_version=VALIDATOR_VERSION)

    assert client.puts == []
    assert not any(k.startswith("raw/fpl/_catalog/") for k in client.keys())
    summary = report.to_json()
    assert summary["dry_run"] is True
    assert sorted(summary["written"]) == sorted(catalog_key(r) for r in IN_SCOPE_RUNS)
    assert "Dry run" in report.to_markdown()
