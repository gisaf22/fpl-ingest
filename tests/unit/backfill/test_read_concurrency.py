"""Parallel S3 reads change the backfill's speed, never its output (#72).

``S3Tree`` prefetches each run's payloads and sidecars with a thread pool
(#72 P1). The read concurrency is a CLI flag and a workflow input, default 16
(P2), and the boto3 client's connection pool is sized to match (P3).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from fpl_ingest.cli import build_parser, run_backfill
from tests.support.backfill_tree import HISTORY_RUN, RUN_1_0_0, build_tree, catalog_key
from tests.support.fake_s3 import FakeS3Client

CONCURRENCIES = (1, 4, 16)


def _args(out: Path, *extra: str):
    return build_parser().parse_args([
        "backfill", "--bucket", "test-bucket", "--write",
        "--report-json", str(out / "report.json"),
        "--summary", str(out / "summary.md"),
        *extra,
    ])


def _run(bucket: Path, out: Path, concurrency: int, **client_kwargs) -> int:
    out.mkdir(parents=True, exist_ok=True)
    args = _args(out, "--read-concurrency", str(concurrency))
    return run_backfill(args, client=FakeS3Client(bucket, **client_kwargs))


def _outputs(bucket: Path, out: Path) -> dict:
    """Every catalog file and the report, minus the wall-clock fields (#72 P5)."""
    catalogs = {}
    for path in sorted((bucket / "raw" / "fpl" / "_catalog").rglob("*.json")):
        body = json.loads(path.read_text())
        body.pop("generated_at")
        catalogs[path.relative_to(bucket).as_posix()] = body
    report = json.loads((out / "report.json").read_text())
    report.pop("runtime_seconds")
    return {"catalogs": catalogs, "report": report}


@pytest.mark.covers("#72 AC1")
def test_catalog_output_is_identical_whatever_the_concurrency(tmp_path):
    source = tmp_path / "source"
    build_tree(source)

    results = {}
    for c in CONCURRENCIES:
        bucket = tmp_path / f"bucket-{c}"
        shutil.copytree(source, bucket)
        assert _run(bucket, tmp_path / f"out-{c}", c) == 0
        results[c] = _outputs(bucket, tmp_path / f"out-{c}")

    assert results[1]["catalogs"], "the sequential run wrote no catalogs"
    for c in CONCURRENCIES[1:]:
        assert results[c] == results[1], f"concurrency {c} differs from 1"


@pytest.mark.covers("#72 AC2")
def test_a_read_error_under_concurrency_exits_non_zero(tmp_path):
    bucket = tmp_path / "bucket"
    tree = build_tree(bucket)
    unreadable = tree.payload_keys[RUN_1_0_0][0]

    assert _run(bucket, tmp_path / "out", 16, failing_keys={unreadable}) != 0


@pytest.mark.covers("#72 AC2")
def test_a_missing_history_sidecar_under_concurrency_is_revalidated(tmp_path):
    # History only: a live capture needs its sidecar's received_at, so B1 fails
    # the run without one, sequentially as in parallel.
    bucket = tmp_path / "bucket"
    tree = build_tree(bucket)
    payload_key = tree.payload_keys[HISTORY_RUN][0]
    bucket.joinpath(*payload_key.rsplit("/", 1)[0].split("/"), "metadata.json").unlink()

    rc = _run(bucket, tmp_path / "out", 16)

    assert rc == 0
    catalog = json.loads(bucket.joinpath(*catalog_key(HISTORY_RUN).split("/")).read_text())
    (entry,) = [e for e in catalog["captures"] if e["key"] == payload_key]
    assert entry["shape_source"] == "revalidated"


@pytest.mark.covers("#72 AC3")
def test_read_concurrency_defaults_to_16(tmp_path):
    assert _args(tmp_path).read_concurrency == 16


@pytest.mark.covers("#72 AC3")
def test_read_concurrency_is_configurable(tmp_path):
    assert _args(tmp_path, "--read-concurrency", "4").read_concurrency == 4


@pytest.mark.covers("#72 AC3")
@pytest.mark.parametrize("value", ["0", "-1", "x"])
def test_read_concurrency_below_one_is_rejected(tmp_path, capsys, value):
    with pytest.raises(SystemExit):
        _args(tmp_path, "--read-concurrency", value)

    err = capsys.readouterr().err
    assert "unrecognized arguments" not in err, "the flag itself is missing"
    assert "--read-concurrency" in err


@pytest.mark.covers("#72 AC3")
@pytest.mark.parametrize("concurrency", [16, 4, 32])
def test_client_pool_is_at_least_the_concurrency(tmp_path, monkeypatch, concurrency):
    import boto3

    bucket = tmp_path / "bucket"
    build_tree(bucket)
    seen = {}

    def fake_client(service, **kwargs):
        seen["config"] = kwargs.get("config")
        return FakeS3Client(bucket)

    monkeypatch.setattr(boto3, "client", fake_client)
    out = tmp_path / "out"
    out.mkdir()

    rc = run_backfill(_args(out, "--read-concurrency", str(concurrency)))

    assert rc == 0
    assert seen["config"] is not None
    assert seen["config"].max_pool_connections >= concurrency
