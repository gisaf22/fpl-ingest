"""The first scheduled run after deploy writes a valid 2.1.0 manifest (#62 AC6).

Read-only against the capture bucket. Run by hand after the first scheduled
run post-merge, with credentials exported and ``FPL_S3_BUCKET`` set; it skips
when either is missing. The result is recorded on #62.
"""

from __future__ import annotations

import json
import os

import boto3
import pytest

from tests.support import raw_contract_schema

_MANIFEST_PREFIX = "raw/fpl/_manifests/"


def _s3_or_skip():
    bucket = os.environ.get("FPL_S3_BUCKET")
    if not bucket:
        pytest.skip("FPL_S3_BUCKET is not set")
    session = boto3.Session()
    if session.get_credentials() is None:
        pytest.skip("no AWS credentials in the environment")
    return session.client("s3"), bucket


def _latest_finalized_manifest(client, bucket: str) -> dict:
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=_MANIFEST_PREFIX):
        keys.extend(o["Key"] for o in page.get("Contents", []) if o["Key"].endswith("/manifest.json"))
    # {extraction_date}/{run_id}/manifest.json sorts chronologically as a string.
    for key in sorted(keys, reverse=True):
        manifest = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        if manifest.get("status") != "IN_PROGRESS":
            return manifest
    pytest.fail(f"no finalized manifest under s3://{bucket}/{_MANIFEST_PREFIX}")


@pytest.mark.covers("#62 AC6")
def test_live_manifest_is_2_1_0():
    client, bucket = _s3_or_skip()

    manifest = _latest_finalized_manifest(client, bucket)

    assert manifest["raw_contract_version"] == "2.1.0", manifest["run_id"]
    assert raw_contract_schema.errors("manifest", manifest) == []
    assert len(manifest["captures"]) == manifest["totals"]["written"]
