"""The backfill's S3 writer is write-once, enforced by S3 (#67 AC3; #63 D2, E7).

Every catalog PUT carries ``If-None-Match: *``. S3 answers an existing key with
412, which the writer reports as "already present" so the run skips it. Any
other refusal, a 409 conditional-write conflict included, is an operational
error and is raised.
"""

from __future__ import annotations

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from fpl_ingest.backfill import S3Tree

BUCKET = "test-bucket"
KEY = "raw/fpl/_catalog/backfill/20260829T012300Z-aaaaaa.json"
BODY = b'{"run_id": "20260829T012300Z-aaaaaa"}\n'


def _stubbed():
    client = boto3.client("s3", region_name="eu-west-2", aws_access_key_id="x", aws_secret_access_key="x")
    return client, Stubber(client)


def _expect_put() -> dict:
    return {"Bucket": BUCKET, "Key": KEY, "Body": BODY, "IfNoneMatch": "*"}


@pytest.mark.covers("#67 AC3")
def test_new_catalog_is_written_with_if_none_match():
    client, stubber = _stubbed()
    stubber.add_response("put_object", {}, _expect_put())

    with stubber:
        written = S3Tree(BUCKET, client=client).put_if_absent(KEY, BODY)

    assert written is True
    stubber.assert_no_pending_responses()


@pytest.mark.covers("#67 AC3")
def test_existing_catalog_refused_by_s3_is_skipped_not_failed():
    client, stubber = _stubbed()
    stubber.add_client_error(
        "put_object", service_error_code="PreconditionFailed", http_status_code=412,
        expected_params=_expect_put(),
    )

    with stubber:
        written = S3Tree(BUCKET, client=client).put_if_absent(KEY, BODY)

    assert written is False
    stubber.assert_no_pending_responses()


@pytest.mark.covers("#67 AC3")
@pytest.mark.parametrize(
    ("code", "status"),
    [
        pytest.param("ConditionalRequestConflict", 409, id="concurrent-conditional-write"),
        pytest.param("AccessDenied", 403, id="access-denied"),
        pytest.param("InternalError", 500, id="server-error"),
    ],
)
def test_other_put_errors_fail_the_run(code, status):
    client, stubber = _stubbed()
    stubber.add_client_error(
        "put_object", service_error_code=code, http_status_code=status,
        expected_params=_expect_put(),
    )

    with stubber, pytest.raises(ClientError):
        S3Tree(BUCKET, client=client).put_if_absent(KEY, BODY)
