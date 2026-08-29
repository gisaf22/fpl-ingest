"""Unit tests for S3Backend (extract/http/s3_backend.py).

Exercises the ``RawStorageBackend`` protocol against a fake boto3 S3 client
(no network, no real AWS credentials) — mirroring what test_local_writer.py
checks for the filesystem backend:
  - key layout: ``raw/`` is prepended to the writer-built key
  - no-overwrite: a second write to an existing key raises
  - overwrite=True (manifest updates) bypasses the existence check
  - location() returns the s3:// URI without performing any I/O
"""

from __future__ import annotations

import pytest

from fpl_ingest.extract.http.local_writer import RawObjectExistsError
from fpl_ingest.extract.http.s3_backend import S3Backend

pytestmark = pytest.mark.unit


class _ClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3Client:
    """Minimal in-memory stand-in for a boto3 S3 client."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_calls: list[tuple[str, str, bytes]] = []

    def head_object(self, *, Bucket: str, Key: str) -> None:
        if Key not in self.objects:
            raise _ClientError("404")

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        self.put_calls.append((Bucket, Key, Body))
        self.objects[Key] = Body

    def list_objects_v2(self, *, Bucket: str, Prefix: str, MaxKeys: int = 1000) -> dict:
        matches = [key for key in self.objects if key.startswith(Prefix)][:MaxKeys]
        return {"Contents": [{"Key": key} for key in matches]} if matches else {}


@pytest.fixture
def client() -> FakeS3Client:
    return FakeS3Client()


@pytest.fixture
def backend(client: FakeS3Client) -> S3Backend:
    return S3Backend("fpl-data-safari", client=client)


def test_put_bytes_prepends_raw_prefix(backend: S3Backend, client: FakeS3Client) -> None:
    location = backend.put_bytes("fpl/fixtures/2026-08-24/run-1/payload.json", b'{"a":1}')
    assert location == "s3://fpl-data-safari/raw/fpl/fixtures/2026-08-24/run-1/payload.json"
    assert client.objects["raw/fpl/fixtures/2026-08-24/run-1/payload.json"] == b'{"a":1}'


def test_location_matches_put_bytes_key(backend: S3Backend) -> None:
    key = "fpl/bootstrap-static/2026-08-24/run-1/payload.json"
    assert backend.location(key) == f"s3://fpl-data-safari/raw/{key}"


def test_duplicate_write_raises(backend: S3Backend) -> None:
    key = "fpl/fixtures/2026-08-24/run-1/payload.json"
    backend.put_bytes(key, b"first")
    with pytest.raises(RawObjectExistsError):
        backend.put_bytes(key, b"second")


def test_overwrite_true_bypasses_existence_check(backend: S3Backend, client: FakeS3Client) -> None:
    key = "fpl/_manifests/2026-08-24/run-1/manifest.json"
    backend.put_bytes(key, b"{}")
    backend.put_bytes(key, b'{"status": "SUCCESS"}', overwrite=True)
    assert client.objects["raw/" + key] == b'{"status": "SUCCESS"}'


def test_no_overwrite_write_does_not_call_head_object_twice_incorrectly(
    backend: S3Backend, client: FakeS3Client
) -> None:
    key = "fpl/event-status/2026-08-24/run-1/payload.json"
    backend.put_bytes(key, b"data")
    assert len(client.put_calls) == 1


def test_exists_prefix_true_after_a_capture_under_a_different_date_and_run(
    backend: S3Backend,
) -> None:
    """The regression scenario: a gameweek captured to S3 by an earlier run,
    under a date/run_id this run knows nothing about, must still be found by
    its endpoint prefix alone."""
    backend.put_bytes(
        "fpl/event-live/01/2026-08-10/20260810T080000Z-aaaaaa/payload.json", b"{}"
    )

    assert backend.exists_prefix("fpl/event-live/01") is True


def test_exists_prefix_false_when_nothing_was_ever_captured(backend: S3Backend) -> None:
    assert backend.exists_prefix("fpl/event-live/01") is False


def test_exists_prefix_does_not_match_a_longer_sibling_id(
    backend: S3Backend,
) -> None:
    """``element-summary/1`` must not be satisfied by a capture under
    ``element-summary/10`` — S3 prefix matching is string-based, not
    path-segment-based, so the boundary must be enforced explicitly."""
    backend.put_bytes(
        "fpl/element-summary/10/2026-08-24/run-1/payload.json", b"{}"
    )

    assert backend.exists_prefix("fpl/element-summary/1") is False
    assert backend.exists_prefix("fpl/element-summary/10") is True
