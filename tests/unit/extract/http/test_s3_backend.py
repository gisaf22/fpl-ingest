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
