"""S3 implementation of the raw-capture storage backend.

Implements the same put-only ``RawStorageBackend`` protocol as
``LocalFilesystemBackend`` (see ``local_writer.py``), against the S3 key
template in ``docs/architecture/fpl-ingest-strategy.md`` §A.1:

    s3://<bucket>/raw/{source}/{endpoint}/{extraction_date}/{run_id}/payload.json

The keys ``LocalRawWriter`` builds via ``raw_keys`` are one segment shallower
(no bucket, no ``raw/`` prefix) — this backend prepends ``raw/`` so the same
writer produces the S3-shaped layout without any change to key-building
logic. Credentials are never handled here: boto3 resolves them from the
standard chain (OIDC-federated role in CI via
``aws-actions/configure-aws-credentials``, ``~/.aws/credentials``/
``AWS_PROFILE`` locally).
"""

from __future__ import annotations

from typing import Any

from fpl_ingest.extract.http.local_writer import RawObjectExistsError

_KEY_PREFIX = "raw/"


class S3Backend:
    """Non-overwriting S3 implementation of ``RawStorageBackend``."""

    def __init__(self, bucket: str, *, client: Any | None = None) -> None:
        """Create a backend targeting ``bucket``.

        Args:
            bucket: Destination S3 bucket name.
            client: Pre-built boto3 S3 client, primarily for tests. Defaults
                to ``boto3.client("s3")``, which resolves credentials via the
                standard chain.
        """
        self.bucket = bucket
        self._client = client if client is not None else _default_client()

    def put_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        """Write ``data`` to ``s3://bucket/raw/{key}``.

        Args:
            key: Relative ``/``-joined key from ``raw_keys``.
            data: Exact bytes to write.
            overwrite: Only the run manifest sets this — it is rewritten as
                the run progresses. Payloads and sidecars never do.

        Raises:
            RawObjectExistsError: The key already exists and ``overwrite`` is
                False.
        """
        full_key = _KEY_PREFIX + key
        if not overwrite and self._object_exists(full_key):
            raise RawObjectExistsError(f"raw object already exists: s3://{self.bucket}/{full_key}")
        self._client.put_object(Bucket=self.bucket, Key=full_key, Body=data)
        return self.location(key)

    def location(self, key: str) -> str:
        """Return the ``s3://`` URI a key resolves to."""
        return f"s3://{self.bucket}/{_KEY_PREFIX}{key}"

    def _object_exists(self, full_key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=full_key)
        except Exception as exc:  # noqa: BLE001 - boto3 raises botocore.exceptions.ClientError
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True


def _default_client() -> Any:
    import boto3

    return boto3.client("s3")
