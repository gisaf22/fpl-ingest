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

Building the real boto3 client is guarded: it is refused unless the process
is a GitHub Actions run with a commit SHA, or ``FPL_ALLOW_LOCAL_S3=1`` is set.
Run 20260902T163935Z-07eb06 wrote 651 objects to the production bucket from
outside CI, with no git SHA; the guard sits here, rather than in the CLI's
backend selection, so any caller that builds an ``S3Backend`` is covered.
An injected ``client`` (tests) is not guarded.
"""

from __future__ import annotations

import os
import re
from typing import Any

from fpl_ingest.extract.http.local_writer import RawObjectExistsError

_KEY_PREFIX = "raw/"

#: Setting this to ``1`` allows S3 writes from outside CI.
LOCAL_S3_OVERRIDE_ENV = "FPL_ALLOW_LOCAL_S3"

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class S3WriteNotAllowedError(RuntimeError):
    """Raised when an S3 backend is built outside CI without the override."""


def _assert_s3_writes_allowed() -> None:
    """Refuse S3 unless running in GitHub Actions with a commit SHA, or overridden."""
    if os.environ.get(LOCAL_S3_OVERRIDE_ENV) == "1":
        return
    in_ci = os.environ.get("GITHUB_ACTIONS") == "true"
    has_sha = bool(_SHA_RE.match(os.environ.get("GITHUB_SHA", "")))
    if in_ci and has_sha:
        return
    raise S3WriteNotAllowedError(
        "Refusing to write to S3 outside CI: GITHUB_ACTIONS=true with a 40-character "
        f"GITHUB_SHA is required. To write to S3 from this machine deliberately, set "
        f"{LOCAL_S3_OVERRIDE_ENV}=1. For a local run, unset FPL_STORAGE_BACKEND "
        "(the default is local storage)."
    )


class S3Backend:
    """Non-overwriting S3 implementation of ``RawStorageBackend``."""

    def __init__(self, bucket: str, *, client: Any | None = None) -> None:
        """Create a backend targeting ``bucket``.

        Args:
            bucket: Destination S3 bucket name.
            client: Pre-built boto3 S3 client, primarily for tests. Defaults
                to ``boto3.client("s3")``, which resolves credentials via the
                standard chain.

        Raises:
            S3WriteNotAllowedError: ``client`` is None and the process is
                neither a CI run with a commit SHA nor has
                ``FPL_ALLOW_LOCAL_S3=1`` set.
        """
        if client is None:
            _assert_s3_writes_allowed()
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

    def exists_prefix(self, prefix: str) -> bool:
        """Return whether any object exists under ``s3://bucket/raw/{prefix}/``.

        A trailing ``/`` is enforced on the queried prefix so that, e.g.,
        ``element-summary/1`` cannot match a key actually written under
        ``element-summary/10`` — S3 prefix matching is a plain string
        comparison, not a path-segment one.
        """
        full_prefix = _KEY_PREFIX + prefix
        if not full_prefix.endswith("/"):
            full_prefix += "/"
        response = self._client.list_objects_v2(
            Bucket=self.bucket, Prefix=full_prefix, MaxKeys=1
        )
        return bool(response.get("Contents"))

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
