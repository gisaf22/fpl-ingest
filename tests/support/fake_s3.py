"""An in-memory stand-in for the boto3 S3 client calls the backfill makes (#67).

Serves a bucket-shaped local tree (``tests.support.backfill_tree``) through the
client surface ``S3Tree`` uses: the ``list_objects_v2`` paginator,
``get_object``, ``head_object`` and ``put_object``. Every PUT is recorded, so a
test can assert that none was sent.
"""

from __future__ import annotations

import io
from pathlib import Path

from botocore.exceptions import ClientError


def _error(code: str, status: int, op: str) -> ClientError:
    return ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, op)


class _Paginator:
    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    def paginate(self, *, Bucket: str, Prefix: str = ""):  # noqa: N803 - boto3 names
        keys = sorted(k for k in self._client.keys() if k.startswith(Prefix))
        yield {"Contents": [{"Key": k} for k in keys]} if keys else {}


class FakeS3Client:
    def __init__(self, root: Path, *, failing_keys: set[str] = frozenset()) -> None:
        self.root = root
        self.failing_keys = set(failing_keys)
        self.puts: list[dict] = []

    def keys(self) -> list[str]:
        return [p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file()]

    def _path(self, key: str) -> Path:
        return self.root.joinpath(*key.split("/"))

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return _Paginator(self)

    def get_object(self, *, Bucket: str, Key: str):  # noqa: N803
        if Key in self.failing_keys:
            raise _error("InternalError", 500, "GetObject")
        path = self._path(Key)
        if not path.is_file():
            raise _error("NoSuchKey", 404, "GetObject")
        return {"Body": io.BytesIO(path.read_bytes())}

    def head_object(self, *, Bucket: str, Key: str):  # noqa: N803
        if not self._path(Key).is_file():
            raise _error("404", 404, "HeadObject")
        return {}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        path = self._path(kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and path.is_file():
            raise _error("PreconditionFailed", 412, "PutObject")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(kwargs["Body"])
        return {}
