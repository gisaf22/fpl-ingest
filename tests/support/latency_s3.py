"""An in-memory boto3 S3 client whose every call takes 50-80 ms, as the capture bucket's do from CI (#123).

Run 39552536884's S3 calls averaged about 77 ms. Each call here sleeps in its own
thread for a seeded random 50-80 ms, so throughput tests can tell overlapping calls
from serialized ones by comparing a stage's wall time with ``total_call_seconds``.
``peak_in_flight`` is the most calls that were ever sleeping at once.

Serves the calls ``S3Backend`` makes: ``list_objects_v2``, ``put_object`` (honouring
``IfNoneMatch="*"`` with a 412, as S3 does), ``head_object`` and ``get_object``.
"""

from __future__ import annotations

import io
import random
import threading
import time

from botocore.exceptions import ClientError

MIN_SECONDS = 0.05
MAX_SECONDS = 0.08


def _error(code: str, status: int, op: str) -> ClientError:
    return ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, op)


class LatencyS3Client:
    def __init__(self, *, seed: int = 123) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[str] = []
        self.in_flight = 0
        self.peak_in_flight = 0
        self.total_call_seconds = 0.0
        self._random = random.Random(seed)
        self._lock = threading.Lock()

    def _call(self, op: str) -> None:
        with self._lock:
            self.calls.append(op)
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
            delay = self._random.uniform(MIN_SECONDS, MAX_SECONDS)
        started = time.monotonic()
        time.sleep(delay)
        with self._lock:
            self.in_flight -= 1
            self.total_call_seconds += time.monotonic() - started

    def list_objects_v2(self, *, Bucket: str, Prefix: str, MaxKeys: int = 1000) -> dict:  # noqa: N803 - boto3 names
        self._call("list_objects_v2")
        with self._lock:
            matches = sorted(key for key in self.objects if key.startswith(Prefix))[:MaxKeys]
        return {"Contents": [{"Key": key} for key in matches]} if matches else {}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, IfNoneMatch: str | None = None) -> dict:  # noqa: N803
        self._call("put_object")
        with self._lock:
            if IfNoneMatch == "*" and Key in self.objects:
                raise _error("PreconditionFailed", 412, "PutObject")
            self.objects[Key] = Body
        return {}

    def head_object(self, *, Bucket: str, Key: str) -> dict:  # noqa: N803
        self._call("head_object")
        with self._lock:
            if Key not in self.objects:
                raise _error("404", 404, "HeadObject")
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> dict:  # noqa: N803
        self._call("get_object")
        with self._lock:
            if Key not in self.objects:
                raise _error("NoSuchKey", 404, "GetObject")
            return {"Body": io.BytesIO(self.objects[Key])}
