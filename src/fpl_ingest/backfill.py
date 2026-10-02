"""Backfill catalogs for runs captured before the 2.1.0 capture index (#63, #66).

A 2.1.0 run lists its payloads in its own manifest's ``captures[]``. Earlier
runs, and the ported 2025-26 history run, have no such list. This module builds
one for each: a catalog file per run under
``raw/{source}/_catalog/backfill/{run_id}.json`` whose entries mirror
``captures[]`` (so a consumer can union the two) plus where each entry's shape
verdict came from.

- **Scope:** every live run not indexed by a manifest at 2.1.0 or later, plus
  every run under ``history/``.
- **Shape (D7, D10):** a sidecar verdict is trusted when it is a dict with
  ``ok``. Anything else is revalidated with the endpoint's current validator.
- **Season (D5, D6):** a live run's season comes from its own bootstrap-static,
  only when that capture is shape-ok, by A's rule (``resolve_season``). Without
  one it is null and the run is listed. History takes its season from its key.
- **Write-once (D2):** a run whose catalog already exists is skipped before it
  is built, and a write never replaces an existing file.

- **Date range (#67 E2):** optional ``from_date`` / ``to_date`` select whole
  runs by the date their ``run_id`` starts on, never single payloads, so no
  catalog is ever written for part of a run.

The builder reads and writes through a small tree interface. ``LocalTree`` is
the filesystem form and ``S3Tree`` the bucket form (#67), whose writes are
conditional (``If-None-Match: *``) so S3 itself refuses an overwrite.
``DryRunTree`` wraps either and sends no write. A shape failure is reported,
never raised: B reports only, and C1 decides exclusion.
"""

from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.raw_keys import (
    BUCKET_KEY_PREFIX,
    MANIFEST_FILENAME,
    MANIFEST_PREFIX,
    METADATA_FILENAME,
    PAYLOAD_STEM,
    backfill_catalog_key,
)
from fpl_ingest.extract.season import resolve_season
from fpl_ingest.extract.stages.bootstrap import validate_bootstrap_shape
from fpl_ingest.extract.stages.element_summary import validate_element_summary_shape
from fpl_ingest.extract.stages.event_status import validate_event_status_shape
from fpl_ingest.extract.stages.fixtures import validate_fixtures_shape
from fpl_ingest.extract.stages.gameweeks import validate_gameweek_shape

logger = logging.getLogger(__name__)

SOURCE = "fpl"
HISTORY_ROOT = "history/"
SHAPE_SOURCE_SIDECAR = "sidecar"
SHAPE_SOURCE_REVALIDATED = "revalidated"

#: Runs whose manifest is at this version or later index themselves.
INDEXED_FROM_VERSION = (2, 1, 0)

#: The current validator for each endpoint, by its first segment.
VALIDATORS: dict[str, Callable[[RawResponse], dict[str, Any]]] = {
    "bootstrap-static": validate_bootstrap_shape,
    "fixtures": validate_fixtures_shape,
    "event-status": validate_event_status_shape,
    "event-live": validate_gameweek_shape,
    "element-summary": validate_element_summary_shape,
}

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class Tree(Protocol):
    """The bucket as the backfill sees it: list, read, and write-if-absent."""

    def list_keys(self, prefix: str) -> Iterator[str]: ...

    def get_bytes(self, key: str) -> bytes | None: ...

    def exists(self, key: str) -> bool: ...

    def put_if_absent(self, key: str, data: bytes) -> bool: ...

    def prefetch(self, keys: list[str]) -> None:
        """Hint: the keys the builder is about to read for one run (#72)."""
        ...


class LocalTree:
    """A bucket-shaped tree on the local filesystem, keys relative to ``root``."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root.joinpath(*key.split("/"))

    def list_keys(self, prefix: str) -> Iterator[str]:
        base = self._path(prefix.rstrip("/"))
        if not base.is_dir():
            return
        for path in sorted(base.rglob("*")):
            if path.is_file():
                yield path.relative_to(self.root).as_posix()

    def get_bytes(self, key: str) -> bytes | None:
        path = self._path(key)
        return path.read_bytes() if path.is_file() else None

    def prefetch(self, keys: list[str]) -> None:
        """No-op: local reads are cheap."""

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def put_if_absent(self, key: str, data: bytes) -> bool:
        """Write ``data`` unless ``key`` exists. Returns whether it wrote."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as fh:
                fh.write(data)
        except FileExistsError:
            return False
        return True


class S3Tree:
    """The capture bucket, keys relative to the bucket root (#67).

    Reads go through the caller's boto3 client. ``put_if_absent`` sends
    ``If-None-Match: *``: S3 answers an existing key with 412, reported as
    "not written" so the run skips it (E7). Any other error, a 409
    conditional-write conflict included, is raised: the workflow's
    concurrency group means a second writer should never exist.

    ``prefetch`` reads one run's keys with ``concurrency`` threads and holds
    them until ``get_bytes`` takes them (#72). The cache holds one run at a
    time: each prefetch replaces it. A missing key prefetches as ``None``; any
    other error is raised from ``prefetch`` and the pending reads are cancelled.
    The one client is shared across threads, which boto3 clients allow.
    """

    def __init__(self, bucket: str, *, client: Any, concurrency: int = 1) -> None:
        if concurrency < 1:
            raise ValueError(f"concurrency must be at least 1, got {concurrency}")
        self.bucket = bucket
        self._client = client
        self._concurrency = concurrency
        self._cache: dict[str, bytes | None] = {}

    def list_keys(self, prefix: str) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def prefetch(self, keys: list[str]) -> None:
        self._cache = {}
        with ThreadPoolExecutor(max_workers=self._concurrency) as pool:
            futures = {key: pool.submit(self._read, key) for key in dict.fromkeys(keys)}
            try:
                cache = {key: future.result() for key, future in futures.items()}
            except BaseException:
                for future in futures.values():
                    future.cancel()
                raise
        self._cache = cache

    def get_bytes(self, key: str) -> bytes | None:
        if key in self._cache:
            return self._cache.pop(key)
        return self._read(key)

    def _read(self, key: str) -> bytes | None:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:  # noqa: BLE001 - boto3 raises botocore.exceptions.ClientError
            if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
        except Exception as exc:  # noqa: BLE001
            if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def put_if_absent(self, key: str, data: bytes) -> bool:
        """Write ``data`` unless ``key`` exists, enforced by S3. Returns whether it wrote."""
        try:
            self._client.put_object(Bucket=self.bucket, Key=key, Body=data, IfNoneMatch="*")
        except Exception as exc:  # noqa: BLE001
            if _error_code(exc) == "PreconditionFailed":
                return False
            raise
        return True


class DryRunTree:
    """Reads through ``inner``; records each write instead of sending it (#67 E1)."""

    dry_run = True

    def __init__(self, inner: Tree) -> None:
        self._inner = inner
        self.would_write: list[str] = []

    def list_keys(self, prefix: str) -> Iterator[str]:
        return self._inner.list_keys(prefix)

    def get_bytes(self, key: str) -> bytes | None:
        return self._inner.get_bytes(key)

    def prefetch(self, keys: list[str]) -> None:
        self._inner.prefetch(keys)

    def exists(self, key: str) -> bool:
        return self._inner.exists(key)

    def put_if_absent(self, key: str, data: bytes) -> bool:
        self.would_write.append(key)
        return True


def _error_code(exc: Exception) -> str | None:
    return getattr(exc, "response", {}).get("Error", {}).get("Code")


@dataclass(frozen=True)
class ShapeResult:
    """One capture's shape verdict and where it came from."""

    shape_ok: bool
    shape_source: str
    validator_version: str | None
    failures: list[str]


def shape_for_capture(
    endpoint: str,
    sidecar: Mapping[str, Any] | None,
    payload: bytes,
    *,
    validator_version: str,
) -> ShapeResult:
    """Return the capture's shape verdict (#63 D7, D10).

    A sidecar ``shape_validation`` that is a dict with ``ok`` is used as
    recorded. An absent, null or unrecognised verdict means the payload is
    revalidated with ``endpoint``'s current validator.
    """
    verdict = (sidecar or {}).get("shape_validation")
    if isinstance(verdict, Mapping) and "ok" in verdict:
        ok = bool(verdict["ok"])
        failures = [] if ok else [str(f) for f in verdict.get("failures") or []]
        return ShapeResult(ok, SHAPE_SOURCE_SIDECAR, None, failures)

    validate = VALIDATORS.get(endpoint.split("/")[0])
    if validate is None:
        raise ValueError(f"no shape validator for endpoint {endpoint!r}")
    status = (sidecar or {}).get("http_status")
    raw = RawResponse(
        url="",
        # The history run never recorded a status (D5); it was only ever
        # written for a 2xx, so the status check must not fail it.
        status=status if isinstance(status, int) else 200,
        headers={},
        body=payload,
        requested_at=_EPOCH,
        received_at=_EPOCH,
    )
    result = validate(raw)
    ok = bool(result["ok"])
    failures = [] if ok else [str(f) for f in result.get("failures") or []]
    return ShapeResult(ok, SHAPE_SOURCE_REVALIDATED, validator_version, failures)


@dataclass
class BackfillReport:
    """What one backfill pass did (#63 D4)."""

    dry_run: bool = False
    runtime_seconds: float | None = None
    indexed: int = 0
    revalidated: int = 0
    shape_failures: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: {"live": [], "history": []}
    )
    null_season_runs: list[str] = field(default_factory=list)
    written: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def shape_failure_count(self) -> int:
        return sum(len(v) for v in self.shape_failures.values())

    def to_json(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "runtime_seconds": self.runtime_seconds,
            "indexed": self.indexed,
            "revalidated": self.revalidated,
            "shape_failure_count": self.shape_failure_count,
            "shape_failures": {k: list(v) for k, v in self.shape_failures.items()},
            "null_season_runs": list(self.null_season_runs),
            "written": list(self.written),
            "skipped": list(self.skipped),
        }

    def to_markdown(self) -> str:
        lines = ["## Capture-index backfill", ""]
        if self.dry_run:
            lines += ["**Dry run: nothing was written.** \"Would write\" counts the files a real run writes.", ""]
        written_label = "Catalog files that would be written" if self.dry_run else "Catalog files written"
        lines += [
            f"- Indexed: {self.indexed}",
            f"- Revalidated: {self.revalidated}",
            f"- Shape failures: {self.shape_failure_count}",
            f"- {written_label}: {len(self.written)}, skipped (already present): {len(self.skipped)}",
        ]
        if self.runtime_seconds is not None:
            lines.append(f"- Runtime: {self.runtime_seconds:.1f}s")
        lines.append("")
        if self.shape_failure_count == 0:
            lines += ["0 shape failures.", ""]
        for scope, heading in (("live", "Live"), ("history", "History")):
            failures = self.shape_failures[scope]
            lines += [f"### {heading}", ""]
            if not failures:
                lines += ["0 shape failures.", ""]
                continue
            lines += [
                "| key | run_id | endpoint | extraction_date | shape_source | failures |",
                "|---|---|---|---|---|---|",
            ]
            for f in failures:
                messages = "; ".join(f["failures"]).replace("|", "\\|")
                lines.append(
                    f"| `{f['key']}` | {f['run_id']} | {f['endpoint']} | "
                    f"{f['extraction_date']} | {f['shape_source']} | {messages} |"
                )
            lines.append("")
        lines += ["### Runs with null season", ""]
        lines += [f"- {r}" for r in self.null_season_runs] or ["None."]
        return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class _Payload:
    key: str
    endpoint: str
    extraction_date: str
    run_id: str


def build_backfill(
    tree: Tree,
    *,
    validator_version: str,
    from_date: date | None = None,
    to_date: date | None = None,
) -> BackfillReport:
    """Write a catalog for every in-scope run that lacks one, and report.

    A run whose catalog exists is skipped before it is built (D2), so a rerun
    after a complete pass reads no payloads and writes nothing. ``from_date``
    and ``to_date`` (inclusive) keep only runs whose ``run_id`` starts in that
    range; a run is always built whole (E2).
    """
    report = BackfillReport(dry_run=getattr(tree, "dry_run", False))
    runs = _in_scope_runs(tree)
    for (scope, run_id), payloads in sorted(runs.items(), key=lambda kv: kv[0][1]):
        started = _run_start_date(run_id)
        if (from_date and started < from_date) or (to_date and started > to_date):
            continue
        catalog_key = BUCKET_KEY_PREFIX + backfill_catalog_key(SOURCE, run_id)
        if tree.exists(catalog_key):
            report.skipped.append(catalog_key)
            continue
        catalog = _build_catalog(tree, scope, run_id, payloads, validator_version, report)
        data = json.dumps(catalog, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
        if tree.put_if_absent(catalog_key, data):
            report.written.append(catalog_key)
        else:
            report.skipped.append(catalog_key)
    report.null_season_runs.sort()
    return report


def _in_scope_runs(tree: Tree) -> dict[tuple[str, str], list[_Payload]]:
    """Group every payload in scope by ``(scope, run_id)``."""
    live_root = f"{BUCKET_KEY_PREFIX}{SOURCE}/"
    indexed = _self_indexed_runs(tree, live_root)
    runs: dict[tuple[str, str], list[_Payload]] = {}
    for key in tree.list_keys(live_root):
        payload = _parse_payload_key(key, endpoint_from=len(live_root.split("/")) - 1)
        if payload is not None and payload.run_id not in indexed:
            runs.setdefault(("live", payload.run_id), []).append(payload)
    for key in tree.list_keys(HISTORY_ROOT):
        # history/{season}/{source}/{endpoint...}/{date}/{run_id}/payload.*
        payload = _parse_payload_key(key, endpoint_from=3)
        if payload is not None:
            runs.setdefault(("history", payload.run_id), []).append(payload)
    return runs


def _self_indexed_runs(tree: Tree, live_root: str) -> set[str]:
    """Run ids whose manifest is at 2.1.0 or later, and so lists its own captures."""
    indexed: set[str] = set()
    for key in tree.list_keys(f"{live_root}{MANIFEST_PREFIX}/"):
        if not key.endswith(f"/{MANIFEST_FILENAME}"):
            continue
        manifest = json.loads(tree.get_bytes(key) or b"{}")
        if _version(manifest.get("raw_contract_version")) >= INDEXED_FROM_VERSION:
            indexed.add(key.split("/")[-2])
    return indexed


def _version(value: Any) -> tuple[int, ...]:
    try:
        return tuple(int(p) for p in str(value).split("."))
    except ValueError:
        return (0,)


def _parse_payload_key(key: str, *, endpoint_from: int) -> _Payload | None:
    """Return the payload a key names, or None for anything else (sidecars, ``_`` prefixes)."""
    parts = key.split("/")
    if not parts[-1].startswith(f"{PAYLOAD_STEM}.") or len(parts) < endpoint_from + 4:
        return None
    endpoint_parts = parts[endpoint_from:-3]
    if endpoint_parts[0].startswith("_"):
        return None
    return _Payload(
        key=key,
        endpoint="/".join(endpoint_parts),
        extraction_date=parts[-3],
        run_id=parts[-2],
    )


def _build_catalog(
    tree: Tree,
    scope: str,
    run_id: str,
    payloads: list[_Payload],
    validator_version: str,
    report: BackfillReport,
) -> dict[str, Any]:
    ordered = sorted(payloads, key=lambda p: p.key)
    tree.prefetch([k for p in ordered for k in (p.key, _sidecar_key(p.key))])
    rows = []
    for p in ordered:
        body = tree.get_bytes(p.key)
        if body is None:
            raise FileNotFoundError(p.key)
        sidecar_bytes = tree.get_bytes(_sidecar_key(p.key))
        sidecar = json.loads(sidecar_bytes) if sidecar_bytes is not None else None
        shape = shape_for_capture(p.endpoint, sidecar, body, validator_version=validator_version)
        rows.append((p, body, sidecar, shape))

    if scope == "history":
        season: str | None = payloads[0].key.split("/")[1]
    else:
        season = _live_season(rows)
        if season is None:
            report.null_season_runs.append(run_id)

    entries = []
    for p, body, sidecar, shape in rows:
        report.indexed += 1
        if shape.shape_source == SHAPE_SOURCE_REVALIDATED:
            report.revalidated += 1
        if not shape.shape_ok:
            report.shape_failures[scope].append({
                "key": p.key,
                "run_id": p.run_id,
                "endpoint": p.endpoint,
                "extraction_date": p.extraction_date,
                "shape_source": shape.shape_source,
                "failures": shape.failures,
            })
        entries.append({
            "key": p.key,
            "endpoint": p.endpoint,
            "received_at": _run_instant(run_id) if scope == "history" else (sidecar or {})["received_at"],
            "content_sha256": hashlib.sha256(body).hexdigest(),
            "content_length": len(body),
            "http_status": None if scope == "history" else (sidecar or {}).get("http_status"),
            "shape_ok": shape.shape_ok,
            "usable": shape.shape_ok,
            "season": season,
            "shape_source": shape.shape_source,
            "validator_version": shape.validator_version,
        })

    return {
        "run_id": run_id,
        "source": SOURCE,
        "scope": scope,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "captures": entries,
    }


def _sidecar_key(payload_key: str) -> str:
    return payload_key.rsplit("/", 1)[0] + "/" + METADATA_FILENAME


def _live_season(rows: list[tuple[_Payload, bytes, Any, ShapeResult]]) -> str | None:
    """A's rule over the run's own bootstrap-static, only when it is shape-ok (D10)."""
    for p, body, _sidecar, shape in rows:
        if p.endpoint == "bootstrap-static" and shape.shape_ok:
            try:
                payload = json.loads(body)
            except ValueError:
                return None
            return resolve_season(payload, shape_ok=True, logger=logger)
    return resolve_season(None, shape_ok=False, logger=logger)


def _run_start_date(run_id: str) -> date:
    """The UTC date a run id's prefix names."""
    return datetime.strptime(run_id.split("-")[0], "%Y%m%dT%H%M%SZ").date()


def _run_instant(run_id: str) -> str:
    """The instant a run id's prefix names, as a UTC timestamp (D5)."""
    started = datetime.strptime(run_id.split("-")[0], "%Y%m%dT%H%M%SZ")
    return started.strftime("%Y-%m-%dT%H:%M:%SZ")
