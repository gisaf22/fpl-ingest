"""Local-disk writer for the raw-capture contract.

Implements sections A.1 (key layout), A.5 (per-object sidecar and per-run
manifest), and A.6 (the Understat HTML companion file) of
``docs/architecture/fpl-ingest-strategy.md``, against a local root directory
instead of S3. The key layout is identical to the S3 template one level
shallower — there is no bucket segment — so moving to S3 later replaces the
storage backend only, never the keys, the sidecar shape, or the manifest shape.

Three properties this writer guarantees, all of which the current
``write_json_cache`` path lacks:

  * **Verbatim bytes.** ``write_object`` takes the response body as received
    and writes those bytes unchanged. It never re-serialises decoded JSON, so
    ``content_sha256`` is the checksum of what the source actually sent.
  * **Atomic writes.** Every file is written to a ``.tmp`` sibling and then
    ``os.replace``d into place, and the temporary file is removed if the write
    fails. A partial file is never visible under its final name.
  * **No overwrites.** Objects are immutable. A second write to an existing
    payload path is a caller bug — a reused ``run_id`` — and raises rather
    than silently replacing captured data.

The writer knows nothing about any particular endpoint: it takes bytes plus
capture metadata and writes files. Stage wiring lives elsewhere.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from fpl_ingest.extract.http import raw_keys
from fpl_ingest.extract.http.raw_keys import (
    RAW_CONTRACT_VERSION,
    RawKeyError,
    iso_utc,
    manifest_key,
    metadata_key,
    payload_filename,
    payload_key,
)
from fpl_ingest.orchestration.run_status import RunStatus

__all__ = [
    "LocalRawWriter",
    "LocalFilesystemBackend",
    "RawStorageBackend",
    "RawObjectExistsError",
    "WriteResult",
    "ManifestResult",
    "RAW_CONTRACT_VERSION",
    "RawKeyError",
]

MANIFEST_STATUS_IN_PROGRESS = "IN_PROGRESS"


class RawObjectExistsError(FileExistsError):
    """Raised when a raw object key already exists.

    Raw objects are immutable, so this always indicates a caller bug — most
    likely two runs sharing one ``run_id``.
    """


class RawStorageBackend(Protocol):
    """Minimal storage surface the writer and selection logic depend on.

    Deliberately narrow: writes never read back what they wrote, and the only
    read this surface offers is ``exists_prefix``, an existence check used by
    stages that decide whether a gameweek or player has already been
    captured. An S3 backend needs the same methods and no more.
    """

    def put_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        """Write ``data`` at ``key`` and return the resolved location."""

    def location(self, key: str) -> str:
        """Return the human-readable location a key resolves to."""

    def exists_prefix(self, prefix: str) -> bool:
        """Return whether any object has ever been written under ``prefix``."""


class LocalFilesystemBackend:
    """Atomic, non-overwriting local-disk implementation of the backend."""

    def __init__(self, root_dir: Path | str) -> None:
        self.root = Path(root_dir).expanduser()

    def put_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> str:
        """Write ``data`` to ``root/key`` via a temporary file and ``os.replace``.

        Args:
            key: Relative ``/``-joined key from ``raw_keys``.
            data: Exact bytes to write.
            overwrite: Only the run manifest sets this — it is rewritten as the
                run progresses. Payloads and sidecars never do.

        Raises:
            RawObjectExistsError: The key already exists and ``overwrite`` is
                False.
        """
        path = self._path_for(key)
        if path.exists() and not overwrite:
            raise RawObjectExistsError(f"raw object already exists: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        try:
            self._write_tmp(tmp, data)
            os.replace(tmp, path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return str(path)

    def location(self, key: str) -> str:
        """Return the absolute local path a key resolves to."""
        return str(self._path_for(key))

    def exists_prefix(self, prefix: str) -> bool:
        """Return whether ``root/prefix`` exists as a directory."""
        return self._path_for(prefix).is_dir()

    def _write_tmp(self, tmp: Path, data: bytes) -> None:
        """Write and flush the temporary file. Overridden in tests to fail."""
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

    def _path_for(self, key: str) -> Path:
        return self.root.joinpath(*key.split("/"))


@dataclass(frozen=True)
class WriteResult:
    """Outcome of one successful object write."""

    source: str
    endpoint: str
    run_id: str
    extraction_date: str
    payload_key: str
    metadata_key: str
    payload_location: str
    content_length: int
    content_sha256: str
    companion_keys: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ManifestResult:
    """Outcome of finalising a run manifest."""

    run_id: str
    source: str
    extraction_date: str
    manifest_key: str
    manifest_location: str
    status: str
    manifest: dict[str, Any]


class LocalRawWriter:
    """Write raw captures and their run manifest under a local root directory.

    One instance corresponds to one run of one source. The manifest is updated
    on disk after every recorded object or failure, so a run that dies partway
    still leaves an honest partial manifest behind, and ``finalize`` stamps the
    terminal status.
    """

    def __init__(
        self,
        root_dir: Path | str,
        source: str,
        *,
        run_id: str | None = None,
        started_at: datetime | None = None,
        extraction_date: str | None = None,
        backend: RawStorageBackend | None = None,
    ) -> None:
        """Create a writer for one run of one source.

        Args:
            root_dir: Local raw root, mirroring the S3 ``raw/`` prefix.
            source: ``fpl``, ``understat``, or ``reep``.
            run_id: Existing run id, or None to mint one from ``started_at``.
            started_at: Run start instant; defaults to now (UTC). Both the run
                id and the extraction date derive from it.
            extraction_date: Override for the derived ``YYYY-MM-DD`` date.
            backend: Storage backend; defaults to the local filesystem.
        """
        raw_keys.validate_source(source)
        self.source = source
        self.started_at = started_at or datetime.now(timezone.utc)
        self.run_id = run_id or raw_keys.new_run_id(self.started_at)
        raw_keys.validate_run_id(self.run_id)
        self.extraction_date = extraction_date or raw_keys.extraction_date_for(self.started_at)
        raw_keys.validate_extraction_date(self.extraction_date)
        self._backend: RawStorageBackend = backend or LocalFilesystemBackend(root_dir)
        self._objects: dict[str, dict[str, int]] = {}
        self._failures: list[dict[str, Any]] = []
        self._finalized = False

    @property
    def backend(self) -> RawStorageBackend:
        """The storage backend this run writes through.

        Exposed so stages that need to check whether something has already
        been captured (e.g. ``gameweeks._has_event_live_capture``) query the
        same backend the run is actually writing to, rather than assuming a
        local filesystem.
        """
        return self._backend

    # -- object writes ------------------------------------------------------

    def write_object(
        self,
        endpoint: str,
        payload_bytes: bytes,
        *,
        request_url: str,
        requested_at: datetime,
        received_at: datetime,
        http_status: int,
        response_headers: Mapping[str, str] | None = None,
        attempt_count: int = 1,
        extension: str = "json",
        shape_validation: Mapping[str, Any] | None = None,
        companions: Mapping[str, bytes] | None = None,
    ) -> WriteResult:
        """Write one captured payload plus its metadata sidecar.

        Args:
            endpoint: Endpoint identity, possibly multi-segment
                (``element-summary/115``, ``event-live/01``).
            payload_bytes: The response body exactly as received.
            request_url: Full request URL including any query string.
            requested_at: When the (final) request was issued.
            received_at: When the response body was received.
            http_status: Response status. Only 2xx should be written.
            response_headers: Response headers; keys are lowercased and kept
                whole, so ``date``, ``age``, ``cache-control`` and ``etag``
                survive when the source sent them.
            attempt_count: Number of attempts made. Anything but 1 means the
                retry path was exercised.
            extension: Payload extension — ``json``, or ``csv`` for reep.
            shape_validation: Structural validation result from the caller.
            companions: Extra files written into the same directory, keyed by
                filename. Used for the Understat ``source.html`` case (A.6);
                ``payload.*`` and ``metadata.json`` are reserved.

        Returns:
            WriteResult describing where the payload landed and its checksum.

        Raises:
            RawObjectExistsError: The payload or sidecar key already exists.
            RawKeyError: The endpoint or extension is not key-safe.
            ValueError: A companion filename collides with a reserved name.
        """
        self._assert_open()
        p_key = payload_key(self.source, endpoint, self.extraction_date, self.run_id, extension)
        m_key = metadata_key(self.source, endpoint, self.extraction_date, self.run_id)
        prefix = raw_keys.object_prefix(self.source, endpoint, self.extraction_date, self.run_id)

        companion_bytes = dict(companions or {})
        self._validate_companions(companion_bytes, extension)

        digest = hashlib.sha256(payload_bytes).hexdigest()
        payload_location = self._backend.put_bytes(p_key, payload_bytes)

        companion_keys: dict[str, str] = {}
        for filename, blob in companion_bytes.items():
            c_key = f"{prefix}/{filename}"
            self._backend.put_bytes(c_key, blob)
            companion_keys[filename] = c_key

        sidecar = self._build_sidecar(
            endpoint=endpoint,
            request_url=request_url,
            requested_at=requested_at,
            received_at=received_at,
            http_status=http_status,
            response_headers=response_headers,
            content_length=len(payload_bytes),
            content_sha256=digest,
            attempt_count=attempt_count,
            shape_validation=shape_validation,
            payload_filename=payload_filename(extension),
            companion_files=sorted(companion_keys),
        )
        self._backend.put_bytes(m_key, _json_bytes(sidecar))

        self._record_object(endpoint, written=1, byte_count=len(payload_bytes))
        self._flush_manifest(MANIFEST_STATUS_IN_PROGRESS)

        return WriteResult(
            source=self.source,
            endpoint=endpoint,
            run_id=self.run_id,
            extraction_date=self.extraction_date,
            payload_key=p_key,
            metadata_key=m_key,
            payload_location=payload_location,
            content_length=len(payload_bytes),
            content_sha256=digest,
            companion_keys=companion_keys,
        )

    def record_failure(
        self,
        endpoint: str,
        *,
        request_url: str,
        error_class: str,
        http_status: int | None = None,
        attempt_count: int = 1,
        message: str | None = None,
    ) -> None:
        """Record an endpoint whose capture failed, with no payload written.

        A failed capture is still an attempt: it counts toward the manifest's
        per-endpoint tallies and appears in ``failures`` so a consumer can see
        exactly what is missing from an incomplete run.
        """
        self._assert_open()
        raw_keys.validate_endpoint(endpoint)
        self._record_object(endpoint, failed=1)
        self._failures.append(
            {
                "endpoint": endpoint,
                "request_url": request_url,
                "http_status": http_status,
                "attempt_count": attempt_count,
                "error_class": error_class,
                "message": message,
            }
        )
        self._flush_manifest(MANIFEST_STATUS_IN_PROGRESS)

    # -- manifest -----------------------------------------------------------

    def finalize(
        self,
        status: RunStatus,
        *,
        git_sha: str | None = None,
        ingest_version: str | None = None,
        config: Mapping[str, Any] | None = None,
        finality: Any | None = None,
        ended_at: datetime | None = None,
    ) -> ManifestResult:
        """Write the terminal manifest for this run.

        Args:
            status: Terminal status from ``orchestration.run_status`` —
                SUCCESS, FAILED_PARTIAL, or FAILED.
            git_sha: Commit that produced the run.
            ingest_version: Producing package version.
            config: Effective run configuration (rate, concurrency, strict,
                force) — whatever the caller has.
            finality: Optional source-specific finality block. For FPL this is
                the ``event-status`` essentials; this writer neither builds nor
                interprets it.
            ended_at: Run end instant; defaults to now (UTC).

        Returns:
            ManifestResult carrying the manifest key and its content.
        """
        self._assert_open()
        manifest = self._build_manifest(
            status=status,
            git_sha=git_sha,
            ingest_version=ingest_version,
            config=config,
            finality=finality,
            ended_at=ended_at or datetime.now(timezone.utc),
        )
        key = manifest_key(self.source, self.extraction_date, self.run_id)
        location = self._backend.put_bytes(key, _json_bytes(manifest), overwrite=True)
        self._finalized = True
        return ManifestResult(
            run_id=self.run_id,
            source=self.source,
            extraction_date=self.extraction_date,
            manifest_key=key,
            manifest_location=location,
            status=status,
            manifest=manifest,
        )

    @property
    def manifest_snapshot(self) -> dict[str, Any]:
        """Return the manifest as it currently stands, without writing it."""
        return self._build_manifest(
            status=MANIFEST_STATUS_IN_PROGRESS,
            git_sha=None,
            ingest_version=None,
            config=None,
            finality=None,
            ended_at=None,
        )

    # -- internals ----------------------------------------------------------

    def _build_sidecar(
        self,
        *,
        endpoint: str,
        request_url: str,
        requested_at: datetime,
        received_at: datetime,
        http_status: int,
        response_headers: Mapping[str, str] | None,
        content_length: int,
        content_sha256: str,
        attempt_count: int,
        shape_validation: Mapping[str, Any] | None,
        payload_filename: str,
        companion_files: list[str],
    ) -> dict[str, Any]:
        return {
            "raw_contract_version": RAW_CONTRACT_VERSION,
            "source": self.source,
            "endpoint": endpoint,
            "run_id": self.run_id,
            "extraction_date": self.extraction_date,
            "request_url": request_url,
            "requested_at": iso_utc(requested_at),
            "received_at": iso_utc(received_at),
            "http_status": http_status,
            "response_headers": _normalize_headers(response_headers),
            "content_length": content_length,
            "content_sha256": content_sha256,
            "attempt_count": attempt_count,
            "payload_filename": payload_filename,
            "companion_files": companion_files,
            "shape_validation": dict(shape_validation) if shape_validation is not None else None,
        }

    def _build_manifest(
        self,
        *,
        status: str,
        git_sha: str | None,
        ingest_version: str | None,
        config: Mapping[str, Any] | None,
        finality: Any | None,
        ended_at: datetime | None,
    ) -> dict[str, Any]:
        manifest: dict[str, Any] = {
            "raw_contract_version": RAW_CONTRACT_VERSION,
            "run_id": self.run_id,
            "source": self.source,
            "extraction_date": self.extraction_date,
            "started_at": iso_utc(self.started_at),
            "ended_at": iso_utc(ended_at) if ended_at is not None else None,
            "duration_seconds": (
                round((ended_at - self.started_at).total_seconds(), 3)
                if ended_at is not None
                else None
            ),
            "status": status,
            "objects": {
                endpoint: dict(counts) for endpoint, counts in sorted(self._objects.items())
            },
            "totals": self._totals(),
            "failures": list(self._failures),
            "git_sha": git_sha,
            "ingest_version": ingest_version,
            "config": dict(config) if config is not None else None,
        }
        if finality is not None:
            manifest["finality"] = finality
        return manifest

    def _totals(self) -> dict[str, int]:
        totals = {"attempted": 0, "written": 0, "failed": 0, "bytes": 0}
        for counts in self._objects.values():
            for name in totals:
                totals[name] += counts[name]
        return totals

    def _record_object(
        self, endpoint: str, *, written: int = 0, failed: int = 0, byte_count: int = 0
    ) -> None:
        counts = self._objects.setdefault(
            endpoint, {"attempted": 0, "written": 0, "failed": 0, "bytes": 0}
        )
        counts["attempted"] += written + failed
        counts["written"] += written
        counts["failed"] += failed
        counts["bytes"] += byte_count

    def _flush_manifest(self, status: str) -> None:
        """Persist the in-progress manifest so a dead run leaves an honest trace."""
        manifest = self._build_manifest(
            status=status,
            git_sha=None,
            ingest_version=None,
            config=None,
            finality=None,
            ended_at=None,
        )
        key = manifest_key(self.source, self.extraction_date, self.run_id)
        self._backend.put_bytes(key, _json_bytes(manifest), overwrite=True)

    def _validate_companions(self, companions: Mapping[str, bytes], extension: str) -> None:
        reserved = {raw_keys.METADATA_FILENAME, payload_filename(extension)}
        for filename in companions:
            if filename in reserved:
                raise ValueError(f"companion filename {filename!r} is reserved")
            if "/" in filename or filename in {".", ".."}:
                raise ValueError(f"invalid companion filename: {filename!r}")

    def _assert_open(self) -> None:
        if self._finalized:
            raise RuntimeError(f"run {self.run_id} has already been finalized")


def _normalize_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    """Lowercase header names and keep every header the source returned."""
    if not headers:
        return {}
    return {str(name).lower(): str(value) for name, value in headers.items()}


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    """Serialise writer-authored metadata. Never applied to captured payloads."""
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False).encode("utf-8") + b"\n"
