"""Key layout for the raw-capture contract.

Implements the key template from ``docs/architecture/fpl-ingest-strategy.md``
section A.1/A.2/A.4::

    {root}/{source}/{endpoint}/{extraction_date}/{run_id}/payload.{ext}
    {root}/{source}/{endpoint}/{extraction_date}/{run_id}/metadata.json
    {root}/{source}/_manifests/{extraction_date}/{run_id}/manifest.json

The template is storage-agnostic: it is expressed as ``/``-joined relative key
strings so the same layout holds for a local root directory today and an S3
bucket prefix later. Nothing here touches the filesystem.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone

RAW_CONTRACT_VERSION = "1.0.0"

MANIFEST_PREFIX = "_manifests"
SETTLEMENT_PREFIX = "_settlement"
PAYLOAD_STEM = "payload"
METADATA_FILENAME = "metadata.json"
MANIFEST_FILENAME = "manifest.json"
SETTLEMENT_MARKER_FILENAME = "marker.json"

_RUN_ID_TIME_FORMAT = "%Y%m%dT%H%M%SZ"
_RUN_ID_RE = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{6}$")
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class RawKeyError(ValueError):
    """Raised when a source, endpoint, run id, or date is not key-safe."""


def new_run_id(started_at: datetime | None = None) -> str:
    """Return a fresh run id of the form ``20260824T080012Z-a3f19c``.

    The UTC timestamp prefix sorts chronologically as a plain string; the
    random suffix keeps two runs started in the same second from colliding
    under one prefix (strategy doc A.4). Contains no colons or dots.
    """
    started = _as_utc(started_at) if started_at is not None else datetime.now(timezone.utc)
    return f"{started.strftime(_RUN_ID_TIME_FORMAT)}-{uuid.uuid4().hex[:6]}"


def extraction_date_for(started_at: datetime) -> str:
    """Return the ``YYYY-MM-DD`` UTC extraction date for a run start instant.

    Derived once from the run start, never from a per-object write time, so a
    run spanning midnight lands entirely under one date.
    """
    return _as_utc(started_at).strftime("%Y-%m-%d")


def object_prefix(source: str, endpoint: str, extraction_date: str, run_id: str) -> str:
    """Return the relative key prefix holding one endpoint's captured object."""
    validate_source(source)
    validate_endpoint(endpoint)
    validate_extraction_date(extraction_date)
    validate_run_id(run_id)
    return f"{source}/{endpoint}/{extraction_date}/{run_id}"


def payload_key(
    source: str, endpoint: str, extraction_date: str, run_id: str, extension: str = "json"
) -> str:
    """Return the key for the verbatim response body."""
    return f"{object_prefix(source, endpoint, extraction_date, run_id)}/{payload_filename(extension)}"


def metadata_key(source: str, endpoint: str, extraction_date: str, run_id: str) -> str:
    """Return the key for an object's per-object metadata sidecar."""
    return f"{object_prefix(source, endpoint, extraction_date, run_id)}/{METADATA_FILENAME}"


def manifest_key(source: str, extraction_date: str, run_id: str) -> str:
    """Return the key for a run's manifest.

    ``_manifests`` is a sibling of the endpoint prefixes so a consumer scanning
    ``{source}/{endpoint}/**`` never reads a manifest as a payload.
    """
    validate_source(source)
    validate_extraction_date(extraction_date)
    validate_run_id(run_id)
    return f"{source}/{MANIFEST_PREFIX}/{extraction_date}/{run_id}/{MANIFEST_FILENAME}"


def settlement_marker_key(source: str, endpoint: str, event_id: int) -> str:
    """Return the key for one endpoint's settlement-refetch marker.

    ``_settlement`` is a reserved sibling of the endpoint prefixes, exactly as
    ``_manifests`` is: ``validate_endpoint`` rejects any segment that does not
    start with an alphanumeric, so no real endpoint can ever collide with it.

    The marker records that a stage has already completed its one forced
    re-fetch of ``endpoint`` for the gameweek that settled — the only piece of
    cross-run state this pipeline keeps, and it is deliberately expressed as
    an object whose mere existence is the whole signal, so the write-only
    ``RawStorageBackend`` surface (``put_bytes`` + ``exists_prefix``) is
    enough to maintain it.
    """
    validate_source(source)
    validate_endpoint(endpoint)
    if event_id < 0:
        raise RawKeyError(f"invalid event_id: {event_id!r}")
    return f"{source}/{SETTLEMENT_PREFIX}/{endpoint}/{event_id}/{SETTLEMENT_MARKER_FILENAME}"


def payload_filename(extension: str = "json") -> str:
    """Return ``payload.<ext>`` for a normalised extension."""
    ext = extension.lstrip(".")
    if not _SEGMENT_RE.match(ext) or "." in ext:
        raise RawKeyError(f"invalid payload extension: {extension!r}")
    return f"{PAYLOAD_STEM}.{ext}"


def validate_source(source: str) -> None:
    """Reject a source that is not a single key-safe segment."""
    if not _SEGMENT_RE.match(source):
        raise RawKeyError(f"invalid source segment: {source!r}")
    if source == MANIFEST_PREFIX:
        raise RawKeyError(f"source may not be the reserved {MANIFEST_PREFIX!r} prefix")


def validate_endpoint(endpoint: str) -> None:
    """Reject an endpoint that is not one or more key-safe ``/``-joined segments.

    Multi-segment endpoints are expected and intentional — ``event-live/01``
    and ``element-summary/115`` are endpoint identities, not extra path levels.
    """
    if not endpoint or endpoint.startswith("/") or endpoint.endswith("/"):
        raise RawKeyError(f"invalid endpoint: {endpoint!r}")
    parts = endpoint.split("/")
    if parts[0] == MANIFEST_PREFIX:
        raise RawKeyError(f"endpoint may not start with the reserved {MANIFEST_PREFIX!r} prefix")
    for part in parts:
        if not _SEGMENT_RE.match(part) or part == "..":
            raise RawKeyError(f"invalid endpoint segment {part!r} in {endpoint!r}")


def validate_run_id(run_id: str) -> None:
    """Reject a run id that does not match ``{utc_start}Z-{short_uuid}``."""
    if not _RUN_ID_RE.match(run_id):
        raise RawKeyError(f"invalid run_id: {run_id!r}")


def validate_extraction_date(extraction_date: str) -> None:
    """Reject an extraction date that is not ``YYYY-MM-DD``."""
    try:
        datetime.strptime(extraction_date, "%Y-%m-%d")
    except ValueError as exc:
        raise RawKeyError(f"invalid extraction_date: {extraction_date!r}") from exc


def iso_utc(moment: datetime) -> str:
    """Return an ISO-8601 UTC timestamp with a trailing ``Z``."""
    return _as_utc(moment).isoformat().replace("+00:00", "Z")


def _as_utc(moment: datetime) -> datetime:
    """Return ``moment`` in UTC, treating a naive datetime as already UTC."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)
