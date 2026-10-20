"""Shared response classifier and router (#54).

Classifies a fetched response from its HTTP status and its stage's shape
verdict, then routes the write: a valid response to ``payload.json``, an
invalid one to ``fpl/_invalid/{class}/...``, and a failed fetch nowhere. The
per-stage shape validators stay where they are and only supply the verdict.

Raw stores what it is given; deciding what a stored response means is
modelling's job (docs/adr/004-failure-classification.md).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Mapping

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter, WriteResult


class ResponseClass(str, Enum):
    """What a fetched response turned out to be."""

    VALID = "valid"
    SHAPE_INVALID = "shape_invalid"
    NOT_FOUND = "not_found"
    HTTP_ERROR = "http_error"
    FETCH_FAILED = "fetch_failed"


@dataclass(frozen=True)
class RoutedResponse:
    """The class a response was given and, when something was stored, where."""

    response_class: ResponseClass
    write: WriteResult | None


def classify_response(
    raw: RawResponse | None, shape_ok: bool, *, absence_ok: bool = False
) -> ResponseClass:
    """Return the class of a response.

    A 429 or 5xx here means the client's retries are spent: the server never
    gave a usable answer, so nothing is stored and the request is retried next
    run. Any other non-2xx is the server refusing us: ``not_found`` for a 404
    the caller declared legitimate, ``http_error`` otherwise.
    """
    if raw is None or raw.status == 429 or raw.status >= 500:
        return ResponseClass.FETCH_FAILED
    if not 200 <= raw.status < 300:
        if raw.status == 404 and absence_ok:
            return ResponseClass.NOT_FOUND
        return ResponseClass.HTTP_ERROR
    return ResponseClass.VALID if shape_ok else ResponseClass.SHAPE_INVALID


def route_response(
    raw_writer: LocalRawWriter,
    endpoint: str,
    raw: RawResponse | None,
    shape: Mapping[str, Any] | None,
    *,
    absence_ok: bool = False,
    expected: frozenset[str] = frozenset(),
) -> RoutedResponse:
    """Classify ``raw`` and write it to the location its class calls for.

    A failed fetch writes nothing and records nothing: the caller holds the
    error to record, if there was one.

    Args:
        raw_writer: Writer for this run.
        endpoint: Endpoint identity, as for ``write_object``.
        raw: The response, or None when none arrived.
        shape: The stage validator's verdict for ``raw``; None when ``raw`` is None.
        absence_ok: The caller declares a 404 legitimate for this request.
            Per call, not per endpoint: entry-picks passes True for a
            random-cohort entry and False for the self cohort.
    """
    shape_ok = bool(shape and shape.get("ok"))
    response_class = classify_response(raw, shape_ok, absence_ok=absence_ok)
    raw_writer.observe_response(response_class.value, raw.status if raw is not None else 0)
    if response_class is ResponseClass.FETCH_FAILED:
        return RoutedResponse(response_class, None)

    assert raw is not None  # every other class has a response
    fields: dict[str, Any] = {
        "request_url": raw.url,
        "requested_at": raw.requested_at,
        "received_at": raw.received_at,
        "http_status": raw.status,
        "response_headers": raw.headers,
        "attempt_count": raw.attempt_count,
        "shape_validation": shape,
    }
    if response_class is ResponseClass.VALID:
        return RoutedResponse(response_class, raw_writer.write_object(endpoint, raw.body, **fields))
    write = raw_writer.write_invalid(
        endpoint, raw.body, response_class=response_class.value,
        reason=_reason(response_class, raw.status, shape), **fields,
    )
    return RoutedResponse(response_class, write)


async def fetch_and_route(
    raw_writer: LocalRawWriter,
    endpoint: str,
    fetch: Callable[[], Awaitable[RawResponse]],
    validate: Callable[[RawResponse], Mapping[str, Any]],
    *,
    request_url: str,
    absence_ok: bool = False,
    expected: frozenset[str] = frozenset(),
) -> RoutedResponse:
    """Fetch, classify, store and record one response: the tail every stage shares (#115 stub)."""
    raise NotImplementedError("#115")


def record_fetch_failed(raw_writer: LocalRawWriter, endpoint: str, raw: RawResponse) -> None:
    """Record the manifest failure for a 429 or 5xx that outlived the client's retries.

    ``route_response`` stored nothing for it, so the failure is the only trace.
    """
    raw_writer.record_failure(
        endpoint,
        request_url=raw.url,
        error_class="FetchFailed",
        http_status=raw.status,
        attempt_count=raw.attempt_count,
        message=f"HTTP {raw.status}",
    )


def _reason(response_class: ResponseClass, status: int, shape: Mapping[str, Any] | None) -> str:
    if response_class is ResponseClass.SHAPE_INVALID:
        failures = (shape or {}).get("failures") or []
        return "; ".join(str(f) for f in failures) or "shape check failed"
    if response_class is ResponseClass.NOT_FOUND:
        return "HTTP 404; absence declared legitimate for this request"
    return f"HTTP {status}"
