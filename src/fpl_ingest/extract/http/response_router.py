"""Shared response classifier and router (#54).

Classifies a fetched response from its HTTP status and its stage's shape
verdict, then routes the write: a valid response to ``payload.json``, an
invalid one to ``fpl/_invalid/{class}/...``, and a failed fetch nowhere. The
per-stage shape validators stay where they are and only supply the verdict.

Raw stores what it is given; deciding what a stored response means is
modelling's job (docs/adr/004-failure-classification.md).
"""

from __future__ import annotations

import logging

from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Mapping

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.http.local_writer import LocalRawWriter, WriteResult
from fpl_ingest.extract.http.sync_http import FPLClientError

logger = logging.getLogger(__name__)


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
    shape: Mapping[str, Any] | None = None


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
        expected: Outcomes the caller declares legitimate for this request:
            ``not_found`` for a declared 404, or the ``outcome`` a stage's shape
            verdict names for a ``shape_invalid`` (the panel's ``no_picks``).
            A declared invalid capture is stored and counted as usual but does
            not mark its endpoint failed (ADR 004). Anything undeclared still does.
    """
    shape_ok = bool(shape and shape.get("ok"))
    response_class = classify_response(raw, shape_ok, absence_ok=absence_ok)
    raw_writer.observe_response(response_class.value, raw.status if raw is not None else 0)
    if response_class is ResponseClass.FETCH_FAILED:
        return RoutedResponse(response_class, None, shape)

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
        return RoutedResponse(response_class, raw_writer.write_object(endpoint, raw.body, **fields), shape)
    write = raw_writer.write_invalid(
        endpoint, raw.body, response_class=response_class.value,
        reason=_reason(response_class, raw.status, shape),
        expected=_outcome(response_class, shape) in expected, **fields,
    )
    return RoutedResponse(response_class, write, shape)


def route_and_record(
    raw_writer: LocalRawWriter,
    endpoint: str,
    raw: RawResponse,
    shape: Mapping[str, Any],
    *,
    label: str,
    absence_ok: bool = False,
    expected: frozenset[str] = frozenset(),
) -> RoutedResponse:
    """Route a response and record the one outcome ``route_response`` leaves unrecorded (#115).

    A 429 or 5xx that outlived the client's retries stores nothing, so its
    manifest failure and log line are written here, once, for every stage.
    ``label`` names the request in that log line ("gameweek 5"). The caller
    keeps its own ``StageResult`` accounting and its log line for the other
    classes.
    """
    routed = route_response(raw_writer, endpoint, raw, shape, absence_ok=absence_ok, expected=expected)
    if routed.response_class is ResponseClass.FETCH_FAILED:
        logger.error("Failed to fetch %s: HTTP %d", label, raw.status)
        record_fetch_failed(raw_writer, endpoint, raw)
    return routed


#: Runs ``fn(*args, **kwargs)`` and returns its result; a stage passes one that keeps blocking I/O off the loop.
BlockingRunner = Callable[..., Awaitable[Any]]


async def _run_inline(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return fn(*args, **kwargs)


async def fetch_and_route(
    raw_writer: LocalRawWriter,
    endpoint: str,
    fetch: Callable[[], Awaitable[RawResponse]],
    validate: Callable[[RawResponse], Mapping[str, Any]],
    *,
    request_url: str,
    label: str | None = None,
    absence_ok: bool = False,
    expected: frozenset[str] = frozenset(),
    run_blocking: BlockingRunner | None = None,
) -> RoutedResponse:
    """Fetch, classify, store and record one response: the tail every stage shares (#115).

    Covers all three ways a request ends: a response (routed by class), a 429 or
    5xx that outlived the client's retries, and no response at all (recorded
    under the exception's class). Both failures come back as ``fetch_failed``.

    Args:
        fetch: Performs the request.
        validate: The stage's shape check for the response.
        request_url: Recorded on a no-response failure, when there is no response to read it from.
        label: Names the request in log lines; defaults to ``endpoint``.
        absence_ok, expected: As for ``route_response``.
        run_blocking: Runs the blocking storage work (the writer's backend calls)
            off the event loop; None runs it inline. The caller owns serialising
            the writer if it passes one (the writer is not thread-safe).
    """
    label = label or endpoint
    run = run_blocking or _run_inline
    try:
        raw = await fetch()
    except FPLClientError as exc:
        await run(record_no_response, raw_writer, endpoint, request_url=request_url, exc=exc, label=label)
        return RoutedResponse(ResponseClass.FETCH_FAILED, None)
    return await run(
        route_and_record,
        raw_writer, endpoint, raw, validate(raw), label=label, absence_ok=absence_ok, expected=expected,
    )


def record_no_response(
    raw_writer: LocalRawWriter, endpoint: str, *, request_url: str, exc: BaseException, label: str | None = None
) -> None:
    """Record the manifest failure for a request that got no response at all."""
    if label is not None:
        logger.error("Failed to fetch %s: %s: %s", label, type(exc).__name__, exc)
    raw_writer.record_failure(
        endpoint, request_url=request_url, error_class=type(exc).__name__, message=str(exc)
    )


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


def _outcome(response_class: ResponseClass, shape: Mapping[str, Any] | None) -> str | None:
    """Name the outcome a caller can declare expected, or None when none applies."""
    if response_class is ResponseClass.NOT_FOUND:
        return "not_found"
    if response_class is ResponseClass.SHAPE_INVALID:
        return (shape or {}).get("outcome")
    return None


def _reason(response_class: ResponseClass, status: int, shape: Mapping[str, Any] | None) -> str:
    if response_class is ResponseClass.SHAPE_INVALID:
        failures = (shape or {}).get("failures") or []
        return "; ".join(str(f) for f in failures) or "shape check failed"
    if response_class is ResponseClass.NOT_FOUND:
        return "HTTP 404; absence declared legitimate for this request"
    return f"HTTP {status}"
