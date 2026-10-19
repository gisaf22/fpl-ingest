"""Shared response classifier and router (#54).

Classifies a fetched response from its HTTP status and its stage's shape
verdict, then routes the write: a valid response to ``payload.json``, an
invalid one to ``fpl/_invalid/{class}/...``, and a failed fetch nowhere.
The per-stage shape validators stay where they are and only supply the verdict.

Stub: signatures only, so the #54 tests can import it. Not yet implemented.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

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


def route_response(
    raw_writer: LocalRawWriter,
    endpoint: str,
    raw: RawResponse | None,
    shape: Mapping[str, Any] | None,
    *,
    absence_ok: bool = False,
) -> RoutedResponse:
    """Classify ``raw`` and write it to the location its class calls for.

    Args:
        raw_writer: Writer for this run.
        endpoint: Endpoint identity, as for ``write_object``.
        raw: The response, or None when none arrived.
        shape: The stage validator's verdict for ``raw``; None when ``raw`` is None.
        absence_ok: The caller declares a 404 legitimate for this request.
    """
    raise NotImplementedError("#54: response classifier and router")
