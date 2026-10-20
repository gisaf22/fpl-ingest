"""Unit tests for the ``entry-picks`` shape validator (#116 AC15).

``validate_entry_picks_shape`` in ``fpl_ingest.extract.stages.entry_picks``
is currently a stub that always reports ``ok``. These tests pin the real
contract: a 200 with 15 picks is valid; a 200 with an empty or missing
``picks`` list, or a body that is a list instead of an object, is
``shape_invalid``. They are expected to FAIL against the stub.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from fpl_ingest.extract.http.client import RawResponse
from fpl_ingest.extract.stages.entry_picks import validate_entry_picks_shape

PICKS_15 = {"picks": [{"element": i, "position": i, "multiplier": 1} for i in range(1, 16)]}


def _raw(body: object) -> RawResponse:
    at = datetime(2026, 10, 19, 4, 30, tzinfo=timezone.utc)
    payload = json.dumps(body).encode() if not isinstance(body, bytes) else body
    return RawResponse(
        url="https://fantasy.premierleague.com/api/entry/x/event/y/picks/", status=200,
        headers={"content-type": "application/json"}, body=payload,
        requested_at=at, received_at=at, attempt_count=1,
    )


class TestValid:

    @pytest.mark.covers("#116 AC15")
    def test_a_200_with_15_picks_is_valid(self):
        shape = validate_entry_picks_shape(_raw(PICKS_15))

        assert shape["ok"] is True


class TestShapeInvalid:

    @pytest.mark.covers("#116 AC15")
    def test_an_empty_picks_list_is_shape_invalid(self):
        shape = validate_entry_picks_shape(_raw({"picks": []}))

        assert shape["ok"] is False
        assert shape.get("failures")

    @pytest.mark.covers("#116 AC15")
    def test_a_missing_picks_key_is_shape_invalid(self):
        shape = validate_entry_picks_shape(_raw({"chips": [], "entry_history": {}}))

        assert shape["ok"] is False
        assert shape.get("failures")

    @pytest.mark.covers("#116 AC15")
    def test_a_body_that_is_a_list_is_shape_invalid(self):
        shape = validate_entry_picks_shape(_raw([1, 2, 3]))

        assert shape["ok"] is False
        assert shape.get("failures")
