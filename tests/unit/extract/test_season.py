"""Season derivation from the same run's bootstrap-static (#62 D5, D6).

Season is ``Y-(Y+1 mod 100)`` where Y is the deadline year of the lowest-id
event in a usable bootstrap. There is no fallback: without a usable bootstrap
the season is None and the run logs ``season_source=none`` at ERROR.
"""

from __future__ import annotations

import logging

import pytest

from fpl_ingest.extract.season import resolve_season

_LOGGER = "fpl_ingest.test.season"


def _bootstrap(*events: tuple[int, str | None]) -> dict:
    rows = []
    for event_id, deadline in events:
        row = {"id": event_id, "finished": False, "is_current": False}
        if deadline is not None:
            row["deadline_time"] = deadline
        rows.append(row)
    return {"events": rows, "elements": [], "teams": [], "element_types": []}


def _source_records(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if "season_source=" in r.getMessage()]


@pytest.mark.covers("#62 AC3")
@pytest.mark.parametrize(
    ("events", "expected"),
    [
        pytest.param([(1, "2026-08-15T17:30:00Z")], "2026-27", id="first-deadline-2026"),
        pytest.param([(1, "2099-08-15T17:30:00Z")], "2099-00", id="century-wraps-to-00"),
        pytest.param(
            [(2, "2027-01-02T11:00:00Z"), (1, "2026-08-15T17:30:00Z")],
            "2026-27",
            id="lowest-id-event-not-list-order",
        ),
    ],
)
def test_season_from_bootstrap_deadline(caplog, events, expected):
    caplog.set_level(logging.INFO, logger=_LOGGER)

    season = resolve_season(_bootstrap(*events), shape_ok=True, logger=logging.getLogger(_LOGGER))

    assert season == expected
    records = _source_records(caplog)
    assert len(records) == 1
    assert "season_source=bootstrap" in records[0].getMessage()
    assert records[0].levelno < logging.ERROR


@pytest.mark.covers("#62 AC3")
def test_season_preseason_bootstrap_keeps_old_season(caplog):
    """A June/July bootstrap still lists last season's events, so it is last season."""
    caplog.set_level(logging.INFO, logger=_LOGGER)
    june_payload = _bootstrap(
        (1, "2025-08-15T17:30:00Z"), (2, "2025-08-22T17:30:00Z"), (38, "2026-05-24T13:30:00Z"),
    )

    season = resolve_season(june_payload, shape_ok=True, logger=logging.getLogger(_LOGGER))

    assert season == "2025-26"


@pytest.mark.covers("#62 AC3")
@pytest.mark.parametrize(
    ("payload", "shape_ok"),
    [
        pytest.param(None, False, id="bootstrap-fetch-failed"),
        pytest.param(_bootstrap((1, "2026-08-15T17:30:00Z")), False, id="shape-invalid"),
        pytest.param(_bootstrap(), True, id="empty-events-at-july-reset"),
        pytest.param(_bootstrap((1, None)), True, id="no-deadline"),
        pytest.param(_bootstrap((1, "not a date")), True, id="unparseable-deadline"),
        pytest.param({"elements": [], "teams": [], "element_types": []}, True, id="no-events-key"),
    ],
)
def test_season_null_when_no_usable_bootstrap(caplog, payload, shape_ok):
    caplog.set_level(logging.INFO, logger=_LOGGER)

    season = resolve_season(payload, shape_ok=shape_ok, logger=logging.getLogger(_LOGGER))

    assert season is None
    records = _source_records(caplog)
    assert len(records) == 1
    assert "season_source=none" in records[0].getMessage()
    assert records[0].levelno == logging.ERROR
