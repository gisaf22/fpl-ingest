"""The bounded dispatch loop (#115 AC6)."""

from __future__ import annotations

import asyncio

import pytest

from fpl_ingest.extract.http.dispatch import dispatch_bounded


def _run(coro):
    return asyncio.run(coro)


@pytest.mark.covers("#115 AC6")
def test_while_not_aborted_every_item_is_dispatched_and_its_result_returned():
    async def work(item):
        return item * 2

    results = _run(dispatch_bounded(list(range(25)), work, aborted=lambda: False, limit=4))

    assert sorted(results) == [(i, i * 2) for i in range(25)]


@pytest.mark.covers("#115 AC6")
def test_concurrency_never_exceeds_the_limit_and_reaches_it():
    running = peak = 0

    async def work(_item):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        running -= 1

    _run(dispatch_bounded(list(range(30)), work, aborted=lambda: False, limit=5))

    assert peak == 5


@pytest.mark.covers("#115 AC6")
def test_nothing_is_dispatched_after_the_abort_flips_and_in_flight_work_finishes():
    started: list[int] = []
    finished: list[int] = []
    flipped = False

    async def work(item):
        nonlocal flipped
        started.append(item)
        await asyncio.sleep(0)
        if len(started) >= 3:
            flipped = True
        await asyncio.sleep(0)
        finished.append(item)

    results = _run(dispatch_bounded(list(range(50)), work, aborted=lambda: flipped, limit=2))

    assert len(started) < 50
    assert sorted(finished) == sorted(started), "every dispatched item finished"
    assert sorted(i for i, _ in results) == sorted(started)
    # nothing started once the flag was observed true
    assert started == sorted(started)


@pytest.mark.covers("#115 AC6")
def test_an_already_aborted_run_dispatches_nothing():
    async def work(_item):
        raise AssertionError("dispatched")

    assert _run(dispatch_bounded([1, 2, 3], work, aborted=lambda: True, limit=2)) == []
