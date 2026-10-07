"""Shared pytest support for the fpl-ingest test suite.

The async runner below was previously local to ``tests/extract/stages``; it
lives here now so every tier can run ``@pytest.mark.asyncio`` tests without a
third-party asyncio plugin.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest


def pytest_pyfunc_call(pyfuncitem):
    if pyfuncitem.get_closest_marker("asyncio") is None:
        return None
    testfunction = pyfuncitem.obj
    if not inspect.iscoroutinefunction(testfunction):
        return None
    kwargs = {
        name: pyfuncitem.funcargs[name]
        for name in pyfuncitem._fixtureinfo.argnames
    }
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(testfunction(**kwargs))
    finally:
        loop.close()
    return True


@pytest.fixture(autouse=True)
def _no_real_sts(monkeypatch):
    """Keep every tier off real AWS: the runner's origin lookup (#75) would
    otherwise call STS with whatever credentials the machine has. Tests that
    exercise the lookup inject their own client."""

    def _unavailable():
        raise RuntimeError("real STS is disabled in tests")

    monkeypatch.setattr("fpl_ingest.orchestration.origin._default_sts_client", _unavailable)


@pytest.fixture(autouse=True)
def _no_committed_baselines(monkeypatch):
    """Keep runs off the committed payload baselines (#82): with no baseline
    directory the writer skips the drift check and writes no ``drift`` block.
    The drift tests supply their own baselines through ``FPL_BASELINE_DIR``."""
    monkeypatch.setattr("fpl_ingest.config._DEFAULT_BASELINE_DIR", None)
    monkeypatch.delenv("FPL_BASELINE_DIR", raising=False)
