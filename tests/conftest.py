"""Shared pytest support for the fpl-ingest test suite.

The async runner below was previously local to ``tests/extract/stages``; it
lives here now so every tier can run ``@pytest.mark.asyncio`` tests without a
third-party asyncio plugin.
"""

from __future__ import annotations

import asyncio
import inspect


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
