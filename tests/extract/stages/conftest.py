"""Local async test support for stage ingest tests."""

from __future__ import annotations

import asyncio
import inspect


def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: run async tests in an event loop")


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
