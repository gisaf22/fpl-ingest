"""Tier marker for ``tests/e2e``.

Every test collected under this directory is a e2e test by virtue of
living here, so the marker is applied automatically. This removes the class of
mistake where a test silently carries no tier marker.

pytest calls this hook with the *whole* collected item list, not just this
directory's, so items are filtered by path before being marked.
"""

from __future__ import annotations

import pathlib

import pytest

_TIER_DIR = pathlib.Path(__file__).parent


def pytest_collection_modifyitems(config, items):
    for item in items:
        path = pathlib.Path(str(getattr(item, "fspath", "")))
        if _TIER_DIR in path.parents:
            item.add_marker(pytest.mark.e2e)
