"""Upstream API structural drift check (smoke test).

The schema-contract compiler, DDL generator, and test-fixture generator that
used to live alongside this were retired with the SQLite writer (strategy
doc B.3) — ``PUBLIC_TABLES`` went to zero tables once ``player_histories``,
the last one, moved to raw capture. What remains is the source-shape smoke
test, re-exported here for convenience.
"""

from fpl_ingest.schema.validation import (
    DEFAULT_SAMPLE_SIZE,
    SmokeTestFailure,
    SmokeTestResult,
    run_smoke_test,
    run_smoke_test_async,
)

__all__ = [
    "DEFAULT_SAMPLE_SIZE",
    "SmokeTestFailure",
    "SmokeTestResult",
    "run_smoke_test",
    "run_smoke_test_async",
]
