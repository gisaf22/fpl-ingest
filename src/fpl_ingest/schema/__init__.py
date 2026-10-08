"""Payload shape baselines and drift detection.

The schema-contract compiler, DDL generator, and test-fixture generator that
used to live here were retired with the SQLite writer (strategy doc B.3) —
``PUBLIC_TABLES`` went to zero tables once ``player_histories``, the last one,
moved to raw capture. The ``smoke-test`` source-shape check was retired in #84,
superseded by the payload drift check (``payload_baseline``, ``payload_drift``).
"""
