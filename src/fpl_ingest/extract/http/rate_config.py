"""Canonical rate-limiting constants shared across HTTP extraction.

Defines ``DEFAULT_RATE`` and ``MAX_RATE`` (both 10 req/s) as the single source
of truth so the CLI, async client, and tests all clamp to the same safe ceiling.
``normalize_rate`` enforces the ceiling at the call site regardless of the
caller-supplied value.
"""

DEFAULT_RATE = 10.0
MAX_RATE = 10.0


def normalize_rate(rate: float) -> float:
    """Return the single canonical applied request rate."""
    return min(rate, MAX_RATE)
