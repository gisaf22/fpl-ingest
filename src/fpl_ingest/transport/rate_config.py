"""Canonical rate-limiting configuration shared across the codebase."""

DEFAULT_RATE = 10.0
MAX_RATE = 10.0


def normalize_rate(rate: float) -> float:
    """Return the single canonical applied request rate."""
    return min(rate, MAX_RATE)
