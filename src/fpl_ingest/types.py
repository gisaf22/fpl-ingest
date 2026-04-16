"""Shared type aliases for JSON-like data used throughout fpl-ingest.

This module only defines type aliases. It does not contain logic,
validation, or data transformation of any kind.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

# Represents any value that can appear in decoded JSON.
JSON = Union[Dict[str, Any], List[Any], str, int, float, bool, None]
