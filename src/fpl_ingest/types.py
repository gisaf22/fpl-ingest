"""Common typing aliases for JSON-like data.

Provide a single `JSON` alias to represent decoded JSON values used
throughout the codebase.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

JSON = Union[Dict[str, Any], List[Any], str, int, float, bool, None]
