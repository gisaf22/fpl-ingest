"""Synchronous FPL API HTTP client (sync_client.py).

Kept for backwards compatibility with callers that cannot use async.
The pipeline itself uses AsyncFPLClient. This client wraps the same
sync HTTP layer (requests session, retry logic, pacing gate) and
exposes the same FPL endpoint methods.

This module is HTTP-only — it has no knowledge of FPL domain models
or pipeline stages.

Usage:
    from fpl_ingest import FPLClient

    client = FPLClient()
    bootstrap = client.get_bootstrap()
    fixtures = client.get_fixtures()
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

import requests
from requests.adapters import HTTPAdapter

from fpl_ingest.sync_http import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_PLAYER_HISTORY_REQUEST_DELAY,
    DEFAULT_REQUEST_DELAY,
    DEFAULT_TIMEOUT,
    FPLClientError,
    RequestGate,
    execute_json_request,
)
from fpl_ingest.types import JSON

logger = logging.getLogger(__name__)

FPL_BASE_URL = "https://fantasy.premierleague.com/api"

ENDPOINTS = {
    "bootstrap": f"{FPL_BASE_URL}/bootstrap-static/",
    "fixtures": f"{FPL_BASE_URL}/fixtures/",
    "live": f"{FPL_BASE_URL}/event/{{gw}}/live/",
    "player": f"{FPL_BASE_URL}/element-summary/{{player_id}}/",
}


class FPLClient:
    """Synchronous HTTP client for the FPL API.

    Only the bootstrap-static response is cached in memory for the lifetime
    of the client instance. All other endpoints fetch fresh on every call.
    """

    def __init__(
        self,
        request_delay: float = DEFAULT_REQUEST_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: float = DEFAULT_TIMEOUT,
        request_gate: RequestGate | None = None,
        pool_size: int = 50,
    ):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "fpl-ingest/1.0.0 (github.com/gisaf22/fpl-ingest)"
        })
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=pool_size)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self._pool_size = pool_size
        self._bootstrap_cache: Optional[Dict[str, Any]] = None
        self._request_delay = request_delay
        self._player_history_request_delay = DEFAULT_PLAYER_HISTORY_REQUEST_DELAY
        self._max_retries = max_retries
        self._timeout = timeout
        self._request_gate = request_gate or RequestGate()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()

    def _get(self, url: str, *, request_delay: float | None = None) -> JSON | None:
        return execute_json_request(
            self.session,
            url,
            timeout=self._timeout,
            request_delay=request_delay if request_delay is not None else self._request_delay,
            max_retries=self._max_retries,
            request_gate=self._request_gate,
        )

    def get_bootstrap(self, force: bool = False) -> Dict[str, Any]:
        """Fetch bootstrap-static data, caching the result for the client lifetime.

        Args:
            force: If True, bypass cache and fetch fresh data.

        Returns:
            Bootstrap data dict.

        Raises:
            FPLClientError: If bootstrap data cannot be fetched.
        """
        if self._bootstrap_cache is None or force:
            logger.info("Fetching bootstrap-static data...")
            self._bootstrap_cache = cast(Optional[Dict[str, Any]], self._get(ENDPOINTS["bootstrap"]))

        if self._bootstrap_cache is None:
            raise FPLClientError("Failed to fetch bootstrap data from FPL API")

        return self._bootstrap_cache

    def get_current_gw(self) -> int:
        """Return the current gameweek, or the latest finished one if none is current.

        Returns:
            Gameweek number.

        Raises:
            RuntimeError: If no gameweek data is found in bootstrap.
        """
        logger.info("Getting current gameweek...")
        bootstrap = self.get_bootstrap()
        events = bootstrap.get("events", [])

        for event in events:
            if event.get("is_current"):
                return event["id"]

        for event in events:
            if event.get("is_next"):
                return event["id"] - 1

        finished = [e for e in events if e.get("finished")]
        if finished:
            return max(e["id"] for e in finished)

        raise RuntimeError("No gameweek data found in bootstrap")

    def get_gw_deadline(self, gameweek: int) -> Optional[datetime]:
        """Return the deadline datetime for a specific gameweek.

        Args:
            gameweek: Gameweek number.

        Returns:
            Deadline as a timezone-aware datetime, or None if not found.
        """
        logger.info("Getting gameweek %d deadline...", gameweek)
        bootstrap = self.get_bootstrap()
        for event in bootstrap.get("events", []):
            if event["id"] == gameweek:
                deadline_str = event.get("deadline_time")
                if deadline_str:
                    return datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
        return None

    def get_gw(self, gameweek: int) -> Optional[Dict[str, Any]]:
        """Fetch live player stats for a gameweek.

        Args:
            gameweek: Gameweek number.

        Returns:
            Dict with an 'elements' list, or None on failure.
        """
        url = ENDPOINTS["live"].format(gw=gameweek)
        logger.info("Fetching gameweek %d data...", gameweek)
        return cast(Optional[Dict[str, Any]], self._get(url))

    def get_fixtures(self) -> Optional[List[Any]]:
        """Fetch all fixtures for the current season.

        Returns:
            List of fixture dicts, or None on failure.
        """
        logger.info("Fetching fixtures...")
        return cast(Optional[List[Any]], self._get(ENDPOINTS["fixtures"]))

    def get_player_history(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Fetch element-summary history for a player.

        Args:
            player_id: FPL element ID.

        Returns:
            Dict with 'history' and 'history_past' lists, or None on failure.
        """
        logger.info("Fetching player %d history...", player_id)
        url = ENDPOINTS["player"].format(player_id=player_id)
        return cast(Optional[Dict[str, Any]], self._get(url, request_delay=self._player_history_request_delay))
