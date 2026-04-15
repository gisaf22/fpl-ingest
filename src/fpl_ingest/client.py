"""FPL API HTTP client with rate limiting and retry logic.

Handles all communication with the official Fantasy Premier League API.

Usage:
    from fpl_ingest import FPLClient

    client = FPLClient()
    bootstrap = client.get_bootstrap()
    fixtures = client.get_fixtures()
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter

from fpl_ingest.transport import (
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
    """HTTP client for the FPL API with rate limiting and bootstrap caching.

    Only the bootstrap-static response is cached in memory for the lifetime of
    the client instance. All other endpoints fetch fresh on every call.
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
        self._bootstrap_cache: Optional[JSON] = None
        self._request_delay = request_delay
        self._player_history_request_delay = DEFAULT_PLAYER_HISTORY_REQUEST_DELAY
        self._max_retries = max_retries
        self._timeout = timeout
        self._request_gate = request_gate or RequestGate()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()

    def clone(self) -> FPLClient:
        """Create a new client with the same transport settings."""
        return FPLClient(
            request_delay=self._request_delay,
            max_retries=self._max_retries,
            timeout=self._timeout,
            request_gate=self._request_gate,
            pool_size=self._pool_size,
        )

    def _get(self, url: str, *, request_delay: float | None = None) -> JSON | None:
        """Make a GET request with retry logic for transient failures."""
        return execute_json_request(
            self.session,
            url,
            timeout=self._timeout,
            request_delay=request_delay if request_delay is not None else self._request_delay,
            max_retries=self._max_retries,
            request_gate=self._request_gate,
        )

    def get_bootstrap(self, force: bool = False) -> JSON:
        """Get bootstrap-static data (cached).

        Args:
            force: If True, bypass cache and fetch fresh data.

        Returns:
            Bootstrap data dict.

        Raises:
            RuntimeError: If bootstrap data cannot be fetched.
        """
        if self._bootstrap_cache is None or force:
            logger.info("Fetching bootstrap-static data...")
            self._bootstrap_cache = self._get(ENDPOINTS["bootstrap"])

        if self._bootstrap_cache is None:
            raise FPLClientError("Failed to fetch bootstrap data from FPL API")

        return self._bootstrap_cache

    def get_current_gw(self) -> int:
        """Get the current gameweek, or the latest finished one if none is current.

        Raises:
            RuntimeError: If no gameweek data found.
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

    def get_gw_deadline(self, gw: int) -> Optional[datetime]:
        """Get deadline datetime for a specific gameweek."""
        logger.info(f"Getting GW{gw} deadline...")
        bootstrap = self.get_bootstrap()
        for event in bootstrap.get("events", []):
            if event["id"] == gw:
                deadline_str = event.get("deadline_time")
                if deadline_str:
                    return datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
        return None

    def get_gw(self, gw: int) -> Optional[JSON]:
        """Get player stats for a gameweek (live endpoint)."""
        url = ENDPOINTS["live"].format(gw=gw)
        logger.info(f"Fetching GW{gw} data...")
        return self._get(url)

    def get_fixtures(self) -> Optional[JSON]:
        """Get all fixtures for the season."""
        logger.info("Fetching fixtures...")
        return self._get(ENDPOINTS["fixtures"])

    def get_player_history(self, player_id: int) -> Optional[JSON]:
        """Get a player's detailed history (element-summary)."""
        logger.info(f"Fetching player {player_id} history...")
        url = ENDPOINTS["player"].format(player_id=player_id)
        return self._get(url, request_delay=self._player_history_request_delay)

    def is_gw_finished(self, gw: int) -> bool:
        """Check if a gameweek has finished (all matches complete, bonus confirmed)."""
        logger.info(f"Checking if GW{gw} is finished...")
        bootstrap = self.get_bootstrap()
        for event in bootstrap.get("events", []):
            if event["id"] == gw:
                return event.get("finished", False)
        return False
