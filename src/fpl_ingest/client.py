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
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

FPL_BASE_URL = "https://fantasy.premierleague.com/api"

ENDPOINTS = {
    "bootstrap": f"{FPL_BASE_URL}/bootstrap-static/",
    "fixtures": f"{FPL_BASE_URL}/fixtures/",
    "live": f"{FPL_BASE_URL}/event/{{gw}}/live/",
    "player": f"{FPL_BASE_URL}/element-summary/{{player_id}}/",
}

# Rate limiting defaults
DEFAULT_REQUEST_DELAY = 1.0
DEFAULT_MAX_RETRIES = 5
MAX_DELAY = 60
RATE_LIMIT_STATUS = 429


class FPLClient:
    """HTTP client for the FPL API with rate limiting and caching."""

    def __init__(
        self,
        request_delay: float = DEFAULT_REQUEST_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/537.36"
        })
        self._bootstrap_cache: Optional[Dict] = None
        self._request_delay = request_delay
        self._max_retries = max_retries
        self._current_delay = request_delay

    def _get(self, url: str) -> Optional[Dict]:
        """Make GET request with retry logic and adaptive rate limiting."""
        for attempt in range(self._max_retries):
            try:
                jitter = random.uniform(0, 0.3 * self._current_delay)
                time.sleep(self._current_delay + jitter)

                resp = self.session.get(url, timeout=30)

                if resp.status_code == RATE_LIMIT_STATUS:
                    retry_after = int(resp.headers.get("Retry-After", 30))
                    self._current_delay = min(self._current_delay * 2, MAX_DELAY)
                    logger.warning(
                        f"Rate limited (429). Waiting {retry_after}s. "
                        f"Delay now: {self._current_delay}s"
                    )
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()

                self._current_delay = max(self._request_delay, self._current_delay * 0.9)
                return resp.json()

            except requests.RequestException as e:
                wait_time = min(2 ** attempt + random.uniform(0, 1), MAX_DELAY)
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{self._max_retries}): {e}. "
                    f"Retrying in {wait_time:.1f}s"
                )
                if attempt < self._max_retries - 1:
                    time.sleep(wait_time)

        logger.error(f"All {self._max_retries} attempts failed for {url}")
        return None

    def get_bootstrap(self, force: bool = False) -> Dict:
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
            raise RuntimeError("Failed to fetch bootstrap data from FPL API")

        return self._bootstrap_cache

    def get_current_gw(self) -> int:
        """Get the current/latest finished gameweek number.

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

    def get_gw(self, gw: int) -> Optional[Dict]:
        """Get player stats for a gameweek (live endpoint)."""
        url = ENDPOINTS["live"].format(gw=gw)
        logger.info(f"Fetching GW{gw} data...")
        return self._get(url)

    def get_fixtures(self) -> Optional[List[Dict]]:
        """Get all fixtures for the season."""
        logger.info("Fetching fixtures...")
        return self._get(ENDPOINTS["fixtures"])

    def get_player_history(self, player_id: int) -> Optional[Dict]:
        """Get a player's detailed history (element-summary)."""
        logger.info(f"Fetching player {player_id} history...")
        url = ENDPOINTS["player"].format(player_id=player_id)
        return self._get(url)

    def is_gw_finished(self, gw: int) -> bool:
        """Check if a gameweek has finished (all matches complete, bonus confirmed)."""
        logger.info(f"Checking if GW{gw} is finished...")
        bootstrap = self.get_bootstrap()
        for event in bootstrap.get("events", []):
            if event["id"] == gw:
                return event.get("finished", False)
        return False
