"""
Simple per-user cooldown rate limiter.

Grug-approved: No Redis, no tokens, just timestamps in memory.
"""

import time
from typing import Dict


class RateLimiter:
    """
    Simple per-user cooldown rate limiter.

    Tracks last request time per user and enforces minimum time between requests.
    """

    def __init__(self, cooldown_seconds: int = 30):
        """
        Initialize rate limiter.

        Args:
            cooldown_seconds: Minimum seconds between requests per user
        """
        self.cooldown_seconds = cooldown_seconds
        self._last_request: Dict[str, float] = {}  # user_id -> timestamp

    def check_rate_limit(self, user_id: str) -> dict:
        """
        Check if user can make a request.

        Args:
            user_id: Unique user identifier

        Returns:
            dict with:
                - allowed (bool): True if request is allowed
                - wait_seconds (int): 0 if allowed, else seconds to wait
        """
        now = time.time()
        last = self._last_request.get(user_id, 0)
        elapsed = now - last

        if elapsed >= self.cooldown_seconds:
            self._last_request[user_id] = now
            return {"allowed": True, "wait_seconds": 0}
        else:
            wait = int(self.cooldown_seconds - elapsed) + 1  # Round up
            return {"allowed": False, "wait_seconds": wait}

    def reset(self, user_id: str):
        """
        Reset rate limit for user (admin override).

        Args:
            user_id: User to reset
        """
        self._last_request.pop(user_id, None)
