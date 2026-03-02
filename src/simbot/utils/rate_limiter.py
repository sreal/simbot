"""
Simple per-user cooldown rate limiter.

Grug-approved: No Redis, no tokens, just timestamps in memory.
"""

import time
from typing import Dict
from dataclasses import dataclass


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    
    allowed: bool
    wait_seconds: int
    
    def __getitem__(self, key: str):
        """Support dict-like access for backward compatibility."""
        if key == 'allowed':
            return self.allowed
        elif key == 'wait_seconds':
            return self.wait_seconds
        else:
            raise KeyError(f"Key '{key}' not found in RateLimitResult")
    
    def get(self, key: str, default=None):
        """Support dict-like get() for backward compatibility."""
        try:
            return self[key]
        except KeyError:
            return default


class RateLimiter:
    """
    Simple per-user cooldown rate limiter.

    Tracks last request time per user and enforces minimum time between requests.
    Supports cleanup to prevent unbounded growth of the internal tracking table.
    """

    def __init__(self, cooldown_seconds: int = 30, max_entries: int = 10000):
        """
        Initialize rate limiter.

        Args:
            cooldown_seconds: Minimum seconds between requests per user
            max_entries: Maximum number of user entries to track (prevents unbounded growth)
        """
        self.cooldown_seconds = cooldown_seconds
        self.max_entries = max_entries
        self._last_request: Dict[str, float] = {}  # user_id -> timestamp

    def check_rate_limit(self, user_id: str) -> RateLimitResult:
        """
        Check if user can make a request.

        Args:
            user_id: Unique user identifier

        Returns:
            RateLimitResult with:
                - allowed (bool): True if request is allowed
                - wait_seconds (int): 0 if allowed, else seconds to wait
                
        The result supports dict-like access for backward compatibility:
            result['allowed'], result['wait_seconds'], result.get('allowed')
        """
        now = time.time()
        last = self._last_request.get(user_id, 0)
        elapsed = now - last

        if elapsed >= self.cooldown_seconds:
            self._last_request[user_id] = now
            return RateLimitResult(allowed=True, wait_seconds=0)
        else:
            wait = int(self.cooldown_seconds - elapsed) + 1  # Round up
            return RateLimitResult(allowed=False, wait_seconds=wait)

    def reset(self, user_id: str):
        """
        Reset rate limit for user (admin override).

        Args:
            user_id: User to reset
        """
        self._last_request.pop(user_id, None)
    
    def cleanup(self, max_age_seconds: int = 86400):
        """
        Remove entries for users with no activity older than max_age_seconds.
        
        This prevents unbounded growth of the internal tracking table.
        By default, removes entries for users who haven't made a request in 24 hours.
        
        Args:
            max_age_seconds: Maximum age of entries to keep (default: 86400 = 24 hours)
            
        Returns:
            Number of entries removed
        """
        now = time.time()
        to_remove = []
        
        for user_id, timestamp in self._last_request.items():
            age = now - timestamp
            if age > max_age_seconds:
                to_remove.append(user_id)
        
        for user_id in to_remove:
            del self._last_request[user_id]
        
        return len(to_remove)
