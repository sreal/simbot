"""Tests for simbot.utils.rate_limiter module."""

import time
import pytest

from simbot.utils.rate_limiter import RateLimiter, RateLimitResult


class TestRateLimitResult:
    """Tests for RateLimitResult dataclass."""

    def test_creation(self):
        """Test creating a RateLimitResult."""
        result = RateLimitResult(allowed=True, wait_seconds=0)
        assert result.allowed is True
        assert result.wait_seconds == 0

    def test_dict_like_access(self):
        """Test dict-like access for backward compatibility."""
        result = RateLimitResult(allowed=False, wait_seconds=5)
        
        # Test bracket access
        assert result['allowed'] is False
        assert result['wait_seconds'] == 5

    def test_dict_like_get(self):
        """Test dict-like get() method for backward compatibility."""
        result = RateLimitResult(allowed=True, wait_seconds=0)
        
        assert result.get('allowed') is True
        assert result.get('wait_seconds') == 0
        assert result.get('nonexistent') is None
        assert result.get('nonexistent', 'default') == 'default'

    def test_dict_like_access_invalid_key(self):
        """Test that dict-like access raises KeyError for invalid keys."""
        result = RateLimitResult(allowed=True, wait_seconds=0)
        
        with pytest.raises(KeyError):
            _ = result['invalid_key']


class TestRateLimiter:
    """Tests for RateLimiter."""

    def test_initialization(self):
        """Test rate limiter initialization."""
        limiter = RateLimiter(cooldown_seconds=30)
        assert limiter.cooldown_seconds == 30
        assert len(limiter._last_request) == 0

    def test_custom_max_entries(self):
        """Test custom max_entries parameter."""
        limiter = RateLimiter(cooldown_seconds=30, max_entries=5000)
        assert limiter.max_entries == 5000

    def test_first_request_allowed(self):
        """Test that first request is always allowed."""
        limiter = RateLimiter(cooldown_seconds=30)
        result = limiter.check_rate_limit("user1")
        
        assert result.allowed is True
        assert result.wait_seconds == 0

    def test_second_request_blocked_within_cooldown(self):
        """Test that second request is blocked within cooldown period."""
        limiter = RateLimiter(cooldown_seconds=2)
        
        # First request - allowed
        result1 = limiter.check_rate_limit("user1")
        assert result1.allowed is True
        
        # Second request immediately - blocked
        result2 = limiter.check_rate_limit("user1")
        assert result2.allowed is False
        assert result2.wait_seconds > 0
        assert result2.wait_seconds <= 2

    def test_request_allowed_after_cooldown(self):
        """Test that request is allowed after cooldown expires."""
        limiter = RateLimiter(cooldown_seconds=1)
        
        # First request
        result1 = limiter.check_rate_limit("user1")
        assert result1.allowed is True
        
        # Wait for cooldown to expire
        time.sleep(1.1)
        
        # Second request - allowed
        result2 = limiter.check_rate_limit("user1")
        assert result2.allowed is True
        assert result2.wait_seconds == 0

    def test_different_users_independent(self):
        """Test that different users have independent rate limits."""
        limiter = RateLimiter(cooldown_seconds=2)
        
        # First user makes request
        result1 = limiter.check_rate_limit("user1")
        assert result1.allowed is True
        
        # Second user should not be rate limited
        result2 = limiter.check_rate_limit("user2")
        assert result2.allowed is True
        
        # First user blocked again
        result3 = limiter.check_rate_limit("user1")
        assert result3.allowed is False
        
        # Second user still allowed
        result4 = limiter.check_rate_limit("user2")
        assert result4.allowed is False

    def test_reset_user(self):
        """Test resetting rate limit for specific user."""
        limiter = RateLimiter(cooldown_seconds=10)
        
        # First request - allowed
        result1 = limiter.check_rate_limit("user1")
        assert result1.allowed is True
        
        # Second request blocked
        result2 = limiter.check_rate_limit("user1")
        assert result2.allowed is False
        
        # Reset user
        limiter.reset("user1")
        
        # Now request is allowed again
        result3 = limiter.check_rate_limit("user1")
        assert result3.allowed is True

    def test_reset_nonexistent_user(self):
        """Test that resetting nonexistent user doesn't raise error."""
        limiter = RateLimiter()
        
        # Should not raise an error
        limiter.reset("nonexistent_user")

    def test_cleanup_removes_old_entries(self):
        """Test that cleanup removes old entries."""
        limiter = RateLimiter(cooldown_seconds=1)
        
        # Create entries for multiple users
        limiter.check_rate_limit("user1")
        limiter.check_rate_limit("user2")
        limiter.check_rate_limit("user3")
        
        assert len(limiter._last_request) == 3
        
        # Wait for entries to age beyond max_age
        time.sleep(1.1)
        
        # Cleanup with 0.5 second max age removes all old entries
        removed = limiter.cleanup(max_age_seconds=0.5)
        
        # Should have removed all entries
        assert removed == 3
        assert len(limiter._last_request) == 0

    def test_cleanup_keeps_recent_entries(self):
        """Test that cleanup keeps entries within max_age."""
        limiter = RateLimiter(cooldown_seconds=1)
        
        # Create entries for multiple users
        limiter.check_rate_limit("user1")
        limiter.check_rate_limit("user2")
        
        time.sleep(0.5)
        
        # Create a new entry
        limiter.check_rate_limit("user3")
        
        # Cleanup with 0.3 second max age
        # Should remove user1 and user2 but keep user3
        removed = limiter.cleanup(max_age_seconds=0.3)
        
        assert removed == 2
        assert "user3" in limiter._last_request
        assert "user1" not in limiter._last_request
        assert "user2" not in limiter._last_request

    def test_cleanup_zero_removed(self):
        """Test cleanup when no entries should be removed."""
        limiter = RateLimiter(cooldown_seconds=1)
        
        limiter.check_rate_limit("user1")
        
        # Cleanup with very large max age removes nothing
        removed = limiter.cleanup(max_age_seconds=3600)
        
        assert removed == 0
        assert len(limiter._last_request) == 1

    def test_cleanup_default_max_age(self):
        """Test cleanup with default max_age_seconds (24 hours)."""
        limiter = RateLimiter(cooldown_seconds=1)
        
        limiter.check_rate_limit("user1")
        
        # Default cleanup should not remove recent entries
        removed = limiter.cleanup()  # Uses 86400 seconds default
        
        assert removed == 0
        assert len(limiter._last_request) == 1

    def test_result_is_typed(self):
        """Test that result is RateLimitResult instance."""
        limiter = RateLimiter()
        result = limiter.check_rate_limit("user1")
        
        assert isinstance(result, RateLimitResult)
        assert hasattr(result, 'allowed')
        assert hasattr(result, 'wait_seconds')

    def test_wait_seconds_accuracy(self):
        """Test that wait_seconds calculation is accurate."""
        limiter = RateLimiter(cooldown_seconds=2)
        
        # First request
        limiter.check_rate_limit("user1")
        
        # Wait 0.5 seconds then check
        time.sleep(0.5)
        result = limiter.check_rate_limit("user1")
        
        # Should need to wait ~1.5 seconds (rounded up)
        assert result.allowed is False
        assert 1 <= result.wait_seconds <= 2

    def test_backward_compatibility_dict_access(self):
        """Test that old code using dict-like access still works."""
        limiter = RateLimiter(cooldown_seconds=2)
        
        # Old code might do this:
        result = limiter.check_rate_limit("user1")
        
        # Both attribute and dict access should work
        assert result.allowed == result['allowed']
        assert result.wait_seconds == result['wait_seconds']
        
        # Old code using get() should also work
        assert result.get('allowed') == result.allowed
        assert result.get('wait_seconds') == result.wait_seconds
