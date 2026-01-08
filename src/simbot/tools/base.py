"""Base tool interface for extensibility."""

import os
import logging

logger = logging.getLogger(__name__)


class Tool:
    """Base class for all tools that can be executed by the bot."""

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self._flag_name = f"ENABLE_{name.upper()}"

    def is_enabled(self) -> bool:
        """
        Check if feature flag enables this tool.

        Default: disabled (opt-in)

        Returns:
            bool: True if feature is enabled, False otherwise
        """
        value = os.getenv(self._flag_name, "false").lower()
        return value in ("true", "1", "yes")

    def check_health(self) -> dict:
        """
        Check if tool's runtime dependencies are available.

        Override this method in subclasses to add health checks.

        Returns:
            dict with keys:
                - healthy (bool): True if dependencies are available
                - error (str or None): Error message if unhealthy
        """
        # Default: no dependencies = always healthy
        return {
            "healthy": True,
            "error": None
        }

    def check_availability(self) -> dict:
        """
        Check if tool is both enabled and healthy.

        Returns:
            dict with keys:
                - available (bool): True if tool can be used
                - reason (str): Human-readable reason
                - error (str or None): Detailed error if unavailable
        """
        # Check feature flag first
        if not self.is_enabled():
            return {
                "available": False,
                "reason": f"Feature flag {self._flag_name} is disabled",
                "error": None
            }

        # Check health
        health = self.check_health()
        if not health["healthy"]:
            return {
                "available": False,
                "reason": "Health check failed",
                "error": health["error"]
            }

        return {
            "available": True,
            "reason": "Feature enabled and healthy",
            "error": None
        }

    def execute(self, **kwargs):
        """
        Execute the tool with given parameters.

        Returns:
            dict with keys: success (bool), result (any), error (str or None)
        """
        raise NotImplementedError("Tool must implement execute method")

    def __repr__(self):
        return f"Tool(name='{self.name}', description='{self.description}')"


class EchoTool(Tool):
    """Example tool that echoes back the provided message with audit logging."""

    def __init__(self):
        super().__init__(
            name="echo",
            description="Echoes back the provided message"
        )

    def check_health(self) -> dict:
        """Check if audit logging service is available."""
        from simbot.services.health import check_audit_logging

        if not check_audit_logging():
            return {
                "healthy": False,
                "error": "Audit logging service not configured"
            }

        return {
            "healthy": True,
            "error": None
        }

    def execute(self, message: str = "", **kwargs):
        """Echo back the message with audit logging."""
        # Log to audit
        self._audit_log(message)

        return {
            "success": True,
            "result": f"Echo: {message}",
            "error": None
        }

    def _audit_log(self, message: str):
        """Log message to audit log."""
        audit_logger = logging.getLogger("audit")
        audit_logger.info(f"Echo executed: {message[:50]}{'...' if len(message) > 50 else ''}")
