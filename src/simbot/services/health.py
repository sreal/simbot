"""Health check functions for external service dependencies."""

import logging

logger = logging.getLogger(__name__)


def check_audit_logging() -> bool:
    """
    Check if audit logging service is configured and working.

    The audit logger is used by tools like EchoTool to log all
    operations for compliance purposes.

    Returns:
        bool: True if audit logger is configured, False otherwise
    """
    try:
        # Check if audit logger exists and has handlers
        audit_logger = logging.getLogger("audit")

        if not audit_logger.handlers:
            logger.debug("Audit logger has no handlers configured")
            return False

        # Test write to verify it works
        audit_logger.debug("Health check test")

        return True
    except Exception as e:
        logger.debug(f"Audit logging check failed: {e}")
        return False
