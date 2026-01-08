"""Configuration management for the Slack bot."""

import os
import logging
from dataclasses import dataclass
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Bot configuration with validation."""
    slack_bot_token: str
    slack_app_token: str
    slack_signing_secret: str
    log_level: str = "INFO"

    def __post_init__(self):
        """Validate configuration after initialization."""
        errors = []

        if not self.slack_bot_token or not self.slack_bot_token.startswith('xoxb-'):
            errors.append("SLACK_BOT_TOKEN must start with 'xoxb-'")

        if not self.slack_app_token or not self.slack_app_token.startswith('xapp-'):
            errors.append("SLACK_APP_TOKEN must start with 'xapp-'")

        if not self.slack_signing_secret:
            errors.append("SLACK_SIGNING_SECRET is required")

        if errors:
            error_message = "Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_message)


def load_config() -> Config:
    """
    Load configuration from environment variables.

    Loads from .env file if present, then from environment variables.

    Returns:
        Config object with validated settings
    """
    load_dotenv()

    try:
        config = Config(
            slack_bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
            slack_app_token=os.getenv("SLACK_APP_TOKEN", ""),
            slack_signing_secret=os.getenv("SLACK_SIGNING_SECRET", ""),
            log_level=os.getenv("LOG_LEVEL", "INFO")
        )
        logger.info("Configuration loaded successfully")
        return config
    except ValueError as error:
        logger.error(f"Failed to load configuration: {error}")
        raise


def setup_logging(log_level: str = "INFO"):
    """
    Setup logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
