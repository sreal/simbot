"""Slack client wrapper providing core functionality."""

import logging
from typing import Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackClient:
    """Wrapper around Slack SDK for common bot operations."""

    def __init__(self, bot_token: str):
        """
        Initialize Slack client.

        Args:
            bot_token: Slack Bot User OAuth Token (xoxb-...)
        """
        if not bot_token or not bot_token.startswith('xoxb-'):
            raise ValueError(f"Invalid bot_token: must start with 'xoxb-', got '{bot_token[:10]}...'")

        self.client = WebClient(token=bot_token)
        self.bot_user_id = None
        self._initialize_bot_info()

    def _initialize_bot_info(self):
        """Get bot user ID - needed to filter out bot's own messages."""
        try:
            response = self.client.auth_test()
            self.bot_user_id = response["user_id"]
            logger.info(f"Bot initialized: user_id={self.bot_user_id}")
        except SlackApiError as error:
            logger.error(f"Failed to get bot info: {error.response['error']}")
            raise

    def post_message(self, channel: str, text: str, thread_ts: Optional[str] = None) -> dict:
        """
        Post a message to a channel.

        Args:
            channel: Channel ID (C...) or DM ID (D...)
            text: Message text
            thread_ts: Optional thread timestamp for replies

        Returns:
            dict with success status and response data
        """
        try:
            response = self.client.chat_postMessage(
                channel=channel,
                text=text,
                thread_ts=thread_ts
            )
            return {
                "success": True,
                "data": response.data,
                "error": None
            }
        except SlackApiError as error:
            error_message = error.response.get('error', 'unknown_error')
            logger.error(f"Failed to post message to {channel}: {error_message}")
            return {
                "success": False,
                "data": None,
                "error": f"Slack API error: {error_message}"
            }

    def add_reaction(self, channel: str, timestamp: str, emoji: str) -> dict:
        """
        Add emoji reaction to a message.

        Args:
            channel: Channel ID
            timestamp: Message timestamp
            emoji: Emoji name (without colons, e.g., 'thumbsup')

        Returns:
            dict with success status
        """
        try:
            self.client.reactions_add(
                channel=channel,
                timestamp=timestamp,
                name=emoji
            )
            return {"success": True, "error": None}
        except SlackApiError as error:
            error_message = error.response.get('error', 'unknown_error')
            logger.error(f"Failed to add reaction {emoji} to {timestamp}: {error_message}")
            return {
                "success": False,
                "error": f"Failed to add reaction: {error_message}"
            }

    def get_user_info(self, user_id: str) -> Optional[dict]:
        """
        Get information about a user.

        Args:
            user_id: User ID (U...)

        Returns:
            User info dict or None if error
        """
        try:
            response = self.client.users_info(user=user_id)
            return response["user"]
        except SlackApiError as error:
            logger.error(f"Failed to get user info for {user_id}: {error.response['error']}")
            return None

    def is_bot_message(self, user_id: str) -> bool:
        """Check if message is from the bot itself."""
        return user_id == self.bot_user_id
