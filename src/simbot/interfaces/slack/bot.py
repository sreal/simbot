"""Main bot entry point."""

import logging
import re
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from simbot.config import load_config, setup_logging
from simbot.slack import SlackClient

logger = logging.getLogger(__name__)


class SlackBot:
    """Main bot orchestrator that manages Slack events and tools."""

    def __init__(self, config):
        """
        Initialize the bot.

        Args:
            config: Config object with tokens
        """
        self.config = config

        # Initialize Slack client wrapper
        self.slack_client = SlackClient(config.slack_bot_token)

        # Initialize Slack Bolt app for event handling
        self.app = App(
            token=config.slack_bot_token,
            signing_secret=config.slack_signing_secret
        )

        # Command handler registry (for tools to self-register)
        self.command_handlers = {}

        # Initialize tools registry (lazy loading)
        self.tools = self._initialize_tools()

        # Let tools register their own command handlers
        for tool in self.tools.values():
            if hasattr(tool, 'register_handlers'):
                tool.register_handlers(self)

        # Register event handlers
        self._register_handlers()

        # Check tool availability
        self._check_tool_availability()

        logger.info("Bot initialized successfully")

    def _initialize_tools(self):
        """
        Initialize tools with lazy imports for better startup performance.

        Returns:
            dict: Tool registry {name: tool_instance}
        """
        from simbot.tools import EchoTool
        from simbot.interfaces.slack.tools.sql_tool import DomainSQLTool

        return {
            "echo": EchoTool(),
            "domain_sql": DomainSQLTool(),
        }

    def _check_tool_availability(self):
        """Log which tools are available on startup."""
        logger.info("Checking tool availability...")

        for name, tool in self.tools.items():
            status = tool.check_availability()

            if status["available"]:
                logger.info(f"✓ {name}: {status['reason']}")
            else:
                logger.warning(f"✗ {name}: {status['reason']}")
                if status["error"]:
                    logger.warning(f"  Error: {status['error']}")

    def _show_help(self, event, say, thread_ts=None):
        """Show available commands from all tools."""
        if thread_ts is None:
            thread_ts = event.get("thread_ts") or event.get("ts")
        user = event.get("user")

        # Collect help text from all available tools
        commands = []
        for name, tool in self.tools.items():
            # Check if tool is available
            if not tool.check_availability()["available"]:
                continue  # Skip unavailable tools

            # Get help text if tool provides it
            if hasattr(tool, 'get_help_text'):
                commands.append(tool.get_help_text())

        if not commands:
            commands.append("_No commands available_")

        help_text = f"👋 Hi <@{user}>! Available commands:\n" + "\n".join(commands)

        say(text=help_text, thread_ts=thread_ts)

    def _dispatch_command(self, text, event, say, channel=None, thread_ts=None):
        """
        Dispatch command to appropriate handler.

        This is the shared logic for both @mentions and DMs.

        Args:
            text: Message text
            event: Slack event
            say: Say function
            channel: Channel ID (optional, not used in DMs)
            thread_ts: Thread timestamp (optional, not used in DMs)

        Returns:
            True if command was handled, False otherwise
        """
        user = event.get("user")
        message_ts = event.get("ts")

        # Add thinking reaction (eyes emoji) to indicate processing
        if channel and message_ts:
            try:
                logger.debug(f"Adding reaction to channel={channel}, ts={message_ts}")
                self.app.client.reactions_add(
                    channel=channel,
                    name="eyes",
                    timestamp=message_ts
                )
            except Exception as e:
                logger.warning(f"Failed to add reaction: {e}")

        # Try registered command handlers
        handled = False
        for handler_name, handler in self.command_handlers.items():
            try:
                if handler(text, event, say, channel, thread_ts):
                    logger.info(f"Command handled by: {handler_name}")
                    handled = True
                    break  # Command was handled
            except Exception as e:
                logger.error(f"Handler {handler_name} failed: {e}", exc_info=True)
                # Continue to next handler

        # If no command matched, show help
        if not handled:
            self._show_help(event, say, thread_ts=thread_ts)

        # Replace thinking reaction with completion reaction
        if channel and message_ts:
            try:
                logger.debug(f"Updating reaction on channel={channel}, ts={message_ts}")
                self.app.client.reactions_remove(
                    channel=channel,
                    name="eyes",
                    timestamp=message_ts
                )
                self.app.client.reactions_add(
                    channel=channel,
                    name="white_check_mark",
                    timestamp=message_ts
                )
            except Exception as e:
                logger.warning(f"Failed to update reactions: {e}")

        return True

    def _register_handlers(self):
        """Register all event handlers."""

        # Handle app mentions (@botname)
        @self.app.event("app_mention")
        def handle_app_mention(event, say):
            """Handle when bot is mentioned."""
            user = event.get("user")
            text = event.get("text", "")
            channel = event.get("channel")
            thread_ts = event.get("thread_ts") or event.get("ts")

            # Don't respond to self
            if self.slack_client.is_bot_message(user):
                return

            logger.info(f"Mentioned in {channel} by {user}: {text}")

            # Dispatch to shared command handler
            self._dispatch_command(text, event, say, channel, thread_ts)

        # Handle direct messages
        @self.app.event("message")
        def handle_message(event, say):
            """Handle direct messages."""
            # Only handle DMs
            channel_type = event.get("channel_type")
            if channel_type != "im":
                return

            user = event.get("user")
            text = event.get("text", "")
            channel = event.get("channel")

            # Don't respond to self or bot messages
            if self.slack_client.is_bot_message(user) or event.get("bot_id"):
                return

            logger.info(f"DM from {user}: {text}")
            logger.debug(f"DM event - channel: {channel}, ts: {event.get('ts')}, channel_type: {channel_type}")

            # Dispatch to shared command handler (same as @mentions)
            # Note: DMs don't use thread_ts, so we pass None
            self._dispatch_command(text, event, say, channel=channel, thread_ts=None)

        logger.info("Event handlers registered")

    def start(self):
        """Start the bot using Socket Mode (WebSocket connection)."""
        logger.info("Starting bot in Socket Mode...")

        handler = SocketModeHandler(
            app=self.app,
            app_token=self.config.slack_app_token
        )

        handler.start()


def _setup_audit_logger():
    """Configure audit logger for compliance logging."""
    import os

    audit_logger = logging.getLogger("audit")
    audit_logger.setLevel(logging.INFO)

    # Get log path from environment or use default
    log_path = os.getenv("AUDIT_LOG_PATH", "./logs/audit.log")

    # Create directory if it doesn't exist
    log_dir = os.path.dirname(log_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Add file handler
    handler = logging.FileHandler(log_path)
    handler.setLevel(logging.INFO)

    # Set format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    audit_logger.addHandler(handler)
    logger.info(f"Audit logger configured: {log_path}")


def main():
    """Main entry point for the bot."""
    try:
        # Load configuration
        config = load_config()

        # Setup logging
        setup_logging(config.log_level)

        # Setup audit logger
        _setup_audit_logger()

        logger.info("=" * 50)
        logger.info("Simbot SQL Tools Bot Starting")
        logger.info("=" * 50)

        # Initialize and start bot
        bot = SlackBot(config)
        bot.start()

    except ValueError as error:
        print(f"\nConfiguration Error:\n{error}\n")
        print("Make sure you have a .env file with required tokens.")
        print("Copy .env.example to .env and fill in your Slack credentials.\n")
        return 1
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        return 0
    except Exception as error:
        logger.error(f"Failed to start bot: {error}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
