"""
Main entry point for running Slack bot OR MCP server.
Run one interface per process for better isolation and reliability.
"""
import os
import sys
import logging
import asyncio
import argparse

logger = logging.getLogger(__name__)


def setup_logging():
    """Configure application logging.
    
    Environment variables:
    - LOG_LEVEL: Logging level (default: INFO)
    - LOG_TO_FILE: Enable file logging (default: true). When false, only log to stdout.
    """
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_to_file = os.getenv('LOG_TO_FILE', 'true').lower() == 'true'

    handlers = [logging.StreamHandler(sys.stdout)]

    if log_to_file:
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        handlers.append(logging.FileHandler('logs/app.log'))

    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=handlers
    )


def run_slack_bot():
    """Run Slack bot in current thread."""
    from simbot.config import load_config
    from simbot.interfaces.slack import SlackBot

    logger.info("Starting Slack bot...")
    config = load_config()
    bot = SlackBot(config)
    bot.start()


def run_mcp_server():
    """Run MCP server in current thread."""
    from simbot.interfaces.mcp.server import main as mcp_main

    logger.info("Starting MCP server...")
    asyncio.run(mcp_main())


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Simbot - Run Slack bot or MCP server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m simbot.main --slack          Run Slack bot only
  python -m simbot.main --mcp            Run MCP server only

  # Environment variables (if no CLI args provided):
  ENABLE_SLACK=true python -m simbot.main
  ENABLE_MCP=true python -m simbot.main

Note: Run ONE interface per process for better isolation and reliability.
        """
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--slack',
        action='store_true',
        help='Run Slack bot (overrides ENABLE_SLACK env var)'
    )
    group.add_argument(
        '--mcp',
        action='store_true',
        help='Run MCP server (overrides ENABLE_MCP env var)'
    )

    return parser.parse_args()


def main():
    """
    Main orchestrator - runs ONE interface at a time.

    Command-line arguments take precedence over environment variables.

    For production deployments, run separate processes:
    - Process 1: python -m simbot.main --slack
    - Process 2: python -m simbot.main --mcp

    This provides:
    - Better error isolation (one crashes, other keeps running)
    - Independent scaling
    - Simpler debugging
    - No daemon thread complexity
    """
    args = parse_args()
    setup_logging()

    # CLI args override environment variables
    if args.slack:
        enable_slack = True
        enable_mcp = False
        logger.info("Interface mode: Slack bot (from --slack flag)")
    elif args.mcp:
        enable_slack = False
        enable_mcp = True
        logger.info("Interface mode: MCP server (from --mcp flag)")
    else:
        # Fall back to environment variables
        enable_slack = os.getenv('ENABLE_SLACK', 'false').lower() == 'true'
        enable_mcp = os.getenv('ENABLE_MCP', 'false').lower() == 'true'
        logger.info(
            f"Interface mode: From environment variables "
            f"(ENABLE_SLACK={enable_slack}, ENABLE_MCP={enable_mcp})"
        )

    if not enable_slack and not enable_mcp:
        logger.error(
            "No interface enabled. Use:\n"
            "  python -m simbot.main --slack   (or ENABLE_SLACK=true)\n"
            "  python -m simbot.main --mcp     (or ENABLE_MCP=true)"
        )
        sys.exit(1)

    if enable_slack and enable_mcp:
        logger.error(
            "Cannot run both Slack and MCP in same process. "
            "Run as separate processes for better isolation:\n"
            "  Process 1: python -m simbot.main --slack\n"
            "  Process 2: python -m simbot.main --mcp"
        )
        sys.exit(1)

    if enable_slack:
        logger.info("Starting Slack interface...")
        run_slack_bot()  # Blocks
    elif enable_mcp:
        logger.info("Starting MCP interface...")
        run_mcp_server()  # Blocks


if __name__ == "__main__":
    main()
