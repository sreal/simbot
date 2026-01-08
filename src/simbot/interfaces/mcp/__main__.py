"""
Entry point for running MCP server standalone.
Usage: python -m simbot.interfaces.mcp
"""
import asyncio
from .server import main

if __name__ == "__main__":
    asyncio.run(main())
