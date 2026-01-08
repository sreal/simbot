"""
MCP server for exposing SQL queries as tools.
Supports both stdio (for local clients like Claude Desktop) and HTTP/SSE (for remote access via nginx).
"""
import sys
import json
import logging
import os
from typing import Dict, Any
import uuid

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

from simbot.sql_tools import QueryLoader, QueryExecutor, ExecutionContext
from .converters import YAMLToMCPConverter


logger = logging.getLogger(__name__)


class SQLQueryMCPServer:
    """MCP server for SQL query tools."""

    def __init__(self):
        self.server = Server("sql-query-server")
        self.query_loader = QueryLoader()
        self.executor = QueryExecutor()
        self.converter = YAMLToMCPConverter()

        # Convert all queries to MCP tools
        self.tools = self._build_tools()

        # Register handlers
        self._register_handlers()

        logger.info(f"MCP Server initialized with {len(self.tools)} tools")

    def _build_tools(self) -> Dict[str, Dict[str, Any]]:
        """Build MCP tools from query definitions."""
        tools_list = self.converter.convert_all(self.query_loader.queries)
        # Index by name for fast lookup
        return {tool['name']: tool for tool in tools_list}

    def _register_handlers(self):
        """Register MCP protocol handlers."""

        @self.server.list_tools()
        async def list_tools() -> list[types.Tool]:
            """Return list of available tools."""
            return [
                types.Tool(
                    name=tool['name'],
                    description=tool['description'],
                    inputSchema=tool['inputSchema']
                )
                for tool in self.tools.values()
            ]

        @self.server.call_tool()
        async def call_tool(
            name: str,
            arguments: dict
        ) -> list[types.TextContent]:
            """Execute a tool (SQL query)."""

            # Find tool metadata
            tool_def = self.tools.get(name)
            if not tool_def:
                error_msg = f"Unknown tool: {name}"
                logger.error(error_msg)
                return [types.TextContent(
                    type="text",
                    text=json.dumps({'error': error_msg, 'success': False})
                )]

            # Get query definition
            query_id = tool_def['metadata']['query_id']
            query_def = self.query_loader.get_query_by_id(query_id)
            if not query_def:
                error_msg = f"Query not found: {query_id}"
                logger.error(error_msg)
                return [types.TextContent(
                    type="text",
                    text=json.dumps({'error': error_msg, 'success': False})
                )]

            # Create execution context
            context = ExecutionContext(
                correlation_id=str(uuid.uuid4()),
                interface='mcp',
                user_id='mcp_client'  # Could extract from MCP session metadata
            )

            # Execute query
            result = self.executor.execute(query_def, arguments, context)

            # Format response
            if result.success:
                response = {
                    'success': True,
                    'data': result.data,
                    'metadata': result.metadata,
                    'correlation_id': result.correlation_id,
                }
            else:
                response = {
                    'success': False,
                    'error': result.error,
                    'error_code': result.error_code,
                    'correlation_id': result.correlation_id,
                }

            return [types.TextContent(
                type="text",
                text=json.dumps(response, indent=2, default=str)
            )]

    async def run(self, transport='stdio', host='0.0.0.0', port=8080):
        """
        Run the MCP server with specified transport.

        Args:
            transport: 'stdio' for local clients (default) or 'http' for remote access
            host: Host to bind to (for HTTP transport)
            port: Port to listen on (for HTTP transport)
        """
        if transport == 'http':
            logger.info(f"Starting MCP server on SSE at {host}:{port}...")

            # Create SSE endpoint for MCP
            from starlette.applications import Starlette
            from starlette.routing import Route
            from starlette.responses import Response, StreamingResponse
            import asyncio
            from collections import defaultdict

            # Store active SSE connections
            sessions = defaultdict(lambda: {'queue': asyncio.Queue(), 'request_id': 0})

            async def health_check(request):
                """Health check endpoint for Docker."""
                return Response("OK", status_code=200)

            async def handle_sse(request):
                """SSE endpoint - server pushes events to client."""
                session_id = request.query_params.get('sessionId', 'default')
                session = sessions[session_id]

                async def event_generator():
                    """Generate SSE events."""
                    try:
                        logger.info(f"SSE connection established for session {session_id}")

                        # Keep connection alive and send messages
                        while True:
                            try:
                                # Wait for messages with timeout to send keepalive
                                message = await asyncio.wait_for(session['queue'].get(), timeout=30.0)
                                if message is None:  # Shutdown signal
                                    break
                                # Send as plain SSE data event
                                yield f"data: {json.dumps(message)}\n\n"
                            except asyncio.TimeoutError:
                                # Send keepalive comment
                                yield ": keepalive\n\n"
                    except asyncio.CancelledError:
                        logger.info(f"SSE connection closed for session {session_id}")
                    except Exception as e:
                        logger.error(f"SSE error for session {session_id}: {e}", exc_info=True)
                    finally:
                        # Cleanup
                        if session_id in sessions:
                            del sessions[session_id]

                return StreamingResponse(
                    event_generator(),
                    media_type="text/event-stream",
                    headers={
                        "Cache-Control": "no-cache",
                        "Connection": "keep-alive",
                        "X-Accel-Buffering": "no",
                    }
                )

            async def handle_messages(request):
                """Handle MCP JSON-RPC messages from client (POST endpoint)."""
                from starlette.responses import JSONResponse

                # Check protocol version
                protocol_version = request.headers.get('mcp-protocol-version', '2024-11-05')
                session_id = request.headers.get('mcp-session-id', 'default')
                session = sessions[session_id]

                try:
                    body = await request.json()
                    logger.info(f"Received MCP request: {body.get('method', 'unknown')} (protocol: {protocol_version})")

                    # Handle MCP protocol methods
                    method = body.get('method')
                    message_id = body.get('id')

                    if method == 'initialize':
                        # MCP initialization handshake - return JSON directly
                        response_data = {
                            'jsonrpc': '2.0',
                            'id': message_id,
                            'result': {
                                'protocolVersion': protocol_version,
                                'capabilities': {
                                    'tools': {}
                                },
                                'serverInfo': {
                                    'name': 'sql-query-server',
                                    'version': '1.0.0'
                                }
                            }
                        }
                        # Return JSON response with session ID header
                        return JSONResponse(
                            response_data,
                            headers={'Mcp-Session-Id': session_id}
                        )

                    elif method == 'tools/list':
                        # Return list of available tools as JSON
                        tools = [
                            {
                                'name': tool['name'],
                                'description': tool['description'],
                                'inputSchema': tool['inputSchema']
                            }
                            for tool in self.tools.values()
                        ]
                        return JSONResponse({
                            'jsonrpc': '2.0',
                            'id': message_id,
                            'result': {'tools': tools}
                        })

                    elif method == 'tools/call':
                        params = body.get('params', {})
                        tool_name = params.get('name')
                        arguments = params.get('arguments', {})

                        # Find tool metadata
                        tool_def = self.tools.get(tool_name)
                        if not tool_def:
                            return JSONResponse({
                                'jsonrpc': '2.0',
                                'id': message_id,
                                'error': {'code': -32602, 'message': f'Unknown tool: {tool_name}'}
                            })

                        # Get query definition
                        query_id = tool_def['metadata']['query_id']
                        query_def = self.query_loader.get_query_by_id(query_id)
                        if not query_def:
                            return JSONResponse({
                                'jsonrpc': '2.0',
                                'id': message_id,
                                'error': {'code': -32602, 'message': f'Query not found: {query_id}'}
                            })

                        # Create execution context
                        context = ExecutionContext(
                            correlation_id=str(uuid.uuid4()),
                            interface='mcp',
                            user_id='mcp_http_client'
                        )

                        # Execute query
                        result = self.executor.execute(query_def, arguments, context)

                        # Format response
                        if result.success:
                            response_data = {
                                'success': True,
                                'data': result.data,
                                'metadata': result.metadata,
                                'correlation_id': result.correlation_id,
                            }
                        else:
                            response_data = {
                                'success': False,
                                'error': result.error,
                                'error_code': result.error_code,
                                'correlation_id': result.correlation_id,
                            }

                        return JSONResponse({
                            'jsonrpc': '2.0',
                            'id': message_id,
                            'result': {
                                'content': [{
                                    'type': 'text',
                                    'text': json.dumps(response_data, indent=2, default=str)
                                }]
                            }
                        })

                    else:
                        return JSONResponse({
                            'jsonrpc': '2.0',
                            'id': message_id,
                            'error': {'code': -32601, 'message': f'Method not found: {method}'}
                        })

                except Exception as e:
                    logger.error(f"Error handling request: {e}", exc_info=True)
                    return JSONResponse({
                        'jsonrpc': '2.0',
                        'id': message_id if 'message_id' in locals() else None,
                        'error': {'code': -32603, 'message': str(e)}
                    })

            async def handle_sse_endpoint(request):
                """Handle both GET (SSE) and POST (messages) at /sse."""
                if request.method == "GET":
                    return await handle_sse(request)
                else:  # POST
                    return await handle_messages(request)

            app = Starlette(
                routes=[
                    Route("/health", health_check),
                    Route("/sse", handle_sse_endpoint, methods=["GET", "POST"]),
                ]
            )

            import uvicorn
            config = uvicorn.Config(app, host=host, port=port, log_level="info")
            server = uvicorn.Server(config)
            await server.serve()

        else:  # stdio (default)
            logger.info("Starting MCP server on stdio...")
            async with stdio_server() as (read_stream, write_stream):
                await self.server.run(
                    read_stream,
                    write_stream,
                    self.server.create_initialization_options()
                )


async def main():
    """Entry point for MCP server."""
    # Configure logging
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler('logs/mcp_server.log'),
            logging.StreamHandler(sys.stderr)  # MCP uses stdout for protocol
        ]
    )

    # Read configuration from environment
    transport = os.getenv('MCP_TRANSPORT', 'stdio').lower()
    host = os.getenv('MCP_HOST', '0.0.0.0')
    port = int(os.getenv('MCP_PORT', '8080'))

    logger.info(f"MCP server starting with transport={transport}, host={host}, port={port}")

    server = SQLQueryMCPServer()
    await server.run(transport=transport, host=host, port=port)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
