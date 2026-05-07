"""
MCP server for exposing SQL queries and blob checks as tools.
Supports both stdio (for local clients like Claude Desktop) and HTTP/SSE (for remote access via nginx).
"""
import sys
import json
import logging
import os
import signal
import atexit
import base64
from typing import Dict, Any
import uuid

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

from simbot.sql_tools import (
    QueryLoader,
    QueryExecutor,
    ExecutionContext,
    CompositeQueryExecutor,
)
from simbot.blob_tools import BlobCheckLoader, BlobCheckExecutor
from .converters import YAMLToMCPConverter, CompositeToMCPConverter
from .blob_converters import BlobToMCPConverter


logger = logging.getLogger(__name__)


class SQLQueryMCPServer:
    """MCP server for SQL query and blob check tools."""

    def __init__(self):
        self.server = Server("sql-query-server")

        # SQL tools
        self.query_loader = QueryLoader()
        self.sql_executor = QueryExecutor()
        self.sql_converter = YAMLToMCPConverter()

        # Composite tools (share the QueryLoader and QueryExecutor)
        self.composite_executor = CompositeQueryExecutor(
            self.sql_executor, self.query_loader
        )
        self.composite_converter = CompositeToMCPConverter()

        # Blob tools
        self.blob_loader = BlobCheckLoader()
        self.blob_executor = BlobCheckExecutor()
        self.blob_converter = BlobToMCPConverter()

        # Convert all definitions to MCP tools
        self.tools = self._build_tools()

        # Register handlers
        self._register_handlers()
        
        # Register shutdown cleanup
        self._register_shutdown_handlers()

        logger.info(f"MCP Server initialized with {len(self.tools)} tools")
    
    def _register_shutdown_handlers(self):
        """Register signal handlers and atexit for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self.shutdown()
            exit(0)
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        atexit.register(self.shutdown)
    
    def shutdown(self):
        """Clean up resources on shutdown."""
        logger.info("Shutting down MCP server...")
        
        # Close database connections
        try:
            self.executor.close_connections()
            logger.info("Closed database connections")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}", exc_info=True)
        
        logger.info("MCP server shutdown complete")

    def _build_tools(self) -> Dict[str, Dict[str, Any]]:
        """Build MCP tools from query and blob check definitions."""
        tools = {}

        # SQL query tools
        sql_tools = self.sql_converter.convert_all(self.query_loader.queries)
        for tool in sql_tools:
            tool['metadata']['tool_type'] = 'sql'
            tools[tool['name']] = tool

        # Blob check tools
        blob_tools = self.blob_converter.convert_all(self.blob_loader.checks)
        for tool in blob_tools:
            tool['metadata']['tool_type'] = 'blob'
            tools[tool['name']] = tool

        # Composite tools
        composite_tools = self.composite_converter.convert_all(
            self.query_loader.composites
        )
        for tool in composite_tools:
            tool['metadata']['tool_type'] = 'composite'
            tools[tool['name']] = tool

        return tools

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
            """Execute a tool (SQL query or blob check)."""

            # Find tool metadata
            tool_def = self.tools.get(name)
            if not tool_def:
                error_msg = f"Unknown tool: {name}"
                logger.error(error_msg)
                return [types.TextContent(
                    type="text",
                    text=json.dumps({'error': error_msg, 'success': False})
                )]

            # Create execution context
            context = ExecutionContext(
                correlation_id=str(uuid.uuid4()),
                interface='mcp',
                user_id='mcp_client'
            )

            # Dispatch based on tool type
            tool_type = tool_def['metadata'].get('tool_type', 'sql')

            if tool_type == 'blob':
                return await self._execute_blob_tool(tool_def, arguments, context)
            elif tool_type == 'composite':
                return await self._execute_composite_tool(tool_def, arguments, context)
            else:
                return await self._execute_sql_tool(tool_def, arguments, context)

    async def _execute_sql_tool(
        self,
        tool_def: Dict[str, Any],
        arguments: dict,
        context: ExecutionContext
    ) -> list[types.TextContent]:
        """Execute a SQL query tool."""
        query_id = tool_def['metadata']['query_id']
        query_def = self.query_loader.get_query_by_id(query_id)
        if not query_def:
            error_msg = f"Query not found: {query_id}"
            logger.error(error_msg)
            return [types.TextContent(
                type="text",
                text=json.dumps({'error': error_msg, 'success': False})
            )]

        # Execute query
        result = self.sql_executor.execute(query_def, arguments, context)

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

    async def _execute_composite_tool(
        self,
        tool_def: Dict[str, Any],
        arguments: dict,
        context: ExecutionContext
    ) -> list[types.TextContent]:
        """Execute a composite tool: fan out to referenced sub-queries."""
        composite_id = tool_def['metadata']['composite_id']
        composite_def = self.query_loader.get_composite_by_id(composite_id)
        if not composite_def:
            error_msg = f"Composite not found: {composite_id}"
            logger.error(error_msg)
            return [types.TextContent(
                type="text",
                text=json.dumps({'error': error_msg, 'success': False})
            )]

        result = self.composite_executor.execute(composite_def, arguments, context)

        return [types.TextContent(
            type="text",
            text=json.dumps(result.to_dict(), indent=2, default=str)
        )]

    async def _execute_blob_tool(
        self,
        tool_def: Dict[str, Any],
        arguments: dict,
        context: ExecutionContext
    ) -> list[types.TextContent]:
        """Execute a blob check tool."""
        check_id = tool_def['metadata']['check_id']
        check_def = self.blob_loader.get_by_id(check_id)
        if not check_def:
            error_msg = f"Blob check not found: {check_id}"
            logger.error(error_msg)
            return [types.TextContent(
                type="text",
                text=json.dumps({'error': error_msg, 'success': False})
            )]

        # Execute blob check
        result = self.blob_executor.execute(check_def, arguments, context)

        # Format response (convert file results to serializable format)
        if result.success:
            files_data = []
            for f in result.files:
                file_data = f.to_dict()
                # Include base64 content if present
                if f.content is not None:
                    file_data['content_base64'] = base64.b64encode(f.content).decode('utf-8')
                files_data.append(file_data)

            response = {
                'success': True,
                'definition_name': result.definition_name,
                'files': files_data,
                'summary': result.summary,
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

                        # Create execution context
                        context = ExecutionContext(
                            correlation_id=str(uuid.uuid4()),
                            interface='mcp',
                            user_id='mcp_http_client'
                        )

                        # Dispatch based on tool type
                        tool_type = tool_def['metadata'].get('tool_type', 'sql')

                        if tool_type == 'blob':
                            # Execute blob check
                            check_id = tool_def['metadata']['check_id']
                            check_def = self.blob_loader.get_by_id(check_id)
                            if not check_def:
                                return JSONResponse({
                                    'jsonrpc': '2.0',
                                    'id': message_id,
                                    'error': {'code': -32602, 'message': f'Blob check not found: {check_id}'}
                                })

                            result = self.blob_executor.execute(check_def, arguments, context)

                            if result.success:
                                files_data = []
                                for f in result.files:
                                    file_data = f.to_dict()
                                    if f.content is not None:
                                        file_data['content_base64'] = base64.b64encode(f.content).decode('utf-8')
                                    files_data.append(file_data)

                                response_data = {
                                    'success': True,
                                    'definition_name': result.definition_name,
                                    'files': files_data,
                                    'summary': result.summary,
                                    'correlation_id': result.correlation_id,
                                }
                            else:
                                response_data = {
                                    'success': False,
                                    'error': result.error,
                                    'error_code': result.error_code,
                                    'correlation_id': result.correlation_id,
                                }
                        elif tool_type == 'composite':
                            composite_id = tool_def['metadata']['composite_id']
                            composite_def = self.query_loader.get_composite_by_id(composite_id)
                            if not composite_def:
                                return JSONResponse({
                                    'jsonrpc': '2.0',
                                    'id': message_id,
                                    'error': {'code': -32602, 'message': f'Composite not found: {composite_id}'}
                                })

                            result = self.composite_executor.execute(composite_def, arguments, context)
                            response_data = result.to_dict()
                        else:
                            # Execute SQL query
                            query_id = tool_def['metadata']['query_id']
                            query_def = self.query_loader.get_query_by_id(query_id)
                            if not query_def:
                                return JSONResponse({
                                    'jsonrpc': '2.0',
                                    'id': message_id,
                                    'error': {'code': -32602, 'message': f'Query not found: {query_id}'}
                                })

                            result = self.sql_executor.execute(query_def, arguments, context)

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
