"""Domain SQL Tool - Execute pre-defined SQL queries from YAML definitions."""

import logging
import re
import uuid
from datetime import datetime
from typing import List, Dict

from simbot.tools.base import Tool
from simbot.sql_tools import QueryLoader, QueryExecutor, ExecutionContext, QueryResult


logger = logging.getLogger(__name__)


class DomainSQLTool(Tool):
    """Execute safe, pre-defined SQL queries for business data."""

    def __init__(self):
        super().__init__(
            name="domain_sql",
            description="Execute pre-defined SQL queries for business data lookups",
        )
        self.query_loader = QueryLoader()
        self.executor = QueryExecutor()

    def execute(self, query_id: str, params: dict, user_id: str = None) -> dict:
        """
        Execute domain query.

        Args:
            query_id: Query identifier
            params: Parameter values
            user_id: User ID for audit logging

        Returns:
            {
                'success': bool,
                'result': formatted result string,
                'error': str | None,
                'metadata': {...}
            }
        """
        query_def = self.query_loader.get_query_by_id(query_id)
        if not query_def:
            return {
                "success": False,
                "error": f"Unknown query: {query_id}",
                "metadata": {},
            }

        # Create execution context
        context = ExecutionContext(
            correlation_id=str(uuid.uuid4()),
            interface='slack',
            user_id=user_id
        )

        # Execute query
        result: QueryResult = self.executor.execute(query_def, params, context)

        # Format result for display
        if result.success:
            formatted = self._format_result_data(result, query_def.name, params)
            return {
                "success": True,
                "result": formatted,
                "metadata": result.metadata,
                "correlation_id": result.correlation_id,
            }
        else:
            return {
                "success": False,
                "error": result.error,
                "metadata": result.metadata,
                "correlation_id": result.correlation_id,
            }

    def _format_result_data(self, result: QueryResult, query_name: str, params: dict) -> str:
        """Format query result for Slack display."""
        # Build parameter display
        if params:
            if len(params) == 1:
                param_display = f"`{list(params.values())[0]}`"
            else:
                param_parts = [f"{k}=`{v}`" for k, v in params.items()]
                param_display = ", ".join(param_parts)
            parts = [f"**{query_name}** for {param_display}:"]
        else:
            parts = [f"**{query_name}**:"]

        # Format data
        if result.data is None:
            parts.append("\nNo results")
        else:
            parts.append("")
            parts.append(self._format_table(result.data))

        # Add cache info if from cache
        if result.metadata.get("from_cache"):
            cached_at = result.metadata["cached_at"]
            cached_time = datetime.fromtimestamp(cached_at).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            parts.append(f"\n[cached: {cached_time}]")

        return "\n".join(parts)

    def _format_table(self, rows: List[Dict]) -> str:
        """Format rows as ASCII table."""
        if not rows:
            return "No results"

        # Get column names from first row
        columns = list(rows[0].keys())

        # Calculate column widths
        widths = {}
        for col in columns:
            widths[col] = len(col)
            for row in rows:
                value_str = str(row[col]) if row[col] is not None else ""
                widths[col] = max(widths[col], len(value_str))

        # Build table
        lines = []

        # Header
        header = "| " + " | ".join(col.ljust(widths[col]) for col in columns) + " |"
        separator = "+-" + "-+-".join("-" * widths[col] for col in columns) + "-+"

        lines.append(separator)
        lines.append(header)
        lines.append(separator)

        # Rows
        for row in rows:
            values = []
            for col in columns:
                value = row[col]
                value_str = str(value) if value is not None else ""
                values.append(value_str.ljust(widths[col]))
            lines.append("| " + " | ".join(values) + " |")

        lines.append(separator)

        table = "\n".join(lines)
        return f"```\n{table}\n```"

    def register_handlers(self, bot):
        """Register Slack command handlers."""
        bot.command_handlers["domain_query"] = self._handle_query
        bot.command_handlers["domain_queries_list"] = self._handle_list_queries
        bot.command_handlers["domain_cache_clear"] = self._handle_clear_cache
        bot.command_handlers["domain_queries_reload"] = self._handle_reload_queries

    def _handle_query(self, text, event, say, channel, thread_ts):
        """
        Handle domain query execution.

        Parses messages like:
        - @bot account lookup lite n easy
        - @bot beacon impressions 123 2025-01-01 2025-12-31
        """
        # Remove bot mention if present (for @mentions in channels)
        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

        # Try to match a query trigger
        query_def = self.query_loader.get_query_by_trigger(text)

        if not query_def:
            return False  # Not a domain query

        # Find query_id from loader
        query_id = None
        for qid, qdef in self.query_loader.queries.items():
            if qdef == query_def:
                query_id = qid
                break

        if not query_id:
            logger.error(f"Could not find query_id for query: {query_def.name}")
            return False

        # Extract parameters (everything after trigger)
        trigger_len = len(query_def.trigger)
        remaining = text[trigger_len:].strip()

        # Get required parameter names
        required_params = [p.name for p in query_def.parameters if p.required]

        if not remaining and required_params:
            # Missing parameters
            say(
                f"❌ {query_def.description}",
                channel=channel,
                thread_ts=thread_ts,
            )
            return True

        # Split remaining text into tokens (simple positional parsing)
        tokens = remaining.split()

        # Map tokens to parameters
        param_names = [p.name for p in query_def.parameters]
        params = {}

        for i, param_name in enumerate(param_names):
            if i < len(tokens):
                params[param_name] = tokens[i]

        # Get user ID
        user_id = event.get("user")

        # Execute query
        result = self.execute(query_id, params, user_id)

        # Send response
        if result["success"]:
            say(
                result["result"],
                channel=channel,
                thread_ts=thread_ts,
            )
        else:
            error_msg = result['error']
            correlation_id = result.get('correlation_id')
            if correlation_id:
                say(
                    f"❌ Error: {error_msg}\n🔍 Correlation ID: {correlation_id}",
                    channel=channel,
                    thread_ts=thread_ts,
                )
            else:
                say(
                    f"❌ Error: {error_msg}",
                    channel=channel,
                    thread_ts=thread_ts,
                )

        return True

    def _handle_list_queries(self, text, event, say, channel, thread_ts):
        """
        Handle listing available queries.

        Triggered by: @bot queries
        """
        # Remove bot mention if present (for @mentions in channels)
        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

        if not re.match(r"^queries?\s*$", text, re.IGNORECASE):
            return False

        queries = self.query_loader.get_all_queries()

        if not queries:
            say(
                "No domain queries available.",
                channel=channel,
                thread_ts=thread_ts,
            )
            return True

        # Format query list
        lines = ["📊 Available Domain Queries:", ""]
        for q in sorted(queries, key=lambda x: x.trigger):
            # Get first line of description for brevity
            desc_first_line = q.description.split('\n')[0].strip()
            lines.append(f"• `{q.trigger}` - {desc_first_line}")

        say("\n".join(lines), channel=channel, thread_ts=thread_ts)
        return True

    def _handle_clear_cache(self, text, event, say, channel, thread_ts):
        """
        Handle cache clearing.

        Triggered by:
        - @bot clear cache <query_id>
        - @bot clear cache all
        """
        # Remove bot mention if present (for @mentions in channels)
        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

        match = re.match(r"^clear\s+cache\s+(.+)$", text, re.IGNORECASE)
        if not match:
            return False

        target = match.group(1).strip()

        if target.lower() == "all":
            count = self.executor.clear_cache()
            say(
                f"✅ Cache cleared: all queries ({count} entries)",
                channel=channel,
                thread_ts=thread_ts,
            )
        else:
            # Try to find query by ID or trigger
            query_def = self.query_loader.get_query_by_id(target)
            if not query_def:
                query_def = self.query_loader.get_query_by_trigger(target)

            if not query_def:
                say(
                    f"❌ Unknown query: {target}",
                    channel=channel,
                    thread_ts=thread_ts,
                )
                return True

            count = self.executor.clear_cache(query_def.name)
            say(
                f"✅ Cache cleared for: {query_def.name} ({count} entries)",
                channel=channel,
                thread_ts=thread_ts,
            )

        return True

    def _handle_reload_queries(self, text, event, say, channel, thread_ts):
        """
        Handle reloading query definitions.

        Triggered by:
        - @bot reload queries
        """
        # Remove bot mention if present (for @mentions in channels)
        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

        if not re.match(r"^reload\s+queries?\s*$", text, re.IGNORECASE):
            return False

        try:
            # Get count before reload
            old_count = len(self.query_loader.queries)

            # Reload query definitions
            self.query_loader.reload()

            # Get count after reload
            new_count = len(self.query_loader.queries)

            # Report results
            if new_count > old_count:
                say(
                    f"✅ Queries reloaded: {new_count} queries loaded ({new_count - old_count} new)",
                    channel=channel,
                    thread_ts=thread_ts,
                )
            elif new_count < old_count:
                say(
                    f"✅ Queries reloaded: {new_count} queries loaded ({old_count - new_count} removed)",
                    channel=channel,
                    thread_ts=thread_ts,
                )
            else:
                say(
                    f"✅ Queries reloaded: {new_count} queries loaded (no changes)",
                    channel=channel,
                    thread_ts=thread_ts,
                )

            logger.info(f"Query definitions reloaded: {old_count} → {new_count}")

        except Exception as e:
            error_msg = f"❌ Failed to reload queries: {str(e)}"
            say(
                error_msg,
                channel=channel,
                thread_ts=thread_ts,
            )
            logger.error(f"Query reload failed: {e}")

        return True

    def get_help_text(self) -> str:
        """Get help text for this tool."""
        lines = []

        # Add meta-commands first
        lines.append("• `queries` - List all available SQL queries")
        lines.append("• `clear cache <query|all>` - Clear query result cache")
        lines.append("• `reload queries` - Reload query definitions")

        # Add all domain queries
        queries = self.query_loader.get_all_queries()
        for q in sorted(queries, key=lambda x: x.trigger):
            # Get first line of description for brevity
            desc_first_line = q.description.split('\n')[0].strip()
            lines.append(f"• `{q.trigger}` - {desc_first_line}")

        return "\n".join(lines)

    def check_health(self) -> dict:
        """Check tool health."""
        try:
            query_count = len(self.query_loader.queries)
            cache_count = len(self.executor.cache)

            return {
                "healthy": True,
                "details": {
                    "queries_loaded": query_count,
                    "cache_entries": cache_count,
                },
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
