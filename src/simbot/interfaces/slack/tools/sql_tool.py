"""Domain SQL Tool - Execute pre-defined SQL queries from YAML definitions."""

import csv
import io
import logging
import re
import uuid
from datetime import datetime
from typing import List, Dict, Optional

from simbot.tools.base import Tool
from simbot.sql_tools import (
    QueryLoader,
    QueryExecutor,
    ExecutionContext,
    QueryResult,
    CompositeQueryExecutor,
    CompositeQueryDefinition,
)


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
        self.composite_executor = CompositeQueryExecutor(self.executor, self.query_loader)
        # Set in register_handlers; needed for files_upload_v2 (the say()
        # callable from slack_bolt cannot upload files).
        self.app = None

    @staticmethod
    def _format_csv_text(rows: List[Dict]) -> str:
        """Build a CSV string (header + rows) from a list of dicts."""
        columns = list(rows[0].keys())

        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(
                [row[col] if row[col] is not None else "" for col in columns]
            )
        return output.getvalue()

    @classmethod
    def _format_csv_bytes(cls, rows: List[Dict]) -> bytes:
        """UTF-8 encoded CSV ready to hand to files_upload_v2."""
        return cls._format_csv_text(rows).encode("utf-8")

    _UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
    _NON_BASE_RE = re.compile(r"[^a-z0-9]+")
    _NON_PARAM_RE = re.compile(r"[^a-z0-9]+")

    @classmethod
    def _slug_base(cls, value: str) -> str:
        """Slug for the query name component: lowercase, non-alnum -> '_', collapse runs."""
        slug = cls._NON_BASE_RE.sub("_", value.lower()).strip("_")
        return slug or "query"

    @classmethod
    def _slug_param(cls, value) -> str:
        """Slug for one parameter value: shorten UUIDs, slug other values, cap length."""
        if value is None:
            return ""
        s = str(value).strip()
        if not s:
            return ""
        if cls._UUID_RE.match(s):
            return s.lower().split("-", 1)[0]  # first 8 hex chars
        slug = cls._NON_PARAM_RE.sub("-", s.lower()).strip("-")
        if not slug:
            return ""
        if len(slug) > 24:
            slug = slug[:24].rstrip("-")
        return slug

    @classmethod
    def _build_csv_filename(
        cls,
        base: str,
        params: dict,
        sub_label: Optional[str] = None,
    ) -> str:
        """Build a Slack-friendly CSV filename.

        Examples:
          base="version_synapse", params={"VersionID": "DBB7EC81-...-..."}
            -> 'simbot_version_synapse_dbb7ec81.csv'
          base="version_sync", params={"VersionID": "..."}, sub_label="cloud"
            -> 'simbot_version_sync_cloud_dbb7ec81.csv'
        """
        parts = ["simbot", cls._slug_base(base)]
        if sub_label:
            parts.append(cls._slug_base(sub_label))

        param_slugs = [cls._slug_param(v) for v in (params or {}).values()]
        param_slugs = [p for p in param_slugs if p]
        if param_slugs:
            parts.append("_".join(param_slugs))

        name = "_".join(p for p in parts if p) + ".csv"

        # Slack max filename length is 255 — truncate the param tail if needed.
        if len(name) > 255:
            head = "_".join(parts[:-1]) + "_"
            tail_budget = 255 - len(head) - 4  # leave room for ".csv"
            tail = parts[-1][: max(0, tail_budget)].rstrip("-_")
            name = (head + tail + ".csv") if tail else (head.rstrip("_") + ".csv")
        return name

    @staticmethod
    def _cache_annotated(comment: str, result: QueryResult) -> str:
        """Append a '(cached: ...)' suffix when the result came from cache."""
        if not result.metadata.get("from_cache"):
            return comment
        cached_at = result.metadata.get("cached_at")
        if cached_at is None:
            return f"{comment} _(cached)_"
        cached_time = datetime.fromtimestamp(cached_at).strftime("%Y-%m-%d %H:%M:%S")
        return f"{comment} _(cached: {cached_time})_"

    def _upload_csv(
        self,
        *,
        channel: str,
        thread_ts: Optional[str],
        base: str,
        params: dict,
        rows: List[Dict],
        title: str,
        comment: str,
        sub_label: Optional[str] = None,
    ) -> None:
        """Upload a CSV file to Slack with a clean filename."""
        if self.app is None:
            raise RuntimeError(
                "DomainSQLTool.app is not set; register_handlers must run before file upload."
            )
        filename = self._build_csv_filename(base, params, sub_label=sub_label)
        self.app.client.files_upload_v2(
            channel=channel,
            content=self._format_csv_text(rows),
            filename=filename,
            title=title,
            initial_comment=comment,
            thread_ts=thread_ts,
        )

    def register_handlers(self, bot):
        """Register Slack command handlers."""
        self.app = bot.app
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
            # Try composite triggers before giving up
            composite_def = self.query_loader.get_composite_by_trigger(text)
            if composite_def:
                return self._handle_composite_query(
                    composite_def, text, event, say, channel, thread_ts
                )
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

        # User-supplied params only — derived (bind_from) params are filled
        # by the executor at bind time, not by the user.
        user_params = [p for p in query_def.parameters if p.bind_from is None]
        required_params = [p.name for p in user_params if p.required]

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

        # Map tokens to user-supplied parameters
        param_names = [p.name for p in user_params]
        params = {}

        for i, param_name in enumerate(param_names):
            if i < len(tokens):
                params[param_name] = tokens[i]

        # Get user ID
        user_id = event.get("user")

        # Execute query directly so we have the raw QueryResult (data rows
        # for the CSV upload, plus metadata for the cache annotation).
        context = ExecutionContext(
            correlation_id=str(uuid.uuid4()),
            interface='slack',
            user_id=user_id,
        )
        result: QueryResult = self.executor.execute(query_def, params, context)

        if not result.success:
            error_msg = result.error
            cid = result.correlation_id
            if cid:
                say(
                    f"❌ Error: {error_msg}\n🔍 Correlation ID: {cid}",
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

        if not result.data:
            say(
                f"*{query_def.name}*: _No results_",
                channel=channel,
                thread_ts=thread_ts,
            )
            return True

        self._upload_csv(
            channel=channel,
            thread_ts=thread_ts,
            base=query_def.mcp.name if query_def.mcp else query_def.trigger,
            params=params,
            rows=result.data,
            title=query_def.name,
            comment=self._cache_annotated(f"*{query_def.name}*", result),
        )
        return True

    def _handle_composite_query(
        self,
        composite_def: CompositeQueryDefinition,
        text: str,
        event: dict,
        say,
        channel,
        thread_ts,
    ) -> bool:
        """Run a composite query and render each sub-result as its own block."""
        trigger_len = len(composite_def.trigger)
        remaining = text[trigger_len:].strip()

        # Composite-level params are user-facing; no bind_from at composite layer.
        param_names = [p.name for p in composite_def.parameters]
        required_param_names = [
            p.name for p in composite_def.parameters if p.required
        ]

        if not remaining and required_param_names:
            say(
                f"❌ {composite_def.description}",
                channel=channel,
                thread_ts=thread_ts,
            )
            return True

        tokens = remaining.split()
        params = {
            name: tokens[i]
            for i, name in enumerate(param_names)
            if i < len(tokens)
        }

        context = ExecutionContext(
            correlation_id=str(uuid.uuid4()),
            interface='slack',
            user_id=event.get("user"),
        )

        result = self.composite_executor.execute(composite_def, params, context)

        # Short header message; sub-results follow as file uploads (or text
        # fallbacks for empty/failed sub-queries).
        say(
            f"*{composite_def.name}*",
            channel=channel,
            thread_ts=thread_ts,
        )

        composite_base = (
            composite_def.mcp.name if composite_def.mcp else composite_def.trigger
        )

        for sub in composite_def.queries:
            sub_result = result.sub_results.get(sub.label)

            if sub_result is None:
                say(
                    f"*{sub.label}*: :warning: missing sub-result",
                    channel=channel,
                    thread_ts=thread_ts,
                )
                continue

            if not sub_result.success:
                err = sub_result.error or "unknown error"
                cid = sub_result.correlation_id or ""
                cid_suffix = f" (correlation: {cid})" if cid else ""
                say(
                    f"*{sub.label}*: :warning: {err}{cid_suffix}",
                    channel=channel,
                    thread_ts=thread_ts,
                )
                continue

            if not sub_result.data:
                say(
                    f"*{sub.label}*: _No results_",
                    channel=channel,
                    thread_ts=thread_ts,
                )
                continue

            self._upload_csv(
                channel=channel,
                thread_ts=thread_ts,
                base=composite_base,
                params=params,
                rows=sub_result.data,
                title=f"{composite_def.name} – {sub.label}",
                comment=self._cache_annotated(f"*{sub.label}*", sub_result),
                sub_label=sub.label,
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

        composites = self.query_loader.get_all_composites()

        # Format query list with usage
        lines = ["📊 Available Domain Queries:", ""]
        combined = list(queries) + list(composites)
        for q in sorted(combined, key=lambda x: x.trigger):
            lines.append(f"• `{self._format_usage(q)}` - {self._first_line(q.description)}")

        say("\n".join(lines), channel=channel, thread_ts=thread_ts)
        return True

    @staticmethod
    def _format_usage(definition) -> str:
        """Build '<trigger> <ParamA> [ParamB]' usage hint for a query or composite."""
        visible_params = [
            p for p in definition.parameters if getattr(p, 'bind_from', None) is None
        ]
        if not visible_params:
            return definition.trigger
        param_str = " ".join(
            f"<{p.name}>" if p.required else f"[{p.name}]"
            for p in visible_params
        )
        return f"{definition.trigger} {param_str}"

    @staticmethod
    def _first_line(text: str) -> str:
        return (text or "").split('\n')[0].strip()

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

            if query_def:
                count = self.executor.clear_cache(query_def.name)
                say(
                    f"✅ Cache cleared for: {query_def.name} ({count} entries)",
                    channel=channel,
                    thread_ts=thread_ts,
                )
                return True

            # Try composite — clear each referenced sub-query's cache
            composite_def = self.query_loader.get_composite_by_id(target)
            if not composite_def:
                composite_def = self.query_loader.get_composite_by_trigger(target)

            if composite_def:
                total = 0
                for sub in composite_def.queries:
                    sub_def = self.query_loader.get_query_by_id(sub.ref)
                    if sub_def is not None:
                        total += self.executor.clear_cache(sub_def.name)
                say(
                    f"✅ Cache cleared for composite: {composite_def.name} "
                    f"({total} entries across {len(composite_def.queries)} sub-queries)",
                    channel=channel,
                    thread_ts=thread_ts,
                )
                return True

            say(
                f"❌ Unknown query: {target}",
                channel=channel,
                thread_ts=thread_ts,
            )
            return True

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

        # Add all domain queries and composites with usage
        queries = self.query_loader.get_all_queries()
        composites = self.query_loader.get_all_composites()
        combined = list(queries) + list(composites)
        for q in sorted(combined, key=lambda x: x.trigger):
            lines.append(f"• `{self._format_usage(q)}` - {self._first_line(q.description)}")

        return "\n".join(lines)

    def check_health(self) -> dict:
        """Check tool health."""
        try:
            query_count = len(self.query_loader.queries)
            composite_count = len(self.query_loader.composites)
            cache_count = len(self.executor.cache)

            return {
                "healthy": True,
                "details": {
                    "queries_loaded": query_count,
                    "composites_loaded": composite_count,
                    "cache_entries": cache_count,
                },
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
