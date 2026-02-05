"""Domain Blob Tool - Execute pre-defined blob storage checks from YAML definitions."""

import logging
import re
import uuid
from typing import List, Dict, Optional

from simbot.tools.base import Tool
from simbot.sql_tools.models import ExecutionContext
from simbot.blob_tools import (
    BlobCheckLoader,
    BlobCheckExecutor,
    BlobCheckResult,
    FileResult,
)


logger = logging.getLogger(__name__)


class DomainBlobTool(Tool):
    """Execute blob storage checks for diagnostics."""

    def __init__(self):
        super().__init__(
            name="domain_blob",
            description="Execute pre-defined blob storage checks for diagnostics",
        )
        self.loader = BlobCheckLoader()
        self.executor = BlobCheckExecutor()

    def execute(self, check_id: str, params: dict, user_id: str = None) -> dict:
        """
        Execute blob check.

        Args:
            check_id: Blob check identifier
            params: Parameter values
            user_id: User ID for audit logging

        Returns:
            {
                'success': bool,
                'result': formatted result string,
                'files_with_content': list of FileResult with content,
                'error': str | None,
                'metadata': {...}
            }
        """
        check_def = self.loader.get_by_id(check_id)
        if not check_def:
            return {
                "success": False,
                "error": f"Unknown blob check: {check_id}",
                "metadata": {},
            }

        # Create execution context
        context = ExecutionContext(
            correlation_id=str(uuid.uuid4()),
            interface='slack',
            user_id=user_id
        )

        # Execute check
        result: BlobCheckResult = self.executor.execute(check_def, params, context)

        # Format result for display
        if result.success:
            formatted = self._format_result(result, params)
            files_with_content = [
                f for f in result.files
                if f.content is not None
            ]
            return {
                "success": True,
                "result": formatted,
                "files_with_content": files_with_content,
                "metadata": result.summary,
                "correlation_id": result.correlation_id,
            }
        else:
            return {
                "success": False,
                "error": result.error,
                "metadata": result.summary,
                "correlation_id": result.correlation_id,
            }

    def _format_result(self, result: BlobCheckResult, params: dict) -> str:
        """
        Format blob check result for Slack display.

        Args:
            result: Blob check result with files and summary
            params: Parameter values used in the check

        Returns:
            Formatted string for Slack message
        """
        lines = []

        # Header
        if params:
            param_display = ", ".join(f"{k}=`{v}`" for k, v in params.items())
            lines.append(f"**{result.definition_name}** ({param_display})")
        else:
            lines.append(f"**{result.definition_name}**")

        # Summary line
        summary = result.summary
        found = summary.get('found', 0)
        total = summary.get('total_files', 0)
        total_size = summary.get('total_size_bytes', 0)

        size_display = self._format_size(total_size)
        lines.append(f"Files: {found}/{total} found | Total size: {size_display}")
        lines.append("")

        # File table
        lines.append(self._format_file_table(result.files))

        # Missing files warning
        missing = summary.get('missing_names', [])
        if missing:
            lines.append("")
            lines.append(f"⚠️ Missing: {', '.join(missing)}")

        return "\n".join(lines)

    def _format_file_table(self, files: List[FileResult]) -> str:
        """
        Format files as ASCII table.

        Args:
            files: List of file results to display

        Returns:
            ASCII table wrapped in markdown code block
        """
        if not files:
            return "No files"

        # Define columns
        columns = ["Name", "Status", "Size", "Modified"]

        # Build rows
        rows = []
        for f in files:
            status = "✓" if f.exists else "✗"
            size = self._format_size(f.size_bytes) if f.size_bytes else "-"
            modified = (
                f.last_modified.strftime("%Y-%m-%d %H:%M")
                if f.last_modified else "-"
            )
            rows.append({
                "Name": f.name,
                "Status": status,
                "Size": size,
                "Modified": modified,
            })

        # Calculate column widths
        widths = {col: len(col) for col in columns}
        for row in rows:
            for col in columns:
                widths[col] = max(widths[col], len(str(row[col])))

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
            values = [str(row[col]).ljust(widths[col]) for col in columns]
            lines.append("| " + " | ".join(values) + " |")

        lines.append(separator)

        table = "\n".join(lines)
        return f"```\n{table}\n```"

    def _format_size(self, size_bytes: Optional[int]) -> str:
        """
        Format byte size for display.

        Args:
            size_bytes: Size in bytes, or None

        Returns:
            Human-readable size string (e.g., '1.5 MB')
        """
        if size_bytes is None:
            return "-"

        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / 1024 / 1024:.1f} MB"
        else:
            return f"{size_bytes / 1024 / 1024 / 1024:.1f} GB"

    def register_handlers(self, bot):
        """Register Slack command handlers."""
        bot.command_handlers["blob_check"] = self._handle_check
        bot.command_handlers["blob_checks_list"] = self._handle_list_checks
        bot.command_handlers["blob_checks_reload"] = self._handle_reload_checks

    def _handle_check(self, text, event, say, channel, thread_ts):
        """
        Handle blob check execution.

        Parses messages like:
        - @bot check pipeline account123
        - @bot check weekly_export account123 2025-05-01

        Args:
            text: Message text
            event: Slack event
            say: Slack say function
            channel: Channel ID
            thread_ts: Thread timestamp

        Returns:
            True if command was handled, False otherwise
        """
        # Remove bot mention if present
        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

        # Try to match a check trigger
        check_def = self.loader.get_by_trigger(text)

        if not check_def:
            return False  # Not a blob check

        # Find check_id from loader
        check_id = None
        for cid, cdef in self.loader.checks.items():
            if cdef == check_def:
                check_id = cid
                break

        if not check_id:
            logger.error(f"Could not find check_id for check: {check_def.name}")
            return False

        # Extract parameters (everything after trigger)
        trigger_len = len(check_def.trigger)
        remaining = text[trigger_len:].strip()

        # Get parameter names
        param_names = [p.name for p in check_def.parameters]

        if not remaining and param_names:
            # Missing parameters - show usage
            param_list = " ".join(f"<{p}>" for p in param_names)
            say(
                f"❌ Usage: `{check_def.trigger} {param_list}`\n{check_def.description}",
                channel=channel,
                thread_ts=thread_ts,
            )
            return True

        # Split remaining text into tokens
        tokens = remaining.split()

        # Map tokens to parameters
        params = {}
        for i, param_name in enumerate(param_names):
            if i < len(tokens):
                params[param_name] = tokens[i]

        # Get user ID
        user_id = event.get("user")

        # Execute check
        result = self.execute(check_id, params, user_id)

        # Send response
        if result["success"]:
            say(
                result["result"],
                channel=channel,
                thread_ts=thread_ts,
            )

            # Upload any files with content
            files_with_content = result.get("files_with_content", [])
            if files_with_content:
                self._upload_files(files_with_content, say, channel, thread_ts)
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

    def _upload_files(self, files: List[FileResult], say, channel, thread_ts):
        """
        Upload files with content to Slack.

        Args:
            files: List of file results with content
            say: Slack say function
            channel: Channel ID
            thread_ts: Thread timestamp
        """
        # Note: This requires the Slack client to be available
        # The bot.py passes the say function, but we need files_upload_v2
        # For now, we'll indicate that files are available
        for f in files:
            if f.content:
                size = self._format_size(len(f.content))
                say(
                    f"📎 File available: `{f.path}` ({size})",
                    channel=channel,
                    thread_ts=thread_ts,
                )

    def _handle_list_checks(self, text, event, say, channel, thread_ts):
        """
        Handle listing available blob checks.

        Triggered by: @bot blob checks

        Args:
            text: Message text
            event: Slack event
            say: Slack say function
            channel: Channel ID
            thread_ts: Thread timestamp

        Returns:
            True if command was handled, False otherwise
        """
        # Remove bot mention if present
        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

        if not re.match(r"^blob\s+checks?\s*$", text, re.IGNORECASE):
            return False

        checks = self.loader.get_all()

        if not checks:
            say(
                "No blob checks available.",
                channel=channel,
                thread_ts=thread_ts,
            )
            return True

        # Format check list
        lines = ["📦 Available Blob Checks:", ""]
        for c in sorted(checks, key=lambda x: x.trigger):
            desc_first_line = c.description.split('\n')[0].strip()
            lines.append(f"• `{c.trigger}` - {desc_first_line}")

        say("\n".join(lines), channel=channel, thread_ts=thread_ts)
        return True

    def _handle_reload_checks(self, text, event, say, channel, thread_ts):
        """
        Handle reloading blob check definitions.

        Triggered by: @bot reload blob checks

        Args:
            text: Message text
            event: Slack event
            say: Slack say function
            channel: Channel ID
            thread_ts: Thread timestamp

        Returns:
            True if command was handled, False otherwise
        """
        # Remove bot mention if present
        text = re.sub(r'<@[A-Z0-9]+>', '', text).strip()

        if not re.match(r"^reload\s+blob\s+checks?\s*$", text, re.IGNORECASE):
            return False

        try:
            old_count = len(self.loader.checks)
            self.loader.reload()
            new_count = len(self.loader.checks)

            if new_count > old_count:
                say(
                    f"✅ Blob checks reloaded: {new_count} checks ({new_count - old_count} new)",
                    channel=channel,
                    thread_ts=thread_ts,
                )
            elif new_count < old_count:
                say(
                    f"✅ Blob checks reloaded: {new_count} checks ({old_count - new_count} removed)",
                    channel=channel,
                    thread_ts=thread_ts,
                )
            else:
                say(
                    f"✅ Blob checks reloaded: {new_count} checks (no changes)",
                    channel=channel,
                    thread_ts=thread_ts,
                )

            logger.info(f"Blob check definitions reloaded: {old_count} → {new_count}")

        except Exception as e:
            say(
                f"❌ Failed to reload blob checks: {str(e)}",
                channel=channel,
                thread_ts=thread_ts,
            )
            logger.error(f"Blob check reload failed: {e}")

        return True

    def get_help_text(self) -> str:
        """Get help text for this tool."""
        lines = []

        # Meta-commands
        lines.append("• `blob checks` - List all available blob checks")
        lines.append("• `reload blob checks` - Reload blob check definitions")

        # All blob checks
        checks = self.loader.get_all()
        for c in sorted(checks, key=lambda x: x.trigger):
            desc_first_line = c.description.split('\n')[0].strip()
            lines.append(f"• `{c.trigger}` - {desc_first_line}")

        return "\n".join(lines)

    def check_health(self) -> dict:
        """Check tool health."""
        try:
            check_count = len(self.loader.checks)

            return {
                "healthy": True,
                "details": {
                    "checks_loaded": check_count,
                },
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
