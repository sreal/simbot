"""
Path resolver for blob check definitions.
Handles parameter substitution and date arithmetic.
"""
import re
import logging
from datetime import date, timedelta
from typing import Dict, Any, Optional

from .models import BlobCheckDefinition, DerivedParameter


logger = logging.getLogger(__name__)


class PathResolver:
    """Resolve path patterns with parameter substitution and date arithmetic."""

    # Pattern for {param} or {param:format}
    PARAM_PATTERN = re.compile(r'\{(\w+)(?::([^}]+))?\}')

    def resolve_all_parameters(
        self,
        blob_def: BlobCheckDefinition,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Resolve all parameters including derived ones.

        Args:
            blob_def: Blob check definition
            params: Input parameter values

        Returns:
            Dict with all resolved parameter values (input + derived)
        """
        resolved = dict(params)

        # Parse date parameters
        for param in blob_def.parameters:
            if param.type == 'date' and param.name in resolved:
                resolved[param.name] = self._parse_date(resolved[param.name])

        # Compute derived parameters
        for derived in blob_def.derived:
            if derived.from_param not in resolved:
                raise ValueError(
                    f"Derived parameter '{derived.name}' references "
                    f"unknown parameter '{derived.from_param}'"
                )

            source_value = resolved[derived.from_param]
            if not isinstance(source_value, date):
                raise ValueError(
                    f"Derived parameter '{derived.name}' requires date source, "
                    f"got {type(source_value).__name__}"
                )

            result_date = source_value

            # Apply alignment first
            if derived.align:
                result_date = self._apply_alignment(result_date, derived.align)

            # Then apply offset
            if derived.offset:
                result_date = self._apply_offset(result_date, derived.offset)

            resolved[derived.name] = result_date
            logger.debug(
                f"Derived '{derived.name}' = {result_date} "
                f"(from {derived.from_param}={source_value}, "
                f"align={derived.align}, offset={derived.offset})"
            )

        return resolved

    def resolve_path(self, pattern: str, values: Dict[str, Any]) -> str:
        """
        Replace {param} and {param:format} placeholders in path.

        Args:
            pattern: Path pattern like "{account_id}/data-{week_start:yyyy-MM-dd}.hyper"
            values: Resolved parameter values

        Returns:
            Resolved path like "X123/data-2025-04-27.hyper"
        """
        def replace_match(match):
            param_name = match.group(1)
            date_format = match.group(2)

            if param_name not in values:
                raise ValueError(f"Unknown parameter in path: {param_name}")

            value = values[param_name]

            if date_format and isinstance(value, date):
                return self._format_date(value, date_format)
            else:
                return str(value)

        return self.PARAM_PATTERN.sub(replace_match, pattern)

    def _parse_date(self, value: Any) -> date:
        """
        Parse date from string or return as-is if already a date.

        Args:
            value: Date string (YYYY-MM-DD, YYYYMMDD, etc.) or date object

        Returns:
            Python date object

        Raises:
            ValueError: If date cannot be parsed
        """
        if isinstance(value, date):
            return value

        if isinstance(value, str):
            # Try common formats
            for fmt in ['%Y-%m-%d', '%Y%m%d', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    from datetime import datetime
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue

            raise ValueError(f"Cannot parse date: {value}")

        raise ValueError(f"Invalid date type: {type(value).__name__}")

    def _apply_alignment(self, d: date, align: str) -> date:
        """
        Align date to the most recent specified day of week.

        Sunday = 6 (Python weekday)
        Monday = 0 (Python weekday)

        If date is already on target day, return as-is.

        Args:
            d: Input date
            align: Target day ('sunday' or 'monday')

        Returns:
            Date aligned to the most recent target day

        Raises:
            ValueError: If align is not 'sunday' or 'monday'
        """
        target_weekday = {'sunday': 6, 'monday': 0}.get(align.lower())
        if target_weekday is None:
            raise ValueError(f"Invalid alignment: {align}")

        current_weekday = d.weekday()

        if current_weekday == target_weekday:
            return d

        # Calculate days to subtract to reach target
        days_back = (current_weekday - target_weekday) % 7
        return d - timedelta(days=days_back)

    def _apply_offset(self, d: date, offset: str) -> date:
        """
        Apply offset like -7d, +2w, -1m to a date.

        Supported units:
        - d: days
        - w: weeks
        - m: months (approximate: 30 days)
        - y: years (approximate: 365 days)

        Args:
            d: Input date
            offset: Offset string (e.g., '-7d', '+2w', '-1m')

        Returns:
            Date with offset applied

        Raises:
            ValueError: If offset format is invalid
        """
        match = re.match(r'^([+-]?)(\d+)([dwmy])$', offset)
        if not match:
            raise ValueError(f"Invalid offset format: {offset}")

        sign = -1 if match.group(1) == '-' else 1
        amount = int(match.group(2)) * sign
        unit = match.group(3)

        if unit == 'd':
            return d + timedelta(days=amount)
        elif unit == 'w':
            return d + timedelta(weeks=amount)
        elif unit == 'm':
            # Approximate month as 30 days
            return d + timedelta(days=amount * 30)
        elif unit == 'y':
            # Approximate year as 365 days
            return d + timedelta(days=amount * 365)
        else:
            raise ValueError(f"Unknown offset unit: {unit}")

    def _format_date(self, d: date, fmt: str) -> str:
        """
        Format date using Java-style format strings.

        Supported formats:
        - yyyy: 4-digit year
        - yy: 2-digit year
        - MM: 2-digit month
        - dd: 2-digit day
        - yyyyMMdd: compact date
        - yyyy-MM-dd: ISO date

        Args:
            d: Date to format
            fmt: Java-style format string

        Returns:
            Formatted date string
        """
        # Convert Java-style format to Python strftime
        python_fmt = fmt
        python_fmt = python_fmt.replace('yyyy', '%Y')
        python_fmt = python_fmt.replace('yy', '%y')
        python_fmt = python_fmt.replace('MM', '%m')
        python_fmt = python_fmt.replace('dd', '%d')

        return d.strftime(python_fmt)
