"""
Convert domain query definitions to MCP tool schemas.
"""
from typing import Dict, List, Any
from simbot.sql_tools import QueryDefinition, CompositeQueryDefinition


# Tool grouping configuration
TOOL_GROUPS = {
    "account_tools": ["account_lookup", "user_lookup", "inactive_users"],
    "metric_tools": ["metric_beacon_impressions", "metric_raw_beacons"],
    "data_quality_tools": [
        "missing_aggregation",
        "report_beacon_mismatch",
        "dashboards_missing_data",
        "duplicate_accounts"
    ],
    "system_tools": ["data_source_check", "tableau_dashboards", "refresh_capacity"],
}


class YAMLToMCPConverter:
    """Converts QueryDefinition to MCP tool schema."""

    def __init__(self, tool_groups: Dict[str, List[str]] = None):
        self.tool_groups = tool_groups or TOOL_GROUPS
        # Reverse mapping: query_id -> group
        self.query_to_group = {}
        for group, queries in self.tool_groups.items():
            for query_id in queries:
                self.query_to_group[query_id] = group

    def convert(self, query_id: str, query_def: QueryDefinition) -> Dict[str, Any]:
        """
        Convert a single query to MCP tool schema.

        Args:
            query_id: Query file name (e.g., 'account_lookup')
            query_def: Validated query definition

        Returns:
            MCP tool schema dict
        """
        # Determine tool name (with group namespace)
        if query_def.mcp and query_def.mcp.name:
            tool_name = query_def.mcp.name
        else:
            # Derive from trigger: "account lookup" -> "account_lookup"
            tool_name = query_def.trigger.replace(" ", "_").lower()

        # Determine group
        if query_def.mcp and query_def.mcp.group:
            group = query_def.mcp.group
        else:
            # Fallback to config or 'ungrouped'
            group = self.query_to_group.get(query_id, 'ungrouped')

        # Namespaced tool name
        namespaced_name = f"{group}.{tool_name}"

        # Use MCP description (which includes usage and return fields info)
        # Fall back to base description if no MCP override
        if query_def.mcp and query_def.mcp.description:
            description = query_def.mcp.description
        else:
            description = query_def.description

        # Build JSON Schema for parameters
        properties = {}
        required = []

        for param in query_def.parameters:
            # Derived params (bind_from) are filled at bind time, never by the
            # MCP caller — keep them out of the schema.
            if param.bind_from is not None:
                continue

            # Map domain types to JSON Schema types
            json_type = {
                'string': 'string',
                'int': 'integer',
                'date': 'string',  # Date as ISO string
            }.get(param.type, 'string')

            properties[param.name] = {
                'type': json_type,
                'description': f"{param.name} ({param.type})"
            }

            if json_type == 'string' and param.type == 'date':
                properties[param.name]['format'] = 'date'
                properties[param.name]['description'] += ' in YYYY-MM-DD format'

            if param.required:
                required.append(param.name)

        return {
            'name': namespaced_name,
            'description': description,
            'inputSchema': {
                'type': 'object',
                'properties': properties,
                'required': required,
            },
            'metadata': {
                'group': group,
                'query_id': query_id,
                'cache_ttl': query_def.cache_ttl_seconds,
            }
        }

    def convert_all(self, queries: Dict[str, QueryDefinition]) -> List[Dict[str, Any]]:
        """Convert all queries to MCP tools."""
        tools = []
        for query_id, query_def in queries.items():
            try:
                tool_schema = self.convert(query_id, query_def)
                tools.append(tool_schema)
            except Exception as e:
                # Log but don't fail entire conversion
                import logging
                logging.error(f"Failed to convert query {query_id}: {e}")
        return tools


class CompositeToMCPConverter:
    """Converts CompositeQueryDefinition to MCP tool schema.

    Schema is built from the composite's own user-facing parameters; sub-query
    parameters are not exposed (they are filled by the composite executor via
    the YAML's params: mapping).
    """

    def convert(self, composite_id: str, composite_def: CompositeQueryDefinition) -> Dict[str, Any]:
        if composite_def.mcp and composite_def.mcp.name:
            tool_name = composite_def.mcp.name
        else:
            tool_name = composite_def.trigger.replace(" ", "_").lower()

        group = (composite_def.mcp.group if composite_def.mcp else None) or 'ungrouped'

        namespaced_name = f"{group}.{tool_name}"

        if composite_def.mcp and composite_def.mcp.description:
            description = composite_def.mcp.description
        else:
            description = composite_def.description

        properties = {}
        required = []

        for param in composite_def.parameters:
            json_type = {
                'string': 'string',
                'int': 'integer',
                'date': 'string',
            }.get(param.type, 'string')

            properties[param.name] = {
                'type': json_type,
                'description': f"{param.name} ({param.type})",
            }

            if json_type == 'string' and param.type == 'date':
                properties[param.name]['format'] = 'date'
                properties[param.name]['description'] += ' in YYYY-MM-DD format'

            if param.required:
                required.append(param.name)

        return {
            'name': namespaced_name,
            'description': description,
            'inputSchema': {
                'type': 'object',
                'properties': properties,
                'required': required,
            },
            'metadata': {
                'group': group,
                'composite_id': composite_id,
                'cache_ttl': composite_def.cache_ttl_seconds,
            },
        }

    def convert_all(self, composites: Dict[str, CompositeQueryDefinition]) -> List[Dict[str, Any]]:
        tools = []
        for composite_id, composite_def in composites.items():
            try:
                tools.append(self.convert(composite_id, composite_def))
            except Exception as e:
                import logging
                logging.error(f"Failed to convert composite {composite_id}: {e}")
        return tools
