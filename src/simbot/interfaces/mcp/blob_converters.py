"""
Convert blob check definitions to MCP tool schemas.
"""
from typing import Dict, List, Any
from simbot.blob_tools import BlobCheckDefinition


# Default tool grouping for blob checks
BLOB_TOOL_GROUPS = {
    "check_tools": ["blob_check_pipeline", "blob_check_export"],
    "health_tools": ["blob_check_config"],
}


class BlobToMCPConverter:
    """Converts BlobCheckDefinition to MCP tool schema."""

    def __init__(self, tool_groups: Dict[str, List[str]] = None):
        self.tool_groups = tool_groups or BLOB_TOOL_GROUPS
        # Reverse mapping: check_id -> group
        self.check_to_group = {}
        for group, checks in self.tool_groups.items():
            for check_id in checks:
                self.check_to_group[check_id] = group

    def convert(self, check_id: str, check_def: BlobCheckDefinition) -> Dict[str, Any]:
        """
        Convert a single blob check to MCP tool schema.

        Args:
            check_id: Check file name (e.g., 'blob_check_pipeline')
            check_def: Validated blob check definition

        Returns:
            MCP tool schema dict
        """
        # Determine tool name (with group namespace)
        if check_def.mcp and check_def.mcp.name:
            tool_name = check_def.mcp.name
        else:
            # Derive from trigger: "check pipeline" -> "check_pipeline"
            tool_name = check_def.trigger.replace(" ", "_").lower()

        # Determine group
        if check_def.mcp and check_def.mcp.group:
            group = check_def.mcp.group
        else:
            # Fallback to config or 'ungrouped'
            group = self.check_to_group.get(check_id, 'ungrouped')

        # Namespaced tool name
        namespaced_name = f"{group}.{tool_name}"

        # Use MCP description or base description
        if check_def.mcp and check_def.mcp.description:
            description = check_def.mcp.description
        else:
            description = check_def.description

        # Build JSON Schema for parameters
        properties = {}
        required = []

        for param in check_def.parameters:
            # Map domain types to JSON Schema types
            json_type = {
                'string': 'string',
                'date': 'string',
            }.get(param.type, 'string')

            properties[param.name] = {
                'type': json_type,
                'description': f"{param.name} ({param.type})"
            }

            if json_type == 'string' and param.type == 'date':
                properties[param.name]['format'] = 'date'
                properties[param.name]['description'] += ' in YYYY-MM-DD format'

            # All blob check parameters are required (no required field in model)
            required.append(param.name)

        # Build file list info for description
        file_info = []
        for fp in check_def.files:
            file_info.append(f"- {fp.name}: {fp.description}")

        if file_info:
            description = f"{description}\n\nFiles checked:\n" + "\n".join(file_info)

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
                'check_id': check_id,
                'file_count': len(check_def.files),
            }
        }

    def convert_all(self, checks: Dict[str, BlobCheckDefinition]) -> List[Dict[str, Any]]:
        """Convert all blob checks to MCP tools."""
        tools = []
        for check_id, check_def in checks.items():
            try:
                tool_schema = self.convert(check_id, check_def)
                tools.append(tool_schema)
            except Exception as e:
                import logging
                logging.error(f"Failed to convert blob check {check_id}: {e}")
        return tools
