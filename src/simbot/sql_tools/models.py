"""
Domain models for SQL query execution.
Provides type-safe configuration and result handling.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict


class QueryParameter(BaseModel):
    """Parameter definition for a query."""
    name: str
    type: str = Field(..., description="string, int, or date")
    required: bool = True

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        if v not in ['string', 'int', 'date']:
            raise ValueError(f"Invalid parameter type: {v}")
        return v


class MCPConfig(BaseModel):
    """Optional MCP-specific configuration."""
    name: str = Field(..., description="MCP tool name (snake_case)")
    group: str = Field(..., description="Tool group for categorization")
    description: Optional[str] = Field(None, description="Override for MCP description")

    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        if ' ' in v:
            raise ValueError("MCP tool name cannot contain spaces")
        return v


class QueryDefinition(BaseModel):
    """Complete query definition from YAML."""
    model_config = ConfigDict(extra='forbid')  # Catch typos in YAML

    name: str
    description: str
    trigger: str
    enabled: bool = True
    database: str
    credentials_env_key: str
    sql: str
    parameters: List[QueryParameter] = Field(default_factory=list)
    cache_ttl_seconds: int = 0
    mcp: Optional[MCPConfig] = None

    @field_validator('sql')
    @classmethod
    def validate_sql(cls, v):
        if not v.strip():
            raise ValueError("SQL cannot be empty")
        return v.strip()


@dataclass
class QueryResult:
    """
    Result of query execution.
    Used across all interfaces to maintain consistent error handling.
    """
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None

    def __post_init__(self):
        """Ensure metadata includes execution timing."""
        if 'executed_at' not in self.metadata:
            from datetime import datetime, UTC
            self.metadata['executed_at'] = datetime.now(UTC).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict (for MCP JSON responses)."""
        return {
            'success': self.success,
            'data': self.data,
            'error': self.error,
            'error_code': self.error_code,
            'metadata': self.metadata,
            'correlation_id': self.correlation_id,
        }


@dataclass
class ExecutionContext:
    """Context passed through execution layers."""
    correlation_id: str
    interface: str  # 'slack' or 'mcp'
    user_id: Optional[str] = None  # Interface-specific user identifier

    def __str__(self):
        return f"[{self.correlation_id}] {self.interface}:{self.user_id or 'unknown'}"
