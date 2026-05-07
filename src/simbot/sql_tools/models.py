"""
Domain models for SQL query execution.
Provides type-safe configuration and result handling.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


class QueryParameter(BaseModel):
    """Parameter definition for a query."""
    name: str
    type: str = Field(..., description="string, int, or date")
    required: bool = True
    bind_from: Optional[str] = Field(
        None,
        description=(
            "If set, the parameter takes its value from another parameter at "
            "bind time instead of being supplied by the caller. Use this to "
            "reuse a single user input across multiple ? placeholders."
        ),
    )

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

    @model_validator(mode='after')
    def validate_bind_from_references(self):
        names = {p.name for p in self.parameters}
        derived = {p.name for p in self.parameters if p.bind_from is not None}
        for p in self.parameters:
            if p.bind_from is None:
                continue
            if p.bind_from == p.name:
                raise ValueError(
                    f"Parameter '{p.name}' has bind_from referencing itself"
                )
            if p.bind_from not in names:
                raise ValueError(
                    f"Parameter '{p.name}' has bind_from='{p.bind_from}' "
                    f"but no parameter with that name is defined"
                )
            if p.bind_from in derived:
                raise ValueError(
                    f"Parameter '{p.name}' bind_from='{p.bind_from}' points to "
                    f"another bind_from parameter; chains are not supported"
                )
        return self


class CompositeSubQuery(BaseModel):
    """One referenced sub-query inside a composite definition."""
    model_config = ConfigDict(extra='forbid')

    label: str = Field(..., description="Unique key inside the composite; appears in MCP JSON and Slack section heading")
    ref: str = Field(..., description="Query id (YAML stem) of an existing SQL query")
    params: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from referenced query's parameter names to template strings. Use '{Name}' to substitute a composite-level parameter.",
    )


class CompositeQueryDefinition(BaseModel):
    """Composite query: fans out to multiple referenced SQL queries."""
    model_config = ConfigDict(extra='forbid')

    name: str
    description: str
    trigger: str
    enabled: bool = True
    type: Literal["composite"]
    parameters: List[QueryParameter] = Field(default_factory=list)
    queries: List[CompositeSubQuery]
    cache_ttl_seconds: int = 0
    mcp: Optional[MCPConfig] = None

    @model_validator(mode='after')
    def validate_unique_labels(self):
        labels = [q.label for q in self.queries]
        if len(labels) != len(set(labels)):
            raise ValueError("Composite sub-query labels must be unique")
        return self

    @model_validator(mode='after')
    def validate_at_least_one_subquery(self):
        if not self.queries:
            raise ValueError("Composite must reference at least one sub-query")
        return self


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
class CompositeQueryResult:
    """Result of composite execution: one QueryResult per labelled sub-query."""
    success: bool
    composite_name: str
    sub_results: Dict[str, QueryResult] = field(default_factory=dict)
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'composite_name': self.composite_name,
            'sub_results': {k: v.to_dict() for k, v in self.sub_results.items()},
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
