"""
Domain models for blob storage checks.
Provides type-safe configuration and result handling.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, field_validator, ConfigDict

# Reuse MCPConfig from sql_tools to maintain consistency
from simbot.sql_tools.models import MCPConfig


class BlobStorageConfig(BaseModel):
    """Azure blob storage connection configuration."""
    provider: Literal["azure"] = "azure"
    account_env_key: str = Field(..., description="Env var for storage account name")
    key_env_key: str = Field(..., description="Env var for storage access key")
    container: str = Field(..., description="Container name")

    @field_validator('container')
    @classmethod
    def validate_container(cls, v):
        if not v.strip():
            raise ValueError("Container name cannot be empty")
        return v.strip()


class BlobParameter(BaseModel):
    """Parameter definition for a blob check."""
    name: str
    type: Literal["string", "date"] = "string"

    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError("Parameter name cannot be empty")
        return v.strip()


class DerivedParameter(BaseModel):
    """Derived parameter with date arithmetic."""
    model_config = ConfigDict(populate_by_name=True)

    name: str
    from_param: str = Field(..., alias="from", description="Source parameter name")
    align: Optional[Literal["sunday", "monday"]] = None
    offset: Optional[str] = Field(None, description="Offset like -7d, -2w, +1d")

    @field_validator('offset')
    @classmethod
    def validate_offset(cls, v):
        if v is None:
            return v
        import re
        if not re.match(r'^[+-]?\d+[dwmy]$', v):
            raise ValueError(
                f"Invalid offset format: {v}. Use format like -7d, +2w, -1m"
            )
        return v


class FilePattern(BaseModel):
    """File pattern definition for blob check."""
    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., description="Logical name for this file")
    description: str = Field(..., description="What this file represents")
    path: str = Field(..., description="Path pattern with {param} placeholders")
    match: Literal["exact", "regex"] = Field("exact", description="How to match path")
    select: Literal["all", "latest", "oldest"] = Field(
        "all", description="For regex matches, which files to return"
    )
    return_type: Literal["metadata", "content", "both"] = Field(
        "metadata", alias="return", description="What to return for this file"
    )

    @field_validator('path')
    @classmethod
    def validate_path(cls, v):
        if not v.strip():
            raise ValueError("File path cannot be empty")
        return v.strip()


class BlobCheckDefinition(BaseModel):
    """Complete blob check definition from YAML."""
    model_config = ConfigDict(extra='forbid')

    name: str
    description: str
    trigger: str
    enabled: bool = True
    storage: BlobStorageConfig
    parameters: List[BlobParameter] = Field(default_factory=list)
    derived: List[DerivedParameter] = Field(default_factory=list)
    files: List[FilePattern]
    mcp: Optional[MCPConfig] = None

    @field_validator('files')
    @classmethod
    def validate_files(cls, v):
        if not v:
            raise ValueError("At least one file pattern is required")
        return v

    @field_validator('trigger')
    @classmethod
    def validate_trigger(cls, v):
        if not v.strip():
            raise ValueError("Trigger cannot be empty")
        return v.strip()


@dataclass
class FileResult:
    """Result for a single file in a blob check."""
    name: str
    path: str
    exists: bool
    size_bytes: Optional[int] = None
    last_modified: Optional[datetime] = None
    content_type: Optional[str] = None
    content: Optional[bytes] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict (excludes binary content)."""
        return {
            'name': self.name,
            'path': self.path,
            'exists': self.exists,
            'size_bytes': self.size_bytes,
            'last_modified': self.last_modified.isoformat() if self.last_modified else None,
            'content_type': self.content_type,
            'has_content': self.content is not None,
            'error': self.error,
        }


@dataclass
class BlobCheckResult:
    """Result of blob check execution."""
    success: bool
    definition_name: str
    files: List[FileResult]
    summary: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    error_code: Optional[str] = None
    correlation_id: Optional[str] = None

    def __post_init__(self):
        """Build summary if not provided."""
        if not self.summary:
            self.summary = self._build_summary()
        if 'executed_at' not in self.summary:
            from datetime import UTC
            self.summary['executed_at'] = datetime.now(UTC).isoformat()

    def _build_summary(self) -> Dict[str, Any]:
        """Build summary statistics from file results."""
        total = len(self.files)
        found = sum(1 for f in self.files if f.exists)
        missing = [f.name for f in self.files if not f.exists]
        total_size = sum(f.size_bytes or 0 for f in self.files if f.exists)

        modified_dates = [
            f.last_modified for f in self.files
            if f.exists and f.last_modified
        ]

        return {
            'total_files': total,
            'found': found,
            'missing_count': total - found,
            'missing_names': missing,
            'total_size_bytes': total_size,
            'oldest_file': min(modified_dates).isoformat() if modified_dates else None,
            'newest_file': max(modified_dates).isoformat() if modified_dates else None,
        }

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict (for MCP JSON responses)."""
        return {
            'success': self.success,
            'definition_name': self.definition_name,
            'files': [f.to_dict() for f in self.files],
            'summary': self.summary,
            'error': self.error,
            'error_code': self.error_code,
            'correlation_id': self.correlation_id,
        }
