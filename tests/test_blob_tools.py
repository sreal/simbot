"""Tests for simbot.blob_tools module."""

import os
import pytest
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
from pydantic import ValidationError

from simbot.blob_tools import (
    BlobCheckLoader,
    BlobCheckExecutor,
    BlobCheckDefinition,
    BlobCheckResult,
    FileResult,
    FilePattern,
    BlobParameter,
    DerivedParameter,
    BlobStorageConfig,
    PathResolver,
)


# Sample YAML content for testing
SAMPLE_BLOB_CHECK_YAML = """
name: "Test Pipeline Check"
description: "A test blob check for unit testing"
trigger: "check test"
enabled: true

storage:
  provider: "azure"
  account_env_key: "AZURE_STORAGE_ACCOUNT"
  key_env_key: "AZURE_STORAGE_KEY"
  container: "test-container"

parameters:
  - name: account_id
    type: string

derived: []

files:
  - name: config
    description: "Config file"
    path: "{account_id}/config.json"
    return: metadata
"""

SAMPLE_BLOB_CHECK_WITH_DERIVED_YAML = """
name: "Test Export Check"
description: "A test blob check with derived parameters"
trigger: "check export"
enabled: true

storage:
  provider: "azure"
  account_env_key: "AZURE_STORAGE_ACCOUNT"
  key_env_key: "AZURE_STORAGE_KEY"
  container: "exports"

parameters:
  - name: account_id
    type: string
  - name: report_date
    type: date

derived:
  - name: week_start
    from: report_date
    align: sunday
  - name: prior_week
    from: report_date
    align: sunday
    offset: "-7d"

files:
  - name: snapshot
    description: "Weekly snapshot"
    path: "{account_id}/snapshot-{week_start:yyyy-MM-dd}.hyper"
    return: metadata
  - name: source_data
    description: "Source data"
    path: "{account_id}/data-{prior_week:yyyyMMdd}.hyper"
    return: content
"""

DISABLED_BLOB_CHECK_YAML = """
name: "Disabled Check"
description: "This check is disabled"
trigger: "check disabled"
enabled: false

storage:
  provider: "azure"
  account_env_key: "AZURE_STORAGE_ACCOUNT"
  key_env_key: "AZURE_STORAGE_KEY"
  container: "test"

parameters: []
derived: []

files:
  - name: test
    description: "Test file"
    path: "test.txt"
    return: metadata
"""


@pytest.fixture
def temp_checks_dir():
    """Create a temporary directory with test blob check YAML files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Write test YAML files (must start with blob_)
        (Path(tmpdir) / "blob_test_pipeline.yaml").write_text(SAMPLE_BLOB_CHECK_YAML)
        (Path(tmpdir) / "blob_test_export.yaml").write_text(SAMPLE_BLOB_CHECK_WITH_DERIVED_YAML)
        (Path(tmpdir) / "blob_disabled.yaml").write_text(DISABLED_BLOB_CHECK_YAML)
        yield tmpdir


class TestBlobCheckLoader:
    """Tests for BlobCheckLoader."""

    def test_load_checks_from_directory(self, temp_checks_dir):
        """Test loading blob checks from directory."""
        loader = BlobCheckLoader(definitions_dir=temp_checks_dir)
        # Should load 2 enabled checks (disabled one is skipped)
        assert len(loader.checks) == 2
        assert "blob_test_pipeline" in loader.checks
        assert "blob_test_export" in loader.checks
        assert "blob_disabled" not in loader.checks

    def test_get_by_id(self, temp_checks_dir):
        """Test getting check by ID."""
        loader = BlobCheckLoader(definitions_dir=temp_checks_dir)
        check = loader.get_by_id("blob_test_pipeline")
        assert check is not None
        assert check.name == "Test Pipeline Check"

    def test_get_by_id_not_found(self, temp_checks_dir):
        """Test getting non-existent check returns None."""
        loader = BlobCheckLoader(definitions_dir=temp_checks_dir)
        check = loader.get_by_id("nonexistent")
        assert check is None

    def test_get_by_trigger(self, temp_checks_dir):
        """Test finding check by trigger phrase."""
        loader = BlobCheckLoader(definitions_dir=temp_checks_dir)
        check = loader.get_by_trigger("check test account123")
        assert check is not None
        assert check.trigger == "check test"

    def test_get_by_trigger_not_found(self, temp_checks_dir):
        """Test trigger not found returns None."""
        loader = BlobCheckLoader(definitions_dir=temp_checks_dir)
        check = loader.get_by_trigger("nonexistent trigger")
        assert check is None

    def test_get_all(self, temp_checks_dir):
        """Test getting all checks."""
        loader = BlobCheckLoader(definitions_dir=temp_checks_dir)
        checks = loader.get_all()
        assert len(checks) == 2

    def test_reload(self, temp_checks_dir):
        """Test hot-reload functionality."""
        loader = BlobCheckLoader(definitions_dir=temp_checks_dir)
        initial_count = len(loader.checks)
        loader.reload()
        assert len(loader.checks) == initial_count

    def test_requires_definitions_dir(self):
        """Test that definitions_dir is required."""
        with patch.dict(os.environ, {}, clear=True):
            if "QUERY_DEFINITIONS_PATH" in os.environ:
                del os.environ["QUERY_DEFINITIONS_PATH"]
            with pytest.raises(ValueError, match="definitions_dir must be provided"):
                BlobCheckLoader()


class TestPathResolver:
    """Tests for PathResolver."""

    def test_resolve_simple_path(self):
        """Test simple parameter substitution."""
        resolver = PathResolver()
        result = resolver.resolve_path(
            "{account_id}/config.json",
            {"account_id": "ABC123"}
        )
        assert result == "ABC123/config.json"

    def test_resolve_path_with_date_format(self):
        """Test date formatting in path."""
        resolver = PathResolver()
        result = resolver.resolve_path(
            "{account_id}/data-{report_date:yyyy-MM-dd}.hyper",
            {"account_id": "ABC", "report_date": date(2025, 5, 1)}
        )
        assert result == "ABC/data-2025-05-01.hyper"

    def test_resolve_path_with_compact_date(self):
        """Test compact date format."""
        resolver = PathResolver()
        result = resolver.resolve_path(
            "data-{report_date:yyyyMMdd}.hyper",
            {"report_date": date(2025, 5, 1)}
        )
        assert result == "data-20250501.hyper"

    def test_apply_alignment_sunday(self):
        """Test aligning to Sunday."""
        resolver = PathResolver()
        # Wednesday May 7, 2025 should align to Sunday May 4, 2025
        result = resolver._apply_alignment(date(2025, 5, 7), "sunday")
        assert result == date(2025, 5, 4)

    def test_apply_alignment_monday(self):
        """Test aligning to Monday."""
        resolver = PathResolver()
        # Wednesday May 7, 2025 should align to Monday May 5, 2025
        result = resolver._apply_alignment(date(2025, 5, 7), "monday")
        assert result == date(2025, 5, 5)

    def test_apply_alignment_already_on_target(self):
        """Test alignment when already on target day."""
        resolver = PathResolver()
        # Sunday May 4, 2025 should remain May 4, 2025
        result = resolver._apply_alignment(date(2025, 5, 4), "sunday")
        assert result == date(2025, 5, 4)

    def test_apply_offset_days(self):
        """Test day offset."""
        resolver = PathResolver()
        result = resolver._apply_offset(date(2025, 5, 10), "-7d")
        assert result == date(2025, 5, 3)

    def test_apply_offset_weeks(self):
        """Test week offset."""
        resolver = PathResolver()
        result = resolver._apply_offset(date(2025, 5, 10), "-2w")
        assert result == date(2025, 4, 26)

    def test_apply_offset_positive(self):
        """Test positive offset."""
        resolver = PathResolver()
        result = resolver._apply_offset(date(2025, 5, 1), "+7d")
        assert result == date(2025, 5, 8)

    def test_resolve_all_parameters_with_derived(self):
        """Test resolving all parameters including derived."""
        resolver = PathResolver()

        check_def = BlobCheckDefinition(
            name="Test",
            description="Test",
            trigger="test",
            storage=BlobStorageConfig(
                account_env_key="AZURE_STORAGE_ACCOUNT",
                key_env_key="AZURE_STORAGE_KEY",
                container="test"
            ),
            parameters=[
                BlobParameter(name="account_id", type="string"),
                BlobParameter(name="report_date", type="date"),
            ],
            derived=[
                DerivedParameter(name="week_start", from_param="report_date", align="sunday"),
                DerivedParameter(name="prior_week", from_param="report_date", align="sunday", offset="-7d"),
            ],
            files=[
                FilePattern(name="test", description="Test", path="test.txt")
            ]
        )

        params = {"account_id": "ABC", "report_date": "2025-05-07"}
        resolved = resolver.resolve_all_parameters(check_def, params)

        assert resolved["account_id"] == "ABC"
        assert resolved["report_date"] == date(2025, 5, 7)
        assert resolved["week_start"] == date(2025, 5, 4)  # Sunday
        assert resolved["prior_week"] == date(2025, 4, 27)  # Sunday - 7 days

    def test_parse_date_iso_format(self):
        """Test parsing ISO date format."""
        resolver = PathResolver()
        result = resolver._parse_date("2025-05-01")
        assert result == date(2025, 5, 1)

    def test_parse_date_compact_format(self):
        """Test parsing compact date format."""
        resolver = PathResolver()
        result = resolver._parse_date("20250501")
        assert result == date(2025, 5, 1)


class TestBlobCheckDefinition:
    """Tests for BlobCheckDefinition model."""

    def test_valid_definition(self):
        """Test creating valid blob check definition."""
        check_def = BlobCheckDefinition(
            name="Test Check",
            description="Test description",
            trigger="check test",
            storage=BlobStorageConfig(
                account_env_key="AZURE_STORAGE_ACCOUNT",
                key_env_key="AZURE_STORAGE_KEY",
                container="test-container"
            ),
            parameters=[BlobParameter(name="account_id", type="string")],
            derived=[],
            files=[FilePattern(name="config", description="Config", path="{account_id}/config.json")],
        )
        assert check_def.name == "Test Check"
        assert check_def.enabled is True
        assert len(check_def.files) == 1

    def test_requires_at_least_one_file(self):
        """Test validation requires at least one file pattern."""
        with pytest.raises(ValidationError):
            BlobCheckDefinition(
                name="Test",
                description="Test",
                trigger="test",
                storage=BlobStorageConfig(
                    account_env_key="AZURE_STORAGE_ACCOUNT",
                    key_env_key="AZURE_STORAGE_KEY",
                    container="test"
                ),
                parameters=[],
                derived=[],
                files=[],  # Empty - should fail
            )

    def test_invalid_offset_format(self):
        """Test validation rejects invalid offset format."""
        with pytest.raises(ValidationError):
            DerivedParameter(
                name="test",
                from_param="source",
                offset="invalid"
            )

    def test_valid_offset_formats(self):
        """Test valid offset formats are accepted."""
        # These should all work
        DerivedParameter(name="t1", from_param="s", offset="-7d")
        DerivedParameter(name="t2", from_param="s", offset="+2w")
        DerivedParameter(name="t3", from_param="s", offset="-1m")
        DerivedParameter(name="t4", from_param="s", offset="14d")


class TestFileResult:
    """Tests for FileResult dataclass."""

    def test_exists_result(self):
        """Test creating result for existing file."""
        from datetime import datetime
        result = FileResult(
            name="config",
            path="ABC/config.json",
            exists=True,
            size_bytes=1024,
            last_modified=datetime(2025, 5, 1, 10, 30),
            content_type="application/json",
        )
        assert result.exists is True
        assert result.size_bytes == 1024

    def test_not_exists_result(self):
        """Test creating result for non-existing file."""
        result = FileResult(
            name="missing",
            path="ABC/missing.json",
            exists=False,
        )
        assert result.exists is False
        assert result.size_bytes is None

    def test_to_dict(self):
        """Test serialization to dict."""
        from datetime import datetime
        result = FileResult(
            name="config",
            path="ABC/config.json",
            exists=True,
            size_bytes=1024,
            last_modified=datetime(2025, 5, 1, 10, 30),
            content=b"test content",
        )
        d = result.to_dict()
        assert d["exists"] is True
        assert d["has_content"] is True
        assert "content" not in d  # Binary content excluded


class TestBlobCheckResult:
    """Tests for BlobCheckResult dataclass."""

    def test_success_result(self):
        """Test creating success result."""
        from datetime import datetime
        files = [
            FileResult(name="f1", path="p1", exists=True, size_bytes=100,
                      last_modified=datetime(2025, 5, 1)),
            FileResult(name="f2", path="p2", exists=True, size_bytes=200,
                      last_modified=datetime(2025, 5, 2)),
        ]
        result = BlobCheckResult(
            success=True,
            definition_name="Test Check",
            files=files,
            correlation_id="abc123",
        )
        assert result.success is True
        assert result.summary["total_files"] == 2
        assert result.summary["found"] == 2
        assert result.summary["total_size_bytes"] == 300

    def test_result_with_missing_files(self):
        """Test result with some missing files."""
        files = [
            FileResult(name="found", path="p1", exists=True, size_bytes=100),
            FileResult(name="missing", path="p2", exists=False),
        ]
        result = BlobCheckResult(
            success=True,
            definition_name="Test Check",
            files=files,
            correlation_id="abc123",
        )
        assert result.summary["found"] == 1
        assert result.summary["missing_count"] == 1
        assert "missing" in result.summary["missing_names"]

    def test_to_dict(self):
        """Test serialization to dict."""
        files = [FileResult(name="f1", path="p1", exists=True)]
        result = BlobCheckResult(
            success=True,
            definition_name="Test",
            files=files,
            correlation_id="abc123",
        )
        d = result.to_dict()
        assert d["success"] is True
        assert d["definition_name"] == "Test"
        assert len(d["files"]) == 1


class TestBlobCheckExecutor:
    """Tests for BlobCheckExecutor."""

    def test_extract_prefix(self):
        """Test extracting static prefix from regex pattern."""
        executor = BlobCheckExecutor()

        # Simple path - no regex
        assert executor._extract_prefix("ABC/config.json") == "ABC/config.json"

        # Path with regex at end
        assert executor._extract_prefix("ABC/output/.*\\.parquet") == "ABC/output/"

        # Path with regex in middle
        assert executor._extract_prefix("ABC/.*/config.json") == "ABC/"

    @patch("simbot.blob_tools.executor.AZURE_AVAILABLE", False)
    def test_execute_without_azure_sdk(self, temp_checks_dir):
        """Test graceful handling when azure SDK unavailable."""
        loader = BlobCheckLoader(definitions_dir=temp_checks_dir)
        executor = BlobCheckExecutor()

        check_def = loader.get_by_id("blob_test_pipeline")
        from simbot.sql_tools.models import ExecutionContext
        context = ExecutionContext(correlation_id="test-123", interface="test")

        result = executor.execute(check_def, {"account_id": "TEST"}, context)
        assert result.success is False
        assert "azure" in result.error.lower()
