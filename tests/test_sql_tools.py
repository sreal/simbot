"""Tests for simbot.sql_tools module."""

import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch
from pydantic import ValidationError

from simbot.sql_tools import (
    QueryLoader,
    QueryExecutor,
    QueryDefinition,
    QueryResult,
    ExecutionContext,
    QueryParameter,
)


# Sample YAML content for testing
SAMPLE_QUERY_YAML = """
name: "Test Query"
description: "A test query for unit testing"
trigger: "test query"
enabled: true
database: "testdb"
credentials_env_key: "DB_TEST"
sql: |
  SELECT * FROM test_table WHERE id = ?
parameters:
  - name: id
    type: string
    required: true
cache_ttl_seconds: 60
"""

SAMPLE_QUERY_NO_PARAMS_YAML = """
name: "No Params Query"
description: "A query with no parameters"
trigger: "no params"
enabled: true
database: "testdb"
credentials_env_key: "DB_TEST"
sql: "SELECT COUNT(*) FROM test_table"
parameters: []
cache_ttl_seconds: 0
"""

DISABLED_QUERY_YAML = """
name: "Disabled Query"
description: "This query is disabled"
trigger: "disabled"
enabled: false
database: "testdb"
credentials_env_key: "DB_TEST"
sql: "SELECT 1"
parameters: []
"""


@pytest.fixture
def temp_queries_dir():
    """Create a temporary directory with test query YAML files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Write test YAML files
        (Path(tmpdir) / "test_query.yaml").write_text(SAMPLE_QUERY_YAML)
        (Path(tmpdir) / "no_params.yaml").write_text(SAMPLE_QUERY_NO_PARAMS_YAML)
        (Path(tmpdir) / "disabled.yaml").write_text(DISABLED_QUERY_YAML)
        yield tmpdir


class TestQueryLoader:
    """Tests for QueryLoader."""

    def test_load_queries_from_directory(self, temp_queries_dir):
        """Test loading queries from directory."""
        loader = QueryLoader(queries_dir=temp_queries_dir)
        # Should load 2 enabled queries (disabled one is skipped)
        assert len(loader.queries) == 2
        assert "test_query" in loader.queries
        assert "no_params" in loader.queries
        assert "disabled" not in loader.queries

    def test_get_query_by_id(self, temp_queries_dir):
        """Test getting query by ID."""
        loader = QueryLoader(queries_dir=temp_queries_dir)
        query = loader.get_query_by_id("test_query")
        assert query is not None
        assert query.name == "Test Query"
        assert query.database == "testdb"

    def test_get_query_by_id_not_found(self, temp_queries_dir):
        """Test getting non-existent query returns None."""
        loader = QueryLoader(queries_dir=temp_queries_dir)
        query = loader.get_query_by_id("nonexistent")
        assert query is None

    def test_get_query_by_trigger(self, temp_queries_dir):
        """Test finding query by trigger phrase."""
        loader = QueryLoader(queries_dir=temp_queries_dir)
        query = loader.get_query_by_trigger("test query something")
        assert query is not None
        assert query.trigger == "test query"

    def test_get_query_by_trigger_not_found(self, temp_queries_dir):
        """Test trigger not found returns None."""
        loader = QueryLoader(queries_dir=temp_queries_dir)
        query = loader.get_query_by_trigger("nonexistent trigger")
        assert query is None

    def test_get_all_queries(self, temp_queries_dir):
        """Test getting all queries."""
        loader = QueryLoader(queries_dir=temp_queries_dir)
        queries = loader.get_all_queries()
        assert len(queries) == 2

    def test_reload(self, temp_queries_dir):
        """Test hot-reload functionality."""
        loader = QueryLoader(queries_dir=temp_queries_dir)
        initial_count = len(loader.queries)
        loader.reload()
        assert len(loader.queries) == initial_count

    def test_requires_queries_dir(self):
        """Test that queries_dir is required."""
        # Clear env var if set
        with patch.dict(os.environ, {}, clear=True):
            if "QUERY_DEFINITIONS_PATH" in os.environ:
                del os.environ["QUERY_DEFINITIONS_PATH"]
            with pytest.raises(ValueError, match="queries_dir must be provided"):
                QueryLoader()

    def test_uses_env_var(self, temp_queries_dir):
        """Test that QUERY_DEFINITIONS_PATH env var is used."""
        with patch.dict(os.environ, {"QUERY_DEFINITIONS_PATH": temp_queries_dir}):
            loader = QueryLoader()
            assert len(loader.queries) == 2


class TestQueryExecutor:
    """Tests for QueryExecutor."""

    def test_cache_key_generation(self):
        """Test cache key generation is consistent."""
        executor = QueryExecutor()
        key1 = executor._build_cache_key("test", {"a": "1", "b": "2"})
        key2 = executor._build_cache_key("test", {"b": "2", "a": "1"})
        assert key1 == key2

    def test_cache_key_different_params(self):
        """Test different params produce different keys."""
        executor = QueryExecutor()
        key1 = executor._build_cache_key("test", {"a": "1"})
        key2 = executor._build_cache_key("test", {"a": "2"})
        assert key1 != key2

    def test_cache_functionality(self):
        """Test caching with TTL."""
        executor = QueryExecutor()
        cache_key = "test:param=value"
        executor._cache_result(cache_key, [{"col": "value"}])

        cached = executor._check_cache(cache_key, ttl_seconds=60)
        assert cached is not None
        assert cached["data"] == [{"col": "value"}]

    def test_cache_expired(self):
        """Test expired cache returns None."""
        executor = QueryExecutor()
        cache_key = "test:param=value"
        executor._cache_result(cache_key, [{"col": "value"}])

        # TTL of 0 means always expired
        cached = executor._check_cache(cache_key, ttl_seconds=0)
        assert cached is None

    def test_clear_cache_specific_query(self):
        """Test clearing cache for specific query."""
        executor = QueryExecutor()
        executor.cache["query1:a=1"] = {"data": [], "timestamp": 0}
        executor.cache["query1:b=2"] = {"data": [], "timestamp": 0}
        executor.cache["query2:c=3"] = {"data": [], "timestamp": 0}
        executor.cache_order = ["query1:a=1", "query1:b=2", "query2:c=3"]

        count = executor.clear_cache("query1")
        assert count == 2
        assert "query2:c=3" in executor.cache
        assert len(executor.cache) == 1

    def test_clear_cache_all(self):
        """Test clearing all cache entries."""
        executor = QueryExecutor()
        executor.cache["query1:a=1"] = {"data": [], "timestamp": 0}
        executor.cache["query2:b=2"] = {"data": [], "timestamp": 0}
        executor.cache_order = ["query1:a=1", "query2:b=2"]

        count = executor.clear_cache()
        assert count == 2
        assert len(executor.cache) == 0

    def test_validate_parameters_missing_required(self):
        """Test validation catches missing required params."""
        executor = QueryExecutor()
        query_def = QueryDefinition(
            name="Test",
            description="Test",
            trigger="test",
            database="db",
            credentials_env_key="DB_TEST",
            sql="SELECT * FROM test WHERE id = ?",
            parameters=[{"name": "id", "type": "string", "required": True}],
        )

        error = executor._validate_parameters(query_def, {})
        assert error is not None
        assert "id" in error

    def test_validate_parameters_all_present(self):
        """Test validation passes when all required params present."""
        executor = QueryExecutor()
        query_def = QueryDefinition(
            name="Test",
            description="Test",
            trigger="test",
            database="db",
            credentials_env_key="DB_TEST",
            sql="SELECT * FROM test WHERE id = ?",
            parameters=[{"name": "id", "type": "string", "required": True}],
        )

        error = executor._validate_parameters(query_def, {"id": "123"})
        assert error is None

    @patch("simbot.sql_tools.executor.PYODBC_AVAILABLE", False)
    def test_execute_without_pyodbc(self):
        """Test graceful handling when pyodbc unavailable."""
        executor = QueryExecutor()
        query_def = QueryDefinition(
            name="Test",
            description="Test",
            trigger="test",
            database="db",
            credentials_env_key="DB_TEST",
            sql="SELECT * FROM test",
            parameters=[],
        )
        context = ExecutionContext(correlation_id="test-123", interface="test")

        result = executor.execute(query_def, {}, context)
        assert result.success is False
        assert "pyodbc" in result.error.lower()


class TestQueryDefinition:
    """Tests for QueryDefinition model."""

    def test_valid_definition(self):
        """Test creating valid query definition."""
        query_def = QueryDefinition(
            name="Test Query",
            description="Test description",
            trigger="test",
            database="testdb",
            credentials_env_key="DB_TEST",
            sql="SELECT * FROM test WHERE id = ?",
            parameters=[{"name": "id", "type": "string", "required": True}],
            cache_ttl_seconds=60,
        )
        assert query_def.name == "Test Query"
        assert query_def.enabled is True
        assert len(query_def.parameters) == 1

    def test_invalid_parameter_type(self):
        """Test validation rejects invalid parameter types."""
        with pytest.raises(ValidationError):
            QueryDefinition(
                name="Test",
                description="Test",
                trigger="test",
                database="db",
                credentials_env_key="DB_TEST",
                sql="SELECT 1",
                parameters=[{"name": "bad", "type": "invalid_type", "required": True}],
            )

    def test_empty_sql_rejected(self):
        """Test validation rejects empty SQL."""
        with pytest.raises(ValidationError):
            QueryDefinition(
                name="Test",
                description="Test",
                trigger="test",
                database="db",
                credentials_env_key="DB_TEST",
                sql="   ",
                parameters=[],
            )

    def test_extra_fields_rejected(self):
        """Test that extra fields in YAML are rejected."""
        with pytest.raises(ValidationError):
            QueryDefinition(
                name="Test",
                description="Test",
                trigger="test",
                database="db",
                credentials_env_key="DB_TEST",
                sql="SELECT 1",
                parameters=[],
                unknown_field="should fail",
            )


class TestQueryResult:
    """Tests for QueryResult dataclass."""

    def test_success_result(self):
        """Test creating success result."""
        result = QueryResult(
            success=True,
            data=[{"id": 1, "name": "test"}],
            correlation_id="abc123",
        )
        assert result.success is True
        assert result.data is not None
        assert result.error is None
        assert "executed_at" in result.metadata

    def test_error_result(self):
        """Test creating error result."""
        result = QueryResult(
            success=False,
            error="Something went wrong",
            error_code="TEST_ERROR",
            correlation_id="abc123",
        )
        assert result.success is False
        assert result.error == "Something went wrong"

    def test_to_dict(self):
        """Test serialization to dict."""
        result = QueryResult(
            success=True,
            data=[{"id": 1}],
            correlation_id="abc123",
        )
        d = result.to_dict()
        assert d["success"] is True
        assert d["data"] == [{"id": 1}]
        assert d["correlation_id"] == "abc123"


class TestExecutionContext:
    """Tests for ExecutionContext dataclass."""

    def test_str_representation(self):
        """Test string representation."""
        context = ExecutionContext(
            correlation_id="abc123",
            interface="api",
            user_id="user456",
        )
        s = str(context)
        assert "abc123" in s
        assert "api" in s
        assert "user456" in s

    def test_str_without_user_id(self):
        """Test string representation without user ID."""
        context = ExecutionContext(
            correlation_id="abc123",
            interface="api",
        )
        s = str(context)
        assert "unknown" in s
