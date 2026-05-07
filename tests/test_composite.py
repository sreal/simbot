"""Tests for composite query loading and execution."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import uuid

from simbot.sql_tools import (
    QueryLoader,
    QueryExecutor,
    QueryResult,
    ExecutionContext,
    CompositeQueryExecutor,
)


SQL_QUERY_A = """
name: "Query A"
description: "First sub-query"
trigger: "lookup a"
enabled: true
database: "testdb"
credentials_env_key: "DB_TEST"
sql: "SELECT 1 WHERE id = ?"
parameters:
  - name: ID
    type: string
    required: true
cache_ttl_seconds: 0
"""

SQL_QUERY_B = """
name: "Query B"
description: "Second sub-query"
trigger: "lookup b"
enabled: true
database: "testdb"
credentials_env_key: "DB_TEST"
sql: "SELECT 2 WHERE id = ?"
parameters:
  - name: ID
    type: string
    required: true
cache_ttl_seconds: 0
"""

SQL_QUERY_TWO_PARAMS = """
name: "Query Two Params"
description: "Sub-query with two parameters"
trigger: "lookup two"
enabled: true
database: "testdb"
credentials_env_key: "DB_TEST"
sql: "SELECT 1 WHERE id = ? AND name = ?"
parameters:
  - name: ID
    type: string
    required: true
  - name: Name
    type: string
    required: false
cache_ttl_seconds: 0
"""

COMPOSITE_OK_YAML = """
name: "Composite OK"
description: "Composite over Query A and Query B"
trigger: "check both"
enabled: true
type: composite
parameters:
  - name: SharedID
    type: string
    required: true
queries:
  - label: a
    ref: query_a
    params:
      ID: "{SharedID}"
  - label: b
    ref: query_b
    params:
      ID: "{SharedID}"
"""

COMPOSITE_BROKEN_REF_YAML = """
name: "Composite Broken Ref"
description: "References a missing query"
trigger: "check broken"
enabled: true
type: composite
parameters:
  - name: SharedID
    type: string
    required: true
queries:
  - label: a
    ref: query_a
    params:
      ID: "{SharedID}"
  - label: ghost
    ref: nonexistent_query
    params:
      ID: "{SharedID}"
"""

COMPOSITE_DUP_LABEL_YAML = """
name: "Composite Dup Label"
description: "Duplicate label"
trigger: "check dup"
enabled: true
type: composite
parameters:
  - name: SharedID
    type: string
    required: true
queries:
  - label: same
    ref: query_a
    params:
      ID: "{SharedID}"
  - label: same
    ref: query_b
    params:
      ID: "{SharedID}"
"""

COMPOSITE_UNKNOWN_PARAM_YAML = """
name: "Composite Unknown Param"
description: "Maps a parameter the sub-query does not declare"
trigger: "check unknown"
enabled: true
type: composite
parameters:
  - name: SharedID
    type: string
    required: true
queries:
  - label: a
    ref: query_a
    params:
      ID: "{SharedID}"
      DoesNotExist: "{SharedID}"
"""

COMPOSITE_MISSING_REQUIRED_YAML = """
name: "Composite Missing Required"
description: "Does not pass a required sub-query parameter"
trigger: "check missing"
enabled: true
type: composite
parameters:
  - name: SharedID
    type: string
    required: true
queries:
  - label: a
    ref: query_a
    params: {}
"""

COMPOSITE_UNKNOWN_TOKEN_YAML = """
name: "Composite Unknown Token"
description: "{Name} does not match any composite parameter"
trigger: "check token"
enabled: true
type: composite
parameters:
  - name: SharedID
    type: string
    required: true
queries:
  - label: a
    ref: query_a
    params:
      ID: "{NotDeclared}"
"""

UNKNOWN_TYPE_YAML = """
name: "Bogus"
description: "Has an unknown type"
trigger: "bogus"
enabled: true
type: not_a_real_kind
"""


def _write(tmpdir: str, name: str, content: str) -> None:
    Path(tmpdir, name).write_text(content)


@pytest.fixture
def composite_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write(tmpdir, "query_a.yaml", SQL_QUERY_A)
        _write(tmpdir, "query_b.yaml", SQL_QUERY_B)
        _write(tmpdir, "composite_ok.yaml", COMPOSITE_OK_YAML)
        yield tmpdir


class TestCompositeLoader:
    def test_loads_composite_alongside_sql(self, composite_dir):
        loader = QueryLoader(queries_dir=composite_dir)
        assert "query_a" in loader.queries
        assert "query_b" in loader.queries
        assert "composite_ok" in loader.composites
        assert loader.get_composite_by_id("composite_ok") is not None

    def test_get_composite_by_trigger(self, composite_dir):
        loader = QueryLoader(queries_dir=composite_dir)
        assert loader.get_composite_by_trigger("check both 123") is not None
        assert loader.get_composite_by_trigger("nope") is None

    def test_absent_type_back_compat(self, composite_dir):
        loader = QueryLoader(queries_dir=composite_dir)
        # The two sub-queries have no `type` key — they must still load as SQL.
        assert loader.queries["query_a"].name == "Query A"

    def test_broken_ref_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _write(tmpdir, "query_a.yaml", SQL_QUERY_A)
            _write(tmpdir, "query_b.yaml", SQL_QUERY_B)
            _write(tmpdir, "broken.yaml", COMPOSITE_BROKEN_REF_YAML)
            with pytest.raises(ValueError, match="nonexistent_query"):
                QueryLoader(queries_dir=tmpdir)

    def test_duplicate_labels_raise(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _write(tmpdir, "query_a.yaml", SQL_QUERY_A)
            _write(tmpdir, "query_b.yaml", SQL_QUERY_B)
            _write(tmpdir, "dup.yaml", COMPOSITE_DUP_LABEL_YAML)
            with pytest.raises(Exception, match="(unique|same)"):
                QueryLoader(queries_dir=tmpdir)

    def test_unknown_subquery_param_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _write(tmpdir, "query_a.yaml", SQL_QUERY_A)
            _write(tmpdir, "bad.yaml", COMPOSITE_UNKNOWN_PARAM_YAML)
            with pytest.raises(ValueError, match="DoesNotExist"):
                QueryLoader(queries_dir=tmpdir)

    def test_missing_required_subquery_param_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _write(tmpdir, "query_a.yaml", SQL_QUERY_A)
            _write(tmpdir, "bad.yaml", COMPOSITE_MISSING_REQUIRED_YAML)
            with pytest.raises(ValueError, match="ID"):
                QueryLoader(queries_dir=tmpdir)

    def test_unknown_composite_token_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _write(tmpdir, "query_a.yaml", SQL_QUERY_A)
            _write(tmpdir, "bad.yaml", COMPOSITE_UNKNOWN_TOKEN_YAML)
            with pytest.raises(ValueError, match="NotDeclared"):
                QueryLoader(queries_dir=tmpdir)

    def test_unknown_type_value_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _write(tmpdir, "bogus.yaml", UNKNOWN_TYPE_YAML)
            with pytest.raises(Exception, match="not_a_real_kind"):
                QueryLoader(queries_dir=tmpdir)


class _FakeExecutor:
    """Minimal stand-in for QueryExecutor.execute(query_def, params, context)."""

    def __init__(self, mode="all_ok", capture=None):
        # mode: 'all_ok', 'b_fails', 'all_fail', 'b_raises'
        self.mode = mode
        self.capture = capture if capture is not None else []

    def execute(self, query_def, params, context):
        self.capture.append((query_def.name, dict(params)))
        if self.mode == "all_ok":
            return QueryResult(success=True, data=[{"col": query_def.name}], correlation_id=context.correlation_id)
        if self.mode == "b_fails" and query_def.name == "Query B":
            return QueryResult(success=False, error="boom", error_code="X", correlation_id=context.correlation_id)
        if self.mode == "b_raises" and query_def.name == "Query B":
            raise RuntimeError("kaboom")
        if self.mode == "all_fail":
            return QueryResult(success=False, error="boom", error_code="X", correlation_id=context.correlation_id)
        return QueryResult(success=True, data=[{"col": query_def.name}], correlation_id=context.correlation_id)


def _make_context() -> ExecutionContext:
    return ExecutionContext(
        correlation_id=str(uuid.uuid4()),
        interface="test",
        user_id="u",
    )


class TestCompositeExecutor:
    def test_happy_path_returns_all_sub_results(self, composite_dir):
        loader = QueryLoader(queries_dir=composite_dir)
        composite = loader.get_composite_by_id("composite_ok")
        capture = []
        executor = CompositeQueryExecutor(_FakeExecutor("all_ok", capture), loader)

        result = executor.execute(composite, {"SharedID": "abc-123"}, _make_context())

        assert result.success is True
        assert set(result.sub_results.keys()) == {"a", "b"}
        assert all(r.success for r in result.sub_results.values())

        # Both sub-queries received the same SharedID via {Name} substitution.
        assert capture == [("Query A", {"ID": "abc-123"}), ("Query B", {"ID": "abc-123"})]

    def test_partial_failure_returns_best_effort(self, composite_dir):
        loader = QueryLoader(queries_dir=composite_dir)
        composite = loader.get_composite_by_id("composite_ok")
        executor = CompositeQueryExecutor(_FakeExecutor("b_fails"), loader)

        result = executor.execute(composite, {"SharedID": "abc-123"}, _make_context())

        assert result.success is True  # 'a' still succeeded
        assert result.sub_results["a"].success is True
        assert result.sub_results["b"].success is False

    def test_sub_query_exception_isolated(self, composite_dir):
        loader = QueryLoader(queries_dir=composite_dir)
        composite = loader.get_composite_by_id("composite_ok")
        executor = CompositeQueryExecutor(_FakeExecutor("b_raises"), loader)

        result = executor.execute(composite, {"SharedID": "abc-123"}, _make_context())

        assert result.sub_results["a"].success is True
        assert result.sub_results["b"].success is False
        assert result.sub_results["b"].error_code == "COMPOSITE_SUB_EXCEPTION"
        assert "kaboom" in result.sub_results["b"].error

    def test_all_fail_marks_composite_failed(self, composite_dir):
        loader = QueryLoader(queries_dir=composite_dir)
        composite = loader.get_composite_by_id("composite_ok")
        executor = CompositeQueryExecutor(_FakeExecutor("all_fail"), loader)

        result = executor.execute(composite, {"SharedID": "abc-123"}, _make_context())

        assert result.success is False
        assert all(not r.success for r in result.sub_results.values())

    def test_whole_value_substitution_preserves_type(self):
        # Build a tiny composite by hand and exercise just _render_params.
        rendered = CompositeQueryExecutor._render_params(
            {"limit": "{Limit}"},
            {"Limit": 5},
        )
        assert rendered == {"limit": 5}
        assert isinstance(rendered["limit"], int)

    def test_partial_template_renders_string(self):
        rendered = CompositeQueryExecutor._render_params(
            {"prefixed": "id_{Name}"},
            {"Name": "abc"},
        )
        assert rendered == {"prefixed": "id_abc"}
