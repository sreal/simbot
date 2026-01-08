"""SQL query executor - stateless execution engine."""

import os
import re
import time
import logging
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime, UTC

try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

from .models import QueryDefinition, QueryResult, ExecutionContext


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("sql_tools_audit")


class QueryExecutor:
    """
    Stateless query executor with caching and connection management.

    Uses simple connection health checks and auto-reconnection.
    Cache has max size limit with FIFO eviction.
    """

    # Simple, boring cache limit - prevents unbounded memory growth
    MAX_CACHE_SIZE = 1000

    def __init__(self):
        if not PYODBC_AVAILABLE:
            logger.warning("pyodbc not available - SQL queries will fail")

        self.db_connections: Dict[str, Any] = {}
        self.cache: Dict[str, dict] = {}
        self.cache_order: List[str] = []  # Track insertion order for FIFO eviction

    def execute(
        self,
        query_def: QueryDefinition,
        params: Dict[str, Any],
        context: Optional[ExecutionContext] = None
    ) -> QueryResult:
        """
        Execute a query with correlation tracking.

        Args:
            query_def: Validated query definition
            params: Parameter values (pre-validated)
            context: Execution context with correlation ID

        Returns:
            QueryResult with success/error status
        """
        # Create context if not provided
        if context is None:
            context = ExecutionContext(
                correlation_id=str(uuid.uuid4()),
                interface='unknown'
            )

        logger.info(f"{context} Executing query: {query_def.name}")

        try:
            # Check cache first
            cache_key = self._build_cache_key(query_def.name, params)
            cached = self._check_cache(cache_key, query_def.cache_ttl_seconds)
            if cached:
                logger.info(f"{context} Cache hit: {cache_key}")
                return QueryResult(
                    success=True,
                    data=cached['data'],
                    metadata={'from_cache': True, 'cached_at': cached['timestamp']},
                    correlation_id=context.correlation_id
                )

            # Validate parameters
            validation_error = self._validate_parameters(query_def, params)
            if validation_error:
                logger.warning(f"{context} Validation failed: {validation_error}")
                self._audit_log(context, query_def, params, success=False, error=validation_error)
                return QueryResult(
                    success=False,
                    error=validation_error,
                    error_code='VALIDATION_ERROR',
                    correlation_id=context.correlation_id
                )

            # Execute query
            start_time = datetime.now(UTC)
            rows = self._execute_sql(query_def, params)
            execution_time = (datetime.now(UTC) - start_time).total_seconds()

            # Convert to list of dicts or None
            data = rows if rows else None
            row_count = len(rows) if rows else 0

            result = QueryResult(
                success=True,
                data=data,
                metadata={
                    'row_count': row_count,
                    'execution_time_seconds': execution_time,
                    'from_cache': False
                },
                correlation_id=context.correlation_id
            )

            # Cache result
            if query_def.cache_ttl_seconds > 0:
                self._cache_result(cache_key, data)

            # Audit log
            self._audit_log(context, query_def, params, success=True, row_count=row_count)

            logger.info(f"{context} Query successful: {row_count} rows in {execution_time:.2f}s")

            return result

        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            logger.error(f"{context} {error_msg}", exc_info=True)
            self._audit_log(context, query_def, params, success=False, error=str(e))

            return QueryResult(
                success=False,
                error=error_msg,
                error_code='EXECUTION_ERROR',
                correlation_id=context.correlation_id
            )

    def _validate_parameters(
        self, query_def: QueryDefinition, params: dict
    ) -> Optional[str]:
        """
        Validate required parameters are present.
        Returns error message if validation fails, None otherwise.
        """
        required = [p.name for p in query_def.parameters if p.required]
        missing = [p for p in required if p not in params]

        if missing:
            return f"Missing required parameters: {', '.join(missing)}. {query_def.description}"

        return None

    def _execute_sql(self, query_def: QueryDefinition, params: dict) -> List[dict]:
        """Execute SQL query and return rows as list of dicts."""
        if not PYODBC_AVAILABLE:
            raise RuntimeError("pyodbc not installed - cannot execute SQL queries")

        # Get database connection
        conn = self._get_connection(
            query_def.database, query_def.credentials_env_key
        )

        # Execute query
        cursor = conn.cursor()

        # Count ? placeholders in SQL
        placeholder_count = query_def.sql.count('?')

        # Get parameter values in order
        param_names = [p.name for p in query_def.parameters]

        # Simple 1:1 mapping - placeholders must match parameters
        if placeholder_count == 0 and len(param_names) == 0:
            # No parameters needed
            param_values = ()
        elif placeholder_count == len(param_names):
            # Expected case: one placeholder per parameter
            param_values = tuple(params.get(name) for name in param_names)
        else:
            # Mismatch - fail fast with clear error
            raise ValueError(
                f"Query '{query_def.name}': SQL has {placeholder_count} placeholders "
                f"but {len(param_names)} parameters defined. "
                f"Each parameter should have exactly one placeholder (?)."
            )

        cursor.execute(query_def.sql, param_values)

        # Fetch results
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        # Convert to list of dicts
        result = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Convert datetime to string for JSON serialization
                if isinstance(value, datetime):
                    value = value.isoformat()
                row_dict[col] = value
            result.append(row_dict)

        cursor.close()
        return result

    def _get_connection(self, database: str, credentials_env_key: str) -> Any:
        """
        Get or create database connection with health check.

        Args:
            database: Database name (validated for safety)
            credentials_env_key: Environment variable containing connection string

        Returns:
            pyodbc connection object

        Raises:
            ValueError: If database name is invalid or connection string not found
        """
        # Validate database name to prevent injection
        if not re.match(r'^[a-zA-Z0-9_]+$', database):
            raise ValueError(
                f"Invalid database name '{database}': "
                "must contain only alphanumeric characters and underscores"
            )

        # Get connection string
        conn_string = os.getenv(credentials_env_key)
        if not conn_string:
            raise ValueError(
                f"Connection string not found in environment: {credentials_env_key}"
            )

        # Append database to connection string if not already present
        if "Database=" not in conn_string and database:
            conn_string = f"{conn_string};Database={database}"

        # Create new connection if doesn't exist
        if database not in self.db_connections:
            logger.debug(f"Creating database connection: {database}")
            self.db_connections[database] = pyodbc.connect(conn_string)
            return self.db_connections[database]

        # Health check existing connection (simple ping)
        conn = self.db_connections[database]
        try:
            # Try simple query to check if connection is alive
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return conn
        except Exception as e:
            # Connection is stale - reconnect
            logger.warning(f"Connection to {database} is stale, reconnecting: {e}")
            try:
                conn.close()
            except:
                pass  # Ignore close errors on stale connections

            logger.debug(f"Reconnecting to database: {database}")
            self.db_connections[database] = pyodbc.connect(conn_string)
            return self.db_connections[database]

    def _build_cache_key(self, query_name: str, params: dict) -> str:
        """Create cache key from query name and sorted parameters."""
        # Sort params for consistent key
        param_str = "_".join(f"{k}={v}" for k, v in sorted(params.items()))
        return f"{query_name}:{param_str}"

    def _check_cache(self, cache_key: str, ttl_seconds: int) -> Optional[dict]:
        """Check if cached result is still valid."""
        if ttl_seconds <= 0:
            return None

        if cache_key not in self.cache:
            return None

        cached = self.cache[cache_key]
        age = time.time() - cached["timestamp"]

        if age < ttl_seconds:
            return cached

        # Expired - remove from both cache and order tracking
        del self.cache[cache_key]
        if cache_key in self.cache_order:
            self.cache_order.remove(cache_key)
        return None

    def _cache_result(self, cache_key: str, data: List[dict]):
        """Cache query result with FIFO eviction."""
        # Evict oldest entry if cache is full
        if len(self.cache) >= self.MAX_CACHE_SIZE:
            if self.cache_order:
                oldest_key = self.cache_order.pop(0)
                if oldest_key in self.cache:
                    del self.cache[oldest_key]
                    logger.debug(f"Evicted oldest cache entry: {oldest_key}")

        timestamp = time.time()
        self.cache[cache_key] = {"data": data, "timestamp": timestamp}

        # Track insertion order
        if cache_key not in self.cache_order:
            self.cache_order.append(cache_key)

        logger.debug(f"Cached result: {cache_key} (cache size: {len(self.cache)})")

    def _audit_log(self, context: ExecutionContext, query_def: QueryDefinition,
                   params: dict, success: bool, row_count: int = 0, error: Optional[str] = None):
        """Log execution for audit trail."""
        audit_logger.info(
            f"{context} query={query_def.name} params={params} "
            f"success={success} rows={row_count} error={error}"
        )

    def clear_cache(self, query_name: Optional[str] = None):
        """Clear cache for specific query or all queries."""
        if query_name is None:
            count = len(self.cache)
            self.cache = {}
            self.cache_order = []
            logger.info(f"Cleared all cache ({count} entries)")
            return count

        # Clear cache entries for specific query
        to_remove = [k for k in self.cache.keys() if k.startswith(f"{query_name}:")]
        for k in to_remove:
            del self.cache[k]
            if k in self.cache_order:
                self.cache_order.remove(k)

        logger.info(f"Cleared cache for query: {query_name} ({len(to_remove)} entries)")
        return len(to_remove)

    def close_connections(self):
        """Close all database connections."""
        for db, conn in self.db_connections.items():
            try:
                conn.close()
                logger.debug(f"Closed connection: {db}")
            except Exception as e:
                logger.error(f"Error closing connection {db}: {e}")

        self.db_connections = {}
