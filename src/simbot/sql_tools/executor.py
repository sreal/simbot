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


class DatabaseError(Exception):
    """Custom exception for database errors with masked user message."""
    def __init__(self, correlation_id: str, user_message: str, detailed_message: str):
        self.correlation_id = correlation_id
        self.user_message = user_message
        self.detailed_message = detailed_message
        super().__init__(user_message)


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
        Execute a query with correlation tracking and error boundary.

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
                    metadata={'correlation_id': context.correlation_id},
                    correlation_id=context.correlation_id
                )

            # Execute query
            start_time = datetime.now(UTC)
            rows = self._execute_sql(query_def, params, context)
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

        except DatabaseError as e:
            # Database error - log detailed error internally, return generic message
            logger.error(f"{context} Database error: {e.detailed_message}", exc_info=True)
            self._audit_log(context, query_def, params, success=False, error=e.detailed_message)

            return QueryResult(
                success=False,
                error=e.user_message,
                error_code='DATABASE_ERROR',
                metadata={'correlation_id': context.correlation_id},
                correlation_id=context.correlation_id
            )
        except Exception as e:
            # Other errors (validation, configuration, etc.)
            error_msg = f"Query execution failed: {str(e)}"
            logger.error(f"{context} {error_msg}", exc_info=True)
            self._audit_log(context, query_def, params, success=False, error=str(e))

            return QueryResult(
                success=False,
                error=error_msg,
                error_code='EXECUTION_ERROR',
                metadata={'correlation_id': context.correlation_id},
                correlation_id=context.correlation_id
            )

    def _validate_parameters(
        self, query_def: QueryDefinition, params: dict
    ) -> Optional[str]:
        """Validate parameters against query definition.

        Checks that:
        - All required parameters are present
        - Values conform to declared types (string/int/date)

        Returns error message if validation fails, None otherwise.
        """
        required = [p for p in query_def.parameters if p.required]
        missing = [p.name for p in required if p.name not in params]

        if missing:
            return (
                "Missing required parameters: "
                f"{', '.join(missing)}. {query_def.description}"
            )

        # Type validation
        for p in query_def.parameters:
            if p.name not in params:
                continue  # optional and not provided

            raw_value = params[p.name]

            # Allow None only for non-required params
            if raw_value is None and p.required:
                return f"Parameter '{p.name}' is required and cannot be null."

            if p.type == "int":
                try:
                    # Coerce to int so downstream always sees correct type
                    params[p.name] = int(raw_value)
                except (TypeError, ValueError):
                    return f"Parameter '{p.name}' must be an integer."
            elif p.type == "date":
                # Accept YYYY-MM-DD strings, convert to datetime object for SQL Server
                # Use datetime (not date) to include time component for DATETIME columns
                from datetime import datetime

                try:
                    if isinstance(raw_value, str):
                        params[p.name] = datetime.strptime(raw_value, "%Y-%m-%d")
                    else:
                        # Allow date/datetime objects; they are already validated
                        str(raw_value)
                except Exception:
                    return (
                        f"Parameter '{p.name}' must be a date in YYYY-MM-DD format."
                    )
            else:
                # string - coerce everything to string for consistency
                if raw_value is not None and not isinstance(raw_value, str):
                    params[p.name] = str(raw_value)

        return None

    def _execute_sql(self, query_def: QueryDefinition, params: dict, 
                     context: Optional[ExecutionContext] = None) -> List[dict]:
        """
        Execute SQL query and return rows as list of dicts.
        
        Wraps database errors to prevent leaking internal DB/pyodbc error details.
        """
        if not PYODBC_AVAILABLE:
            raise RuntimeError("pyodbc not installed - cannot execute SQL queries")

        # Create a minimal context for error handling if not provided
        if context is None:
            context = ExecutionContext(
                correlation_id=str(uuid.uuid4()),
                interface='internal'
            )

        # Get database connection
        try:
            conn = self._get_connection(
                query_def.database, query_def.credentials_env_key
            )
        except Exception as e:
            detailed_msg = f"Failed to establish database connection: {str(e)}"
            user_msg = "Database connection error. Please contact support with correlation ID."
            raise DatabaseError(context.correlation_id, user_msg, detailed_msg) from e

        # Execute query
        cursor = None
        try:
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

            return result
            
        except DatabaseError:
            # Re-raise our custom error as-is
            raise
        except Exception as e:
            # Wrap database/pyodbc errors to hide implementation details
            # (catches both pyodbc.Error and other exceptions)
            detailed_msg = f"Database execution error for query '{query_def.name}': {str(e)}"
            user_msg = "Database query error. Please contact support with correlation ID."
            raise DatabaseError(context.correlation_id, user_msg, detailed_msg) from e
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception as e:
                    logger.warning(f"Error closing cursor: {e}")

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
        """Check if cached result is still valid.
        
        Logs cache hit/miss with cache_key and current cache size.
        """
        if ttl_seconds <= 0:
            logger.debug(
                f"Cache bypass (TTL={ttl_seconds}): {cache_key} "
                f"(cache_size={len(self.cache)})"
            )
            return None

        if cache_key not in self.cache:
            logger.debug(
                f"Cache miss: {cache_key} "
                f"(cache_size={len(self.cache)})"
            )
            return None

        cached = self.cache[cache_key]
        age = time.time() - cached["timestamp"]

        if age < ttl_seconds:
            logger.debug(
                f"Cache hit: {cache_key} "
                f"(age={age:.2f}s, ttl={ttl_seconds}s, cache_size={len(self.cache)})"
            )
            return cached

        # Expired - remove from both cache and order tracking
        logger.debug(
            f"Cache expired: {cache_key} "
            f"(age={age:.2f}s, ttl={ttl_seconds}s, cache_size={len(self.cache)})"
        )
        del self.cache[cache_key]
        if cache_key in self.cache_order:
            self.cache_order.remove(cache_key)
        return None

    def _cache_result(self, cache_key: str, data: List[dict]):
        """Cache query result with FIFO eviction.
        
        Logs cache operation with cache_key, current cache size, and eviction details if applicable.
        """
        # Evict oldest entry if cache is full
        eviction_msg = ""
        if len(self.cache) >= self.MAX_CACHE_SIZE:
            if self.cache_order:
                oldest_key = self.cache_order.pop(0)
                if oldest_key in self.cache:
                    del self.cache[oldest_key]
                    eviction_msg = f" (evicted_key={oldest_key})"
                    logger.debug(
                        f"Cache eviction (FIFO): {oldest_key} "
                        f"(cache_size_before={len(self.cache) + 1})"
                    )

        timestamp = time.time()
        self.cache[cache_key] = {"data": data, "timestamp": timestamp}

        # Track insertion order
        if cache_key not in self.cache_order:
            self.cache_order.append(cache_key)

        logger.debug(
            f"Cache result stored: {cache_key} "
            f"(cache_size={len(self.cache)}/max={self.MAX_CACHE_SIZE}){eviction_msg}"
        )

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
