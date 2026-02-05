"""Blob check executor - gather files from Azure Blob Storage."""

import os
import re
import logging
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime, UTC

try:
    from azure.storage.blob import BlobServiceClient, ContainerClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

from simbot.sql_tools.models import ExecutionContext
from .models import (
    BlobCheckDefinition,
    BlobStorageConfig,
    FilePattern,
    FileResult,
    BlobCheckResult,
)
from .path_resolver import PathResolver


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("blob_tools_audit")


class BlobCheckExecutor:
    """
    Execute blob checks - gather files based on definition.

    Stateless executor with connection management.
    """

    # Safety limit for content downloads
    MAX_CONTENT_SIZE_MB = 50.0

    def __init__(self):
        if not AZURE_AVAILABLE:
            logger.warning("azure-storage-blob not available - blob checks will fail")

        self._clients: Dict[str, BlobServiceClient] = {}
        self.path_resolver = PathResolver()

    def execute(
        self,
        blob_def: BlobCheckDefinition,
        params: Dict[str, Any],
        context: Optional[ExecutionContext] = None
    ) -> BlobCheckResult:
        """
        Execute a blob check with correlation tracking.

        Args:
            blob_def: Validated blob check definition
            params: Parameter values
            context: Execution context with correlation ID

        Returns:
            BlobCheckResult with file results and summary
        """
        if context is None:
            context = ExecutionContext(
                correlation_id=str(uuid.uuid4()),
                interface='unknown'
            )

        logger.info(f"{context} Executing blob check: {blob_def.name}")

        try:
            # Resolve all parameters including derived
            resolved_params = self.path_resolver.resolve_all_parameters(
                blob_def, params
            )
            logger.debug(f"{context} Resolved parameters: {resolved_params}")

            # Get container client
            container = self._get_container(blob_def.storage)

            # Gather files
            file_results: List[FileResult] = []
            for file_pattern in blob_def.files:
                results = self._gather_file(
                    container, file_pattern, resolved_params, context
                )
                file_results.extend(results)

            # Build result
            result = BlobCheckResult(
                success=True,
                definition_name=blob_def.name,
                files=file_results,
                correlation_id=context.correlation_id
            )

            # Audit log
            self._audit_log(
                context, blob_def, params,
                success=True,
                files_found=result.summary.get('found', 0)
            )

            logger.info(
                f"{context} Blob check successful: "
                f"{result.summary.get('found', 0)}/{result.summary.get('total_files', 0)} files"
            )

            return result

        except Exception as e:
            error_msg = f"Blob check failed: {str(e)}"
            logger.error(f"{context} {error_msg}", exc_info=True)
            self._audit_log(context, blob_def, params, success=False, error=str(e))

            return BlobCheckResult(
                success=False,
                definition_name=blob_def.name,
                files=[],
                error=error_msg,
                error_code='EXECUTION_ERROR',
                correlation_id=context.correlation_id
            )

    def _gather_file(
        self,
        container: "ContainerClient",
        file_pattern: FilePattern,
        params: Dict[str, Any],
        context: ExecutionContext
    ) -> List[FileResult]:
        """
        Gather file(s) matching a pattern.

        Args:
            container: Azure container client
            file_pattern: File pattern definition from YAML
            params: Resolved parameter values
            context: Execution context with correlation ID

        Returns:
            List of FileResult objects (one for exact match, multiple for regex)
        """
        resolved_path = self.path_resolver.resolve_path(file_pattern.path, params)
        logger.debug(f"{context} Resolved path: {file_pattern.path} -> {resolved_path}")

        if file_pattern.match == "regex":
            return self._gather_regex_match(
                container, file_pattern, resolved_path, context
            )
        else:
            return [self._gather_exact_match(
                container, file_pattern, resolved_path, context
            )]

    def _gather_exact_match(
        self,
        container: "ContainerClient",
        file_pattern: FilePattern,
        path: str,
        context: ExecutionContext
    ) -> FileResult:
        """
        Gather a single file by exact path.

        Args:
            container: Azure container client
            file_pattern: File pattern definition from YAML
            path: Resolved blob path
            context: Execution context with correlation ID

        Returns:
            FileResult with exists=True/False and metadata if found
        """
        try:
            blob_client = container.get_blob_client(path)

            # Check if exists and get properties
            if not blob_client.exists():
                return FileResult(
                    name=file_pattern.name,
                    path=path,
                    exists=False
                )

            properties = blob_client.get_blob_properties()

            result = FileResult(
                name=file_pattern.name,
                path=path,
                exists=True,
                size_bytes=properties.size,
                last_modified=properties.last_modified,
                content_type=properties.content_settings.content_type,
            )

            # Download content if requested
            if file_pattern.return_type in ('content', 'both'):
                result.content = self._download_content(
                    blob_client, properties.size, context
                )

            return result

        except Exception as e:
            logger.warning(f"{context} Error gathering {path}: {e}")
            return FileResult(
                name=file_pattern.name,
                path=path,
                exists=False,
                error=str(e)
            )

    def _gather_regex_match(
        self,
        container: "ContainerClient",
        file_pattern: FilePattern,
        pattern: str,
        context: ExecutionContext
    ) -> List[FileResult]:
        """
        Gather files matching a regex pattern.

        Args:
            container: Azure container client
            file_pattern: File pattern definition from YAML
            pattern: Regex pattern to match blob names
            context: Execution context with correlation ID

        Returns:
            List of FileResult objects for matching blobs
        """
        # Extract prefix for efficient listing (everything before first regex char)
        prefix = self._extract_prefix(pattern)
        regex = re.compile(pattern)

        logger.debug(f"{context} Listing blobs with prefix: {prefix}")

        # List blobs and filter by regex
        matching_blobs = []
        try:
            for blob in container.list_blobs(name_starts_with=prefix):
                if regex.match(blob.name):
                    matching_blobs.append({
                        'name': blob.name,
                        'size': blob.size,
                        'last_modified': blob.last_modified,
                        'content_type': (
                            blob.content_settings.content_type
                            if blob.content_settings else None
                        ),
                    })
        except Exception as e:
            logger.warning(f"{context} Error listing blobs: {e}")
            return [FileResult(
                name=file_pattern.name,
                path=pattern,
                exists=False,
                error=f"Failed to list blobs: {e}"
            )]

        if not matching_blobs:
            return [FileResult(
                name=file_pattern.name,
                path=pattern,
                exists=False
            )]

        # Apply select filter
        if file_pattern.select == 'latest':
            matching_blobs = [max(matching_blobs, key=lambda b: b['last_modified'])]
        elif file_pattern.select == 'oldest':
            matching_blobs = [min(matching_blobs, key=lambda b: b['last_modified'])]

        # Build results
        results = []
        for blob in matching_blobs:
            result = FileResult(
                name=file_pattern.name,
                path=blob['name'],
                exists=True,
                size_bytes=blob['size'],
                last_modified=blob['last_modified'],
                content_type=blob['content_type'],
            )

            # Download content if requested
            if file_pattern.return_type in ('content', 'both'):
                blob_client = container.get_blob_client(blob['name'])
                result.content = self._download_content(
                    blob_client, blob['size'], context
                )

            results.append(result)

        return results

    def _extract_prefix(self, pattern: str) -> str:
        """
        Extract static prefix from regex pattern for efficient listing.

        Args:
            pattern: Regex pattern that may contain metacharacters

        Returns:
            Static prefix string before any regex metacharacters
        """
        # Find first regex metacharacter that indicates a pattern
        # Note: standalone '.' is common in filenames, so we look for
        # '.*', '.+', or other regex constructs
        regex_patterns = ['.*', '.+', '.?', '[', '(', '|', '^', '$', '\\']
        prefix_end = len(pattern)

        for regex_pat in regex_patterns:
            idx = pattern.find(regex_pat)
            if idx != -1 and idx < prefix_end:
                prefix_end = idx

        return pattern[:prefix_end]

    def _download_content(
        self,
        blob_client,
        size_bytes: int,
        context: ExecutionContext
    ) -> Optional[bytes]:
        """
        Download blob content with size limit.

        Args:
            blob_client: Azure blob client for the specific blob
            size_bytes: Size of blob in bytes
            context: Execution context with correlation ID

        Returns:
            Blob content as bytes, or None if too large or download fails
        """
        max_size_bytes = int(self.MAX_CONTENT_SIZE_MB * 1024 * 1024)

        if size_bytes > max_size_bytes:
            logger.warning(
                f"{context} Blob too large to download: "
                f"{size_bytes / 1024 / 1024:.1f}MB > {self.MAX_CONTENT_SIZE_MB}MB"
            )
            return None

        try:
            return blob_client.download_blob().readall()
        except Exception as e:
            logger.warning(f"{context} Failed to download content: {e}")
            return None

    def _get_container(self, storage: BlobStorageConfig) -> "ContainerClient":
        """
        Get or create Azure container client.

        Args:
            storage: Storage configuration with account/key env var names

        Returns:
            Azure ContainerClient object

        Raises:
            RuntimeError: If azure-storage-blob is not installed
            ValueError: If storage credentials not found in environment
        """
        if not AZURE_AVAILABLE:
            raise RuntimeError(
                "azure-storage-blob not installed - cannot execute blob checks"
            )

        # Get credentials from environment
        account_name = os.getenv(storage.account_env_key)
        account_key = os.getenv(storage.key_env_key)

        if not account_name:
            raise ValueError(
                f"Storage account not found in environment: {storage.account_env_key}"
            )
        if not account_key:
            raise ValueError(
                f"Storage key not found in environment: {storage.key_env_key}"
            )

        # Create or reuse client
        client_key = f"{account_name}/{storage.container}"
        if client_key not in self._clients:
            account_url = f"https://{account_name}.blob.core.windows.net"
            self._clients[client_key] = BlobServiceClient(
                account_url=account_url,
                credential=account_key
            )

        return self._clients[client_key].get_container_client(storage.container)

    def _audit_log(
        self,
        context: ExecutionContext,
        blob_def: BlobCheckDefinition,
        params: Dict[str, Any],
        success: bool,
        files_found: int = 0,
        error: Optional[str] = None
    ):
        """
        Log execution for audit trail.

        Args:
            context: Execution context with correlation ID
            blob_def: Blob check definition
            params: Parameter values used
            success: Whether execution succeeded
            files_found: Number of files found
            error: Error message if failed
        """
        audit_logger.info(
            f"{context} check={blob_def.name} params={params} "
            f"success={success} files={files_found} error={error}"
        )

    def check_health(self, storage: BlobStorageConfig) -> Dict[str, Any]:
        """
        Check if storage is accessible.

        Args:
            storage: Storage configuration to test

        Returns:
            Dict with 'healthy' bool and 'container' name, plus 'error' if unhealthy
        """
        try:
            container = self._get_container(storage)
            # Try to get container properties
            container.get_container_properties()
            return {
                "healthy": True,
                "container": storage.container,
            }
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e),
                "container": storage.container,
            }
