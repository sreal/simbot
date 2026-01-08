"""Load and validate query definitions from YAML files."""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional
import yaml
from pydantic import ValidationError

from .models import QueryDefinition


logger = logging.getLogger(__name__)


class QueryLoader:
    """Load query definitions from YAML files."""

    def __init__(self, queries_dir: Optional[str] = None):
        """
        Initialize query loader.

        Args:
            queries_dir: Directory containing query YAML files.
                        If None, uses QUERY_DEFINITIONS_PATH env var.
                        Raises ValueError if neither is provided.
        """
        if queries_dir is None:
            queries_dir = os.getenv("QUERY_DEFINITIONS_PATH")

        if queries_dir is None:
            raise ValueError(
                "queries_dir must be provided or QUERY_DEFINITIONS_PATH env var must be set"
            )

        self.queries_dir = Path(queries_dir)
        self.queries: Dict[str, QueryDefinition] = {}

        if not self.queries_dir.exists():
            logger.warning(f"Queries directory not found: {self.queries_dir}")
            return

        self._load_all_queries()

    def _load_all_queries(self):
        """Load all YAML files and validate with Pydantic."""
        if not self.queries_dir.is_dir():
            logger.error(f"Queries path is not a directory: {self.queries_dir}")
            return

        yaml_files = list(self.queries_dir.glob("*.yaml")) + list(
            self.queries_dir.glob("*.yml")
        )

        if not yaml_files:
            logger.warning(f"No YAML files found in {self.queries_dir}")
            return

        logger.info(f"Loading queries from {self.queries_dir}")

        for yaml_file in yaml_files:
            try:
                with open(yaml_file, 'r') as f:
                    raw_data = yaml.safe_load(f)

                if not raw_data:
                    logger.warning(f"Empty YAML file: {yaml_file}")
                    continue

                # Validate with Pydantic
                query_def = QueryDefinition(**raw_data)

                if query_def.enabled:
                    self.queries[yaml_file.stem] = query_def
                    logger.info(f"Loaded query: {query_def.name} (trigger: {query_def.trigger})")
                else:
                    logger.info(f"Skipped disabled query: {query_def.name}")

            except ValidationError as e:
                logger.error(f"Validation failed for {yaml_file}: {e}")
                raise
            except Exception as e:
                logger.error(f"Failed to load {yaml_file}: {e}")
                raise

        logger.info(f"Loaded {len(self.queries)} enabled queries")

    def get_query_by_id(self, query_id: str) -> Optional[QueryDefinition]:
        """Get query by file name (without .yaml)."""
        return self.queries.get(query_id)

    def get_query_by_trigger(self, text: str) -> Optional[QueryDefinition]:
        """Find query by trigger phrase."""
        text_lower = text.lower().strip()
        for query_def in self.queries.values():
            if text_lower.startswith(query_def.trigger.lower()):
                return query_def
        return None

    def get_all_queries(self) -> List[QueryDefinition]:
        """Get all enabled queries."""
        return list(self.queries.values())

    def reload(self):
        """Hot-reload query definitions."""
        self.queries.clear()
        self._load_all_queries()
