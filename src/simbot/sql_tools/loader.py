"""Load and validate query definitions from YAML files."""

import os
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional
import yaml
from pydantic import ValidationError

from .models import QueryDefinition, CompositeQueryDefinition


logger = logging.getLogger(__name__)


VALID_TYPES = {None, "composite"}  # None = legacy SQL definition


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
        self.composites: Dict[str, CompositeQueryDefinition] = {}

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

                yaml_type = raw_data.get('type')

                if yaml_type not in VALID_TYPES:
                    raise ValueError(
                        f"Unknown 'type' value in {yaml_file.name}: {yaml_type!r}. "
                        f"Valid values: omit (SQL query) or 'composite'."
                    )

                if yaml_type == "composite":
                    composite_def = CompositeQueryDefinition(**raw_data)
                    if composite_def.enabled:
                        self.composites[yaml_file.stem] = composite_def
                        logger.info(
                            f"Loaded composite: {composite_def.name} "
                            f"(trigger: {composite_def.trigger})"
                        )
                    else:
                        logger.info(f"Skipped disabled composite: {composite_def.name}")
                else:
                    query_def = QueryDefinition(**raw_data)
                    if query_def.enabled:
                        self.queries[yaml_file.stem] = query_def
                        logger.info(
                            f"Loaded query: {query_def.name} (trigger: {query_def.trigger})"
                        )
                    else:
                        logger.info(f"Skipped disabled query: {query_def.name}")

            except ValidationError as e:
                logger.error(f"Validation failed for {yaml_file}: {e}")
                raise
            except Exception as e:
                logger.error(f"Failed to load {yaml_file}: {e}")
                raise

        self._validate_composites()

        logger.info(
            f"Loaded {len(self.queries)} enabled queries and "
            f"{len(self.composites)} enabled composites"
        )

    def _validate_composites(self):
        """Cross-reference composite sub-query refs and parameter mappings."""
        token_pattern = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

        for composite_id, composite in self.composites.items():
            composite_param_names = {p.name for p in composite.parameters}

            for sub in composite.queries:
                ref_def = self.queries.get(sub.ref)
                if ref_def is None:
                    raise ValueError(
                        f"Composite '{composite_id}' sub-query label='{sub.label}' "
                        f"references unknown query '{sub.ref}'. "
                        f"Either the file does not exist, is disabled, or is itself a composite."
                    )

                user_params = {
                    p.name: p for p in ref_def.parameters if p.bind_from is None
                }
                required_param_names = {
                    p.name for p in user_params.values() if p.required
                }

                # Reject keys that don't match a sub-query parameter
                for key in sub.params.keys():
                    if key not in user_params:
                        raise ValueError(
                            f"Composite '{composite_id}' sub-query label='{sub.label}' "
                            f"maps unknown parameter '{key}' on referenced query '{sub.ref}'. "
                            f"Valid parameters: {sorted(user_params.keys())}"
                        )

                # Ensure all required params are provided
                missing = required_param_names - set(sub.params.keys())
                if missing:
                    raise ValueError(
                        f"Composite '{composite_id}' sub-query label='{sub.label}' "
                        f"missing required parameters {sorted(missing)} for query '{sub.ref}'."
                    )

                # Validate {Name} tokens point to composite-level params
                for key, value in sub.params.items():
                    if not isinstance(value, str):
                        continue
                    for token in token_pattern.findall(value):
                        if token not in composite_param_names:
                            raise ValueError(
                                f"Composite '{composite_id}' sub-query label='{sub.label}' "
                                f"params['{key}']='{value}' references unknown "
                                f"composite parameter '{{{token}}}'. "
                                f"Declared composite parameters: {sorted(composite_param_names)}"
                            )

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

    def get_composite_by_id(self, composite_id: str) -> Optional[CompositeQueryDefinition]:
        """Get composite by file name (without .yaml)."""
        return self.composites.get(composite_id)

    def get_composite_by_trigger(self, text: str) -> Optional[CompositeQueryDefinition]:
        """Find composite by trigger phrase.

        Note: Slack dispatch checks SQL queries first, so a SQL trigger that
        is a prefix of a composite trigger will win. Choose triggers that
        do not collide.
        """
        text_lower = text.lower().strip()
        for composite_def in self.composites.values():
            if text_lower.startswith(composite_def.trigger.lower()):
                return composite_def
        return None

    def get_all_composites(self) -> List[CompositeQueryDefinition]:
        """Get all enabled composites."""
        return list(self.composites.values())

    def reload(self):
        """Hot-reload query and composite definitions."""
        self.queries.clear()
        self.composites.clear()
        self._load_all_queries()
