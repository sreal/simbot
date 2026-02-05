"""Load and validate blob check definitions from YAML files."""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional
import yaml
from pydantic import ValidationError

from .models import BlobCheckDefinition


logger = logging.getLogger(__name__)


class BlobCheckLoader:
    """Load blob check definitions from YAML files."""

    # Only load files matching this prefix
    FILE_PREFIX = "blob_"

    def __init__(self, definitions_dir: Optional[str] = None):
        """
        Initialize blob check loader.

        Args:
            definitions_dir: Directory containing blob check YAML files.
                            If None, uses QUERY_DEFINITIONS_PATH env var.
                            Raises ValueError if neither is provided.
        """
        if definitions_dir is None:
            definitions_dir = os.getenv("QUERY_DEFINITIONS_PATH")

        if definitions_dir is None:
            raise ValueError(
                "definitions_dir must be provided or QUERY_DEFINITIONS_PATH env var must be set"
            )

        self.definitions_dir = Path(definitions_dir)
        self.checks: Dict[str, BlobCheckDefinition] = {}

        if not self.definitions_dir.exists():
            logger.warning(f"Definitions directory not found: {self.definitions_dir}")
            return

        self._load_all_checks()

    def _load_all_checks(self):
        """
        Load all blob_*.yaml files and validate with Pydantic.

        Only loads files starting with 'blob_' prefix.
        Skips disabled checks (enabled: false).

        Raises:
            ValidationError: If YAML validation fails
            Exception: If file loading fails
        """
        if not self.definitions_dir.is_dir():
            logger.error(f"Definitions path is not a directory: {self.definitions_dir}")
            return

        # Only load files starting with blob_
        yaml_files = list(self.definitions_dir.glob(f"{self.FILE_PREFIX}*.yaml")) + list(
            self.definitions_dir.glob(f"{self.FILE_PREFIX}*.yml")
        )

        if not yaml_files:
            logger.info(f"No blob check YAML files found in {self.definitions_dir}")
            return

        logger.info(f"Loading blob checks from {self.definitions_dir}")

        for yaml_file in yaml_files:
            try:
                with open(yaml_file, 'r') as f:
                    raw_data = yaml.safe_load(f)

                if not raw_data:
                    logger.warning(f"Empty YAML file: {yaml_file}")
                    continue

                # Validate with Pydantic
                check_def = BlobCheckDefinition(**raw_data)

                if check_def.enabled:
                    self.checks[yaml_file.stem] = check_def
                    logger.info(
                        f"Loaded blob check: {check_def.name} "
                        f"(trigger: {check_def.trigger})"
                    )
                else:
                    logger.info(f"Skipped disabled blob check: {check_def.name}")

            except ValidationError as e:
                logger.error(f"Validation failed for {yaml_file}: {e}")
                raise
            except Exception as e:
                logger.error(f"Failed to load {yaml_file}: {e}")
                raise

        logger.info(f"Loaded {len(self.checks)} enabled blob checks")

    def get_by_id(self, check_id: str) -> Optional[BlobCheckDefinition]:
        """Get blob check by file name (without .yaml)."""
        return self.checks.get(check_id)

    def get_by_trigger(self, text: str) -> Optional[BlobCheckDefinition]:
        """Find blob check by trigger phrase."""
        text_lower = text.lower().strip()
        for check_def in self.checks.values():
            if text_lower.startswith(check_def.trigger.lower()):
                return check_def
        return None

    def get_all(self) -> List[BlobCheckDefinition]:
        """Get all enabled blob checks."""
        return list(self.checks.values())

    def reload(self):
        """Hot-reload blob check definitions."""
        self.checks.clear()
        self._load_all_checks()
