"""Organization hierarchy: resolves parent organizations from a YAML mapping."""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

import yaml

from .models import CanonicalOrganization

logger = logging.getLogger(__name__)


class OrganizationHierarchy:
    """Resolves parent organizations based on website domain mappings."""

    def __init__(self, mapping_path: str | Path) -> None:
        self._domain_to_parent: dict[str, CanonicalOrganization] = {}
        self._load(Path(mapping_path))

    def _load(self, path: Path) -> None:
        if not path.exists():
            logger.warning("Organization hierarchy file not found: %s", path)
            return

        with open(path, encoding="utf-8") as f:
            entries = yaml.safe_load(f)

        if not entries:
            return

        for entry in entries:
            parent_data = entry.get("parent", {})
            parent = CanonicalOrganization(
                name=parent_data["name"],
                website=parent_data.get("website"),
            )
            for domain in entry.get("children", []):
                self._domain_to_parent[domain.lower()] = parent

        logger.info(
            "Loaded organization hierarchy: %d child domains",
            len(self._domain_to_parent),
        )

    def resolve_parent(self, organization: CanonicalOrganization) -> None:
        """Set parent on the organization if its website domain matches."""

        if not organization.website or organization.parent is not None:
            return

        try:
            hostname = urlparse(organization.website).hostname
        except Exception:
            return

        if not hostname:
            return

        parent = self._domain_to_parent.get(hostname.lower())
        if parent:
            organization.parent = parent
