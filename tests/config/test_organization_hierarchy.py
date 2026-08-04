"""Tests for organization hierarchy resolver."""

from importlib.resources import files
from pathlib import Path

from src.climatesense_kg.config.models import CanonicalOrganization
from src.climatesense_kg.config.organization_hierarchy import OrganizationHierarchy


def _write_hierarchy(tmp: Path, content: str) -> Path:
    path = tmp / "hierarchy.yaml"
    path.write_text(content)
    return path


class TestOrganizationHierarchy:
    def test_packaged_hierarchy_data_is_available(self) -> None:
        resource = files("climatesense_kg.data").joinpath("organization_hierarchy.yaml")

        assert resource.is_file()

    def test_resolves_parent_for_matching_domain(self, tmp_path: Path) -> None:
        path = _write_hierarchy(
            tmp_path,
            """
- parent:
    name: "AFP"
    website: "https://www.afp.com"
  children:
    - "factuel.afp.com"
""",
        )
        hierarchy = OrganizationHierarchy(path)
        org = CanonicalOrganization(
            name="AFP Factuel", website="https://factuel.afp.com"
        )

        hierarchy.resolve_parent(org)

        assert org.parent is not None
        assert org.parent.name == "AFP"
        assert org.parent.website == "https://www.afp.com"

    def test_no_parent_for_unmatched_domain(self, tmp_path: Path) -> None:
        path = _write_hierarchy(
            tmp_path,
            """
- parent:
    name: "AFP"
    website: "https://www.afp.com"
  children:
    - "factuel.afp.com"
""",
        )
        hierarchy = OrganizationHierarchy(path)
        org = CanonicalOrganization(name="BBC", website="https://www.bbc.com")

        hierarchy.resolve_parent(org)

        assert org.parent is None

    def test_no_parent_when_no_website(self, tmp_path: Path) -> None:
        path = _write_hierarchy(
            tmp_path,
            """
- parent:
    name: "AFP"
    website: "https://www.afp.com"
  children:
    - "factuel.afp.com"
""",
        )
        hierarchy = OrganizationHierarchy(path)
        org = CanonicalOrganization(name="Some Org")

        hierarchy.resolve_parent(org)

        assert org.parent is None

    def test_does_not_overwrite_existing_parent(self, tmp_path: Path) -> None:
        path = _write_hierarchy(
            tmp_path,
            """
- parent:
    name: "AFP"
    website: "https://www.afp.com"
  children:
    - "factuel.afp.com"
""",
        )
        hierarchy = OrganizationHierarchy(path)
        existing_parent = CanonicalOrganization(name="Custom Parent")
        org = CanonicalOrganization(
            name="AFP Factuel",
            website="https://factuel.afp.com",
            parent=existing_parent,
        )

        hierarchy.resolve_parent(org)

        assert org.parent is existing_parent

    def test_missing_file_loads_empty(self) -> None:
        hierarchy = OrganizationHierarchy("/nonexistent/path.yaml")
        org = CanonicalOrganization(name="Test", website="https://example.com")

        hierarchy.resolve_parent(org)

        assert org.parent is None

    def test_domain_matching_is_case_insensitive(self, tmp_path: Path) -> None:
        path = _write_hierarchy(
            tmp_path,
            """
- parent:
    name: "AFP"
    website: "https://www.afp.com"
  children:
    - "Factuel.AFP.com"
""",
        )
        hierarchy = OrganizationHierarchy(path)
        org = CanonicalOrganization(
            name="AFP Factuel", website="https://factuel.afp.com"
        )

        hierarchy.resolve_parent(org)

        assert org.parent is not None
        assert org.parent.name == "AFP"
