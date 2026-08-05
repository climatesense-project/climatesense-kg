"""Tests for curated organization identity resolution."""

from pathlib import Path

import pytest

from climatesense_kg.config.models import CanonicalOrganization
from climatesense_kg.config.organizations import OrganizationCatalog


def _write_catalog(tmp_path: Path, organizations: str) -> Path:
    path = tmp_path / "organizations.ttl"
    path.write_text(
        """
@prefix schema: <http://schema.org/> .
@prefix org: <http://data.climatesense-project.eu/organization/> .

"""
        + organizations,
        encoding="utf-8",
    )
    return path


def test_repository_catalog_uses_readable_organization_iris() -> None:
    path = Path(__file__).parents[2] / "data" / "organizations.ttl"
    catalog = OrganizationCatalog(path)
    organization = CanonicalOrganization(
        name="Source label is not used",
        website="https://www.lessurligneurs.eu/article",
    )

    assert catalog.resolve(organization) == (
        "http://data.climatesense-project.eu/organization/les-surligneurs"
    )


def test_resolves_normalized_website(tmp_path: Path) -> None:
    path = _write_catalog(
        tmp_path,
        """
org:example a schema:Organization ;
    schema:name "Example" ;
    schema:url <https://www.example.org/about> .
""",
    )
    catalog = OrganizationCatalog(path)
    organization = CanonicalOrganization(
        name="Unknown", website="http://www.example.org/article"
    )

    assert catalog.resolve(organization) is not None
    assert organization.uri.endswith("/example")


def test_resolves_any_curated_website_for_one_organization(tmp_path: Path) -> None:
    path = _write_catalog(
        tmp_path,
        """
org:example a schema:Organization ;
    schema:name "Example Fact Check" ;
    schema:url <https://example.org>, <https://factcheck.example.net/about> .
""",
    )
    catalog = OrganizationCatalog(path)
    organization = CanonicalOrganization(
        name="Unrelated source label",
        website="http://www.factcheck.example.net/article",
    )

    assert catalog.resolve(organization) is not None
    assert organization.uri.endswith("/example")


def test_rejects_url_shared_by_different_organizations(tmp_path: Path) -> None:
    path = _write_catalog(
        tmp_path,
        """
org:first a schema:Organization ;
    schema:name "First" ;
    schema:url <https://example.org> .
org:second a schema:Organization ;
    schema:name "Second" ;
    schema:url <http://www.example.org/path> .
""",
    )

    with pytest.raises(ValueError, match="Normalized organization URL"):
        OrganizationCatalog(path)


def test_does_not_resolve_by_name(tmp_path: Path) -> None:
    path = _write_catalog(
        tmp_path,
        """
org:example a schema:Organization ;
    schema:name "Shared" ;
    schema:url <https://example.org> .
""",
    )
    catalog = OrganizationCatalog(path)
    organization = CanonicalOrganization(
        name="Shared", website="https://unrelated.example"
    )

    assert catalog.resolve(organization) is None
    with pytest.raises(ValueError, match="has not been resolved"):
        _ = organization.uri


def test_requires_one_name_per_organization(tmp_path: Path) -> None:
    path = _write_catalog(
        tmp_path,
        "org:example a schema:Organization ; schema:url <https://example.org> .",
    )

    with pytest.raises(ValueError, match="exactly one non-empty schema:name"):
        OrganizationCatalog(path)


def test_requires_a_website_per_organization(tmp_path: Path) -> None:
    path = _write_catalog(
        tmp_path,
        'org:example a schema:Organization ; schema:name "Example" .',
    )

    with pytest.raises(ValueError, match="at least one schema:url"):
        OrganizationCatalog(path)


def test_canonical_organization_requires_valid_website() -> None:
    with pytest.raises(ValueError, match=r"requires a valid HTTP\(S\) website URL"):
        CanonicalOrganization(name="Example", website="not a URL")
