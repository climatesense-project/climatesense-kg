"""Curated organization catalog and extraction identity resolution."""

from __future__ import annotations

import logging
from pathlib import Path

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF

from ..domain import CanonicalOrganization, OrganizationReference
from ..utils.text_processing import normalize_organization_url

logger = logging.getLogger(__name__)

SCHEMA_ORGANIZATION = URIRef("http://schema.org/Organization")
SCHEMA_NAME = URIRef("http://schema.org/name")
SCHEMA_URL = URIRef("http://schema.org/url")

ORGANIZATION_URI_PREFIX = "http://data.climatesense-project.eu/organization/"
ORGANIZATION_CATALOG_PATH = Path("data/organizations.ttl")
ORGANIZATION_SOURCE_NAME = "organizations"


class OrganizationCatalog:
    """Load canonical organization identities from a checked-in Turtle graph."""

    def __init__(self, path: str | Path) -> None:
        catalog_path = Path(path)
        if not catalog_path.is_file():
            raise FileNotFoundError(f"Organization catalog not found: {catalog_path}")

        graph = Graph()
        try:
            graph.parse(catalog_path, format="turtle")
        except Exception as exc:
            raise ValueError(
                f"Failed to parse organization catalog {catalog_path}: {exc}"
            ) from exc

        self._urls: dict[str, CanonicalOrganization] = {}
        self._build_index(graph, catalog_path)

    def _build_index(self, graph: Graph, catalog_path: Path) -> None:
        organization_subjects = {
            subject
            for subject in graph.subjects(RDF.type, SCHEMA_ORGANIZATION)
            if isinstance(subject, URIRef)
            and str(subject).startswith(ORGANIZATION_URI_PREFIX)
        }
        if not organization_subjects:
            raise ValueError(
                "Organization catalog must contain at least one ClimateSense "
                "schema:Organization"
            )

        for organization_uri in organization_subjects:
            names = list(graph.objects(organization_uri, SCHEMA_NAME))
            if (
                len(names) != 1
                or not isinstance(names[0], Literal)
                or not str(names[0]).strip()
            ):
                raise ValueError(
                    f"Organization {organization_uri} must have exactly one non-empty "
                    "schema:name"
                )

            websites = list(graph.objects(organization_uri, SCHEMA_URL))
            if not websites:
                raise ValueError(
                    f"Organization {organization_uri} must have at least one schema:url"
                )
            normalized_websites: list[str] = []
            for website in websites:
                if not isinstance(website, URIRef):
                    raise ValueError(
                        f"Organization {organization_uri} has a non-IRI schema:url"
                    )
                normalized_url = normalize_organization_url(str(website))
                if not normalized_url:
                    raise ValueError(
                        f"Organization {organization_uri} has an invalid schema:url"
                    )
                normalized_websites.append(normalized_url)

            organization = CanonicalOrganization(
                uri=str(organization_uri),
                name=str(names[0]).strip(),
                website=sorted(normalized_websites)[0],
            )
            for normalized_url in normalized_websites:
                existing = self._urls.get(normalized_url)
                if existing is not None and existing.uri != organization.uri:
                    raise ValueError(
                        f"Normalized organization URL {normalized_url!r} is shared "
                        f"by {existing.uri} and {organization_uri}"
                    )
                self._urls[normalized_url] = organization

        logger.info(
            "Loaded %d canonical organizations from %s",
            len(organization_subjects),
            catalog_path,
        )

    def resolve(
        self, organization: OrganizationReference
    ) -> CanonicalOrganization | None:
        """Return the curated organization matching one source reference."""

        return self._urls.get(organization.website)
