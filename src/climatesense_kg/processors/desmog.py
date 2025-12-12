"""Desmog data processor for ClimateSense KG."""

from collections.abc import Iterator
from html import unescape
import re
from typing import Any
from urllib.parse import urlparse

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, Namespace
from rdflib.term import Node

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
)
from ..utils.text_processing import sanitize_url
from .base import BaseProcessor

SCHEMA = Namespace("https://schema.org/")


class DesmogProcessor(BaseProcessor):
    """Processor for DeSmog claims stored as RDF Turtle."""

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        """Parse RDF content and yield canonical claim reviews."""
        graph = Graph()

        try:
            graph.parse(data=raw_data.decode("utf-8"), format="turtle")
        except Exception as exc:  # pragma: no cover - logged diagnostic
            self.logger.error(f"Failed to parse RDF Turtle data: {exc}")
            return

        for claim_uri in graph.subjects(RDF.type, SCHEMA.Claim):
            if not isinstance(claim_uri, URIRef):
                self.logger.warning(
                    "Skipping claim %s because it is not a URIRef", claim_uri
                )
                continue
            try:
                canonical_review = self._build_claim_review(graph, claim_uri)
            except Exception as exc:
                self.logger.warning(
                    "Skipping claim %s due to error: %s", claim_uri, exc
                )
                continue

            yield canonical_review

    def _build_claim_review(
        self, graph: Graph, claim_uri: URIRef
    ) -> CanonicalClaimReview:
        """Convert a schema:Claim node into a canonical review."""
        claim_text = self._literal_to_str(graph.value(claim_uri, SCHEMA.abstract))
        if not claim_text:
            raise ValueError("claim missing schema:abstract")

        review_url = self._literal_to_str(graph.value(claim_uri, SCHEMA.url))
        if not review_url:
            raise ValueError("claim missing schema:url")

        claim = CanonicalClaim(
            text=claim_text,
            appearances=self._extract_appearances(graph, claim_uri, review_url),
        )

        organization = self._build_organization(graph, claim_uri)

        # Get date_published from the archivedAt source
        date_published = None
        archived_ref = graph.value(claim_uri, SCHEMA.archivedAt)
        if archived_ref:
            date_published = self._literal_to_str(
                graph.value(archived_ref, SCHEMA.datePublished)
            )

        review = CanonicalClaimReview(
            claim=claim,
            review_url=review_url,
            organization=organization,
            date_published=date_published,
            language=self._literal_to_str(graph.value(claim_uri, SCHEMA.inLanguage)),
            rating=None,
            source_type="desmog",
            source_name=self.name,
        )

        review.review_text = self._literal_to_str(
            graph.value(claim_uri, SCHEMA.description)
        )

        return review

    def _extract_appearances(
        self, graph: Graph, claim_uri: URIRef, review_url: str
    ) -> list[str]:
        appearances: list[str] = []
        seen: set[str] = set()

        def add_url(candidate: str | None) -> None:
            if not candidate:
                return
            normalized = candidate.strip()
            if normalized.startswith("//"):
                normalized = f"https:{normalized}"
            if not normalized.lower().startswith(("http://", "https://")):
                return
            sanitized = sanitize_url(normalized)
            if not sanitized:
                return
            if sanitized in seen:
                return
            seen.add(sanitized)
            appearances.append(sanitized)

        for archived_ref in graph.objects(claim_uri, SCHEMA.archivedAt):
            citation_literal = graph.value(archived_ref, SCHEMA.citation)
            for url in self._extract_urls_from_literal(citation_literal):
                add_url(url)

            archive_url = self._literal_to_str(graph.value(archived_ref, SCHEMA.url))
            if archive_url:
                add_url(archive_url)
            elif isinstance(archived_ref, URIRef):
                add_url(str(archived_ref))

        add_url(review_url)

        return appearances

    def _build_organization(
        self, graph: Graph, claim_uri: URIRef
    ) -> CanonicalOrganization | None:
        publisher = graph.value(claim_uri, SCHEMA.publisher)
        if publisher is None:
            return None

        publisher_name = self._get_resource_label(graph, publisher)
        publisher_url = None

        if isinstance(publisher, URIRef):
            publisher_url = str(publisher)
            if not publisher_name:
                publisher_name = self._name_from_url(publisher_url)

        if not publisher_name and isinstance(publisher, Literal):
            publisher_name = str(publisher)

        if not publisher_name:
            return None

        return CanonicalOrganization(name=publisher_name, website=publisher_url)

    def _get_resource_label(self, graph: Graph, resource: Node) -> str | None:
        if isinstance(resource, Literal):
            return str(resource)

        label = graph.value(resource, SCHEMA.name)
        if isinstance(label, Literal):
            return str(label)

        label = graph.value(resource, RDFS.label)
        if isinstance(label, Literal):
            return str(label)

        return None

    def _literal_to_str(self, value: Any) -> str | None:
        if isinstance(value, Literal):
            return str(value)
        if isinstance(value, URIRef):
            return str(value)
        if value is None:
            return None
        return str(value)

    def _extract_urls_from_literal(self, literal: Any) -> list[str]:
        text = self._literal_to_str(literal)
        if not text:
            return []

        decoded = unescape(text)
        candidates: list[str] = []

        candidates.extend(
            re.findall(r"href\s*=\s*['\"]([^'\"]+)['\"]", decoded, flags=re.IGNORECASE)
        )

        candidates.extend(
            re.findall(r"https?://[\w\-._~:/?#\[\]@!$&'()*+,;=%]+", decoded)
        )

        unique: list[str] = []
        seen: set[str] = set()
        for url in candidates:
            if url in seen:
                continue
            seen.add(url)
            unique.append(url)

        return unique

    def _name_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        hostname = parsed.netloc or parsed.path
        return hostname.rstrip("/") if hostname else url
