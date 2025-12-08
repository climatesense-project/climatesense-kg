"""Climafacts data processor."""

from collections.abc import Iterable, Iterator
from typing import Any

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF, Namespace

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalRating,
)
from ..utils.ratings import normalize_rating_label
from ..utils.text_processing import sanitize_url
from .base import BaseProcessor

SCHEMA_NS = Namespace("https://schema.org/")
SCHEMA_PERSON_TYPE = SCHEMA_NS["Person"]
SCHEMA_ORGANIZATION_TYPE = SCHEMA_NS["Organization"]
SCHEMA_CLAIM_TYPE = SCHEMA_NS["Claim"]
SCHEMA_CLAIM_REVIEW_TYPE = SCHEMA_NS["ClaimReview"]


class ClimafactsProcessor(BaseProcessor):
    """Processor for Climafacts RDF Turtle releases."""

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        graph = Graph()
        try:
            graph.parse(data=raw_data.decode("utf-8"), format="turtle")
        except Exception as exc:
            self.logger.error(f"Failed to parse Climafacts Turtle data: {exc}")
            return

        processed: set[URIRef] = set()
        for review_uri in graph.subjects(RDF.type, SCHEMA_CLAIM_REVIEW_TYPE):
            if not isinstance(review_uri, URIRef):
                continue
            if review_uri in processed:
                continue
            # Skip nodes that are also Claims (multilingual aggregates)
            if (review_uri, RDF.type, SCHEMA_CLAIM_TYPE) in graph:
                continue
            processed.add(review_uri)
            try:
                canonical_review = self._build_claim_review(graph, review_uri)
            except Exception as exc:
                self.logger.warning(
                    "Skipping claim review %s due to error: %s", review_uri, exc
                )
                continue
            yield canonical_review

    def _create_canonical_review(
        self,
        claim: CanonicalClaim,
        review_url: str,
        organization: CanonicalOrganization | None,
        date_published: str | None,
        language: str | None,
        rating: CanonicalRating | None,
        review_text: str | None,
        description: str | None,
        abstract: str | None,
        keywords: list[str],
        authors: list[CanonicalPerson],
        license_url: str | None,
    ) -> CanonicalClaimReview:
        """Create a CanonicalClaimReview with the given parameters."""
        return CanonicalClaimReview(
            claim=claim,
            review_url=review_url,
            organization=organization,
            date_published=date_published,
            language=language,
            rating=rating,
            review_text=review_text,
            description=description,
            abstract=abstract,
            keywords=keywords,
            authors=authors,
            license_url=license_url,
            source_type="climafacts",
            source_name=self.name,
        )

    def _build_claim_review(
        self, graph: Graph, review_uri: URIRef
    ) -> CanonicalClaimReview:
        review_url = self._literal_to_str(self._first_literal(graph, review_uri, "url"))
        if not review_url:
            raise ValueError("claim review missing schema:url")

        claim, metadata_nodes = self._extract_claim(graph, review_uri, review_url)

        organization = self._extract_organization(graph, review_uri)
        authors = self._extract_people(graph, review_uri)
        rating = self._extract_rating(graph, review_uri)

        date_published = self._find_creation_date(graph, metadata_nodes)

        review_body_literals = self._collect_literals(graph, review_uri, "reviewBody")
        review_text_literals = self._collect_literals(graph, review_uri, "text")
        review_description_literals = self._collect_literals(
            graph, review_uri, "description"
        )
        review_abstract_literals = self._collect_literals(graph, review_uri, "abstract")

        # Combine all review text candidates
        review_text_candidates = review_body_literals + review_text_literals
        if not review_text_candidates:
            review_text_candidates = (
                review_description_literals + review_abstract_literals
            )

        review_text = self._pick_best_literal(review_text_candidates)
        review_description = self._pick_best_literal(review_description_literals)
        review_abstract = self._pick_best_literal(review_abstract_literals)

        review_keywords = self._collect_keywords(graph, [review_uri])
        license_url = self._extract_license(graph, metadata_nodes)

        # Extract language from inLanguage
        language = self._literal_to_str(
            self._first_literal(graph, review_uri, "inLanguage")
        )
        return self._create_canonical_review(
            claim=claim,
            review_url=review_url,
            organization=organization,
            date_published=date_published,
            language=language,
            rating=rating,
            review_text=review_text if review_text else None,
            description=review_description if review_description else None,
            abstract=review_abstract if review_abstract else None,
            keywords=review_keywords,
            authors=authors,
            license_url=license_url,
        )

    def _extract_claim(
        self, graph: Graph, review_uri: URIRef, review_url: str
    ) -> tuple[CanonicalClaim, list[Any]]:
        claim_nodes: set[URIRef | BNode] = set()
        metadata_nodes: list[Any] = [review_uri]

        # Get claimReviewed objects
        for obj in self._objects(graph, review_uri, "claimReviewed"):
            if isinstance(obj, (URIRef | BNode)):
                claim_nodes.add(obj)

        # Handle claim nodes
        claim_texts: list[Literal] = []
        headlines: list[Literal] = []

        for node in claim_nodes:
            metadata_nodes.append(node)
            # Collect text
            for value in self._objects(graph, node, "text"):
                if isinstance(value, Literal):
                    claim_texts.append(value)
            for value in self._objects(graph, node, "headline"):
                if isinstance(value, Literal):
                    headlines.append(value)

        claim_text = self._pick_best_literal(claim_texts)
        if not claim_text:
            raise ValueError("claim review missing claim text")

        headline = self._pick_best_literal(headlines)
        appearances = self._collect_appearances(
            graph, review_uri, claim_nodes, review_url
        )
        all_keywords = self._collect_keywords(graph, metadata_nodes)

        claim = CanonicalClaim(
            text=claim_text,
            headline=headline if headline else None,
            appearances=appearances,
            keywords=all_keywords,
        )

        return claim, metadata_nodes

    def _collect_literals(
        self, graph: Graph, node: Any, predicate_name: str
    ) -> list[Literal]:
        literals: list[Literal] = []
        for value in self._objects(graph, node, predicate_name):
            if isinstance(value, Literal):
                literals.append(value)
        return literals

    def _collect_appearances(
        self,
        graph: Graph,
        review_uri: URIRef,
        claim_nodes: Iterable[URIRef | BNode],
        review_url: str,
    ) -> list[str]:
        seen: set[str] = set()
        appearances: list[str] = []

        def add_url(candidate: str | None) -> None:
            if not candidate:
                return
            sanitized = sanitize_url(candidate)
            if not sanitized or sanitized in seen:
                return
            seen.add(sanitized)
            appearances.append(sanitized)

        add_url(review_url)

        # Add citations from claim nodes
        for node in claim_nodes:
            for value in self._objects(graph, node, "citation"):
                candidate = self._literal_to_str(value)
                add_url(candidate)

        return appearances

    def _collect_keywords(self, graph: Graph, nodes: Iterable[Any]) -> list[str]:
        seen: set[str] = set()
        keywords: list[str] = []
        for node in nodes:
            if not isinstance(node, (URIRef | BNode)):
                continue
            for value in self._objects(graph, node, "keywords"):
                text = self._literal_to_str(value)
                if not text:
                    continue
                parts = [part.strip() for part in text.split(",") if part.strip()]
                if not parts:
                    continue
                for part in parts:
                    lowered = part.casefold()
                    if lowered in seen:
                        continue
                    seen.add(lowered)
                    keywords.append(part)
        return keywords

    def _extract_people(
        self, graph: Graph, review_uri: URIRef
    ) -> list[CanonicalPerson]:
        people: list[CanonicalPerson] = []
        seen: set[tuple[str, str | None, str | None]] = set()
        for candidate in self._objects(graph, review_uri, "author"):
            person = self._build_person(graph, candidate, role="author")
            if not person:
                continue
            key = (person.name, person.website, person.role)
            if key in seen:
                continue
            seen.add(key)
            people.append(person)
        return people

    def _build_person(
        self, graph: Graph, resource: Any, role: str | None = None
    ) -> CanonicalPerson | None:
        if isinstance(resource, Literal):
            name = self._literal_to_str(resource)
            if not name:
                return None
            return CanonicalPerson(name=name, role=role)

        if not isinstance(resource, (URIRef | BNode)):
            return None

        if self._is_organization(graph, resource):
            return None

        name = self._resource_label(graph, resource)
        website_literal = self._first_literal(graph, resource, "url")
        website = self._literal_to_str(website_literal)
        sanitized = sanitize_url(website) if website else None

        if not name and sanitized:
            name = sanitized

        if not name and isinstance(resource, URIRef):
            name = str(resource)

        if not name:
            return None

        source_uri = str(resource) if isinstance(resource, URIRef) else None
        return CanonicalPerson(
            name=name, website=sanitized, role=role, source_uri=source_uri
        )

    def _build_organization(
        self, graph: Graph, resource: Any
    ) -> CanonicalOrganization | None:
        if isinstance(resource, Literal):
            name = self._literal_to_str(resource)
            if not name:
                return None
            return CanonicalOrganization(name=name)

        if not isinstance(resource, (URIRef | BNode)):
            return None

        if self._is_person(graph, resource):
            return None

        name = self._resource_label(graph, resource)
        website_literal = self._first_literal(graph, resource, "url")
        website = self._literal_to_str(website_literal)
        sanitized = sanitize_url(website) if website else None

        if not name and sanitized:
            name = sanitized

        if not name and isinstance(resource, URIRef):
            name = str(resource)

        if not name:
            return None

        return CanonicalOrganization(name=name, website=sanitized)

    def _resource_types(self, graph: Graph, resource: Any) -> set[URIRef]:
        types: set[URIRef] = set()
        if isinstance(resource, (URIRef | BNode)):
            for value in graph.objects(resource, RDF.type):
                if isinstance(value, URIRef):
                    types.add(value)
        return types

    def _is_person(self, graph: Graph, resource: Any) -> bool:
        if isinstance(resource, Literal):
            return True
        if not isinstance(resource, (URIRef | BNode)):
            return False
        return SCHEMA_PERSON_TYPE in self._resource_types(graph, resource)

    def _is_organization(self, graph: Graph, resource: Any) -> bool:
        if not isinstance(resource, (URIRef | BNode)):
            return False
        return SCHEMA_ORGANIZATION_TYPE in self._resource_types(graph, resource)

    def _extract_license(self, graph: Graph, nodes: Iterable[Any]) -> str | None:
        for node in nodes:
            if not isinstance(node, (URIRef | BNode)):
                continue
            literal = self._first_literal(graph, node, "license")
            if not literal:
                continue
            text = self._literal_to_str(literal)
            if not text:
                continue
            sanitized = sanitize_url(text) or text.strip()
            if sanitized:
                return sanitized
        return None

    def _literal_to_float(self, literal: Any) -> float | None:
        text = self._literal_to_str(literal)
        if text is None:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _extract_organization(
        self, graph: Graph, review_uri: URIRef
    ) -> CanonicalOrganization | None:
        for candidate in self._objects(graph, review_uri, "publisher"):
            organization = self._build_organization(graph, candidate)
            if organization:
                return organization

        for candidate in self._objects(graph, review_uri, "author"):
            if self._is_person(graph, candidate):
                continue
            organization = self._build_organization(graph, candidate)
            if organization:
                return organization
        return None

    def _extract_rating(
        self, graph: Graph, review_uri: URIRef
    ) -> CanonicalRating | None:
        for rating_node in self._objects(graph, review_uri, "reviewRating"):
            if not isinstance(rating_node, (URIRef | BNode)):
                continue

            name_literal = self._first_literal(graph, rating_node, "name")
            original_label = self._literal_to_str(name_literal)
            normalized_label = normalize_rating_label(original_label)
            if not normalized_label:
                continue

            explanation_literals: list[Literal] = []
            for value in self._objects(graph, rating_node, "ratingExplanation"):
                if isinstance(value, Literal):
                    explanation_literals.append(value)

            explanation = self._pick_best_literal(explanation_literals)
            rating_value = self._literal_to_float(
                self._first_literal(graph, rating_node, "ratingValue")
            )
            best_rating = self._literal_to_float(
                self._first_literal(graph, rating_node, "bestRating")
            )
            worst_rating = self._literal_to_float(
                self._first_literal(graph, rating_node, "worstRating")
            )

            return CanonicalRating(
                label=normalized_label,
                original_label=original_label,
                explanation=explanation,
                rating_value=rating_value,
                best_rating=best_rating,
                worst_rating=worst_rating,
            )
        return None

    def _resource_label(self, graph: Graph, resource: Any) -> str | None:
        if isinstance(resource, Literal):
            text = str(resource).strip()
            return text if text else None

        if isinstance(resource, (URIRef | BNode)):
            literal = self._first_literal(graph, resource, "name")
            if literal is not None:
                text = self._literal_to_str(literal)
                if text:
                    return text

        return None

    def _pick_best_literal(self, literals: list[Literal]) -> str | None:
        if not literals:
            return None

        preferred_languages = ("en", "en-us", "en-gb")

        # Check preferred languages first
        for lang in preferred_languages:
            for literal in literals:
                if literal.language and literal.language.lower() == lang:
                    text = str(literal).strip()
                    if text:
                        return text

        # Fallback to first non-empty literal
        for literal in literals:
            text = str(literal).strip()
            if text:
                return text

        return None

    def _first_literal(
        self, graph: Graph, node: Any, predicate_name: str
    ) -> Literal | None:
        for value in self._objects(graph, node, predicate_name):
            if isinstance(value, Literal):
                return value
        return None

    def _objects(self, graph: Graph, node: Any, predicate_name: str) -> Iterator[Any]:
        yield from graph.objects(node, SCHEMA_NS[predicate_name])

    def _literal_to_str(self, value: Any) -> str | None:
        if isinstance(value, Literal):
            text = str(value).strip()
            return text if text else None
        return None

    def _find_creation_date(self, graph: Graph, nodes: list[Any]) -> str | None:
        for node in nodes:
            if not isinstance(node, (URIRef | BNode)):
                continue
            date_lit = self._first_literal(graph, node, "dateCreated")
            if date_lit:
                date_str = self._literal_to_str(date_lit)
                if date_str:
                    return date_str
        return None
