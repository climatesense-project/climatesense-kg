"""Climafacts data processor."""

from collections.abc import Generator, Iterable, Iterator
from typing import Any

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, Namespace

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
)
from ..utils.ratings import normalize_rating_label
from ..utils.text_processing import sanitize_url
from .base import BaseProcessor

SCHEMA_HTTPS = Namespace("https://schema.org/")
SCHEMA_HTTP = Namespace("http://schema.org/")
SCHEMA_NAMESPACES = (SCHEMA_HTTPS, SCHEMA_HTTP)


def _schema_terms(local_name: str) -> Generator[URIRef, None, None]:
    for namespace in SCHEMA_NAMESPACES:
        yield namespace[local_name]


class ClimafactsProcessor(BaseProcessor):
    """Processor for Climafacts RDF Turtle releases."""

    CLAIM_TEXT_PREDICATES = (
        "text",
        "abstract",
        "description",
        "headline",
        "name",
    )

    APPEARANCE_PREDICATES = (
        "url",
        "citation",
        "sameAs",
        "isBasedOn",
        "mainEntityOfPage",
        "subjectOf",
    )

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        graph = Graph()
        try:
            graph.parse(data=raw_data.decode("utf-8"), format="turtle")
        except Exception as exc:
            self.logger.error(f"Failed to parse Climafacts Turtle data: {exc}")
            return

        processed: set[URIRef] = set()
        for claim_review_type in _schema_terms("ClaimReview"):
            for review_uri in graph.subjects(RDF.type, claim_review_type):
                if not isinstance(review_uri, URIRef):
                    continue
                if review_uri in processed:
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

    def _build_claim_review(
        self, graph: Graph, review_uri: URIRef
    ) -> CanonicalClaimReview:
        review_url = self._literal_to_str(self._first_literal(graph, review_uri, "url"))
        if not review_url:
            raise ValueError("claim review missing schema:url")

        claim_text, appearances = self._extract_claim(graph, review_uri, review_url)

        claim = CanonicalClaim(text=claim_text, appearances=appearances)

        organization = self._extract_organization(graph, review_uri)
        rating = self._extract_rating(graph, review_uri)

        date_published = self._literal_to_str(
            self._first_literal(graph, review_uri, "datePublished")
        )
        if not date_published:
            date_published = self._literal_to_str(
                self._first_literal(graph, review_uri, "dateCreated")
            )

        language = self._literal_to_str(
            self._first_literal(graph, review_uri, "inLanguage")
        )

        review_text = self._literal_to_str(
            self._first_literal(graph, review_uri, "reviewBody")
        ) or self._literal_to_str(self._first_literal(graph, review_uri, "description"))

        return CanonicalClaimReview(
            claim=claim,
            review_url=review_url,
            organization=organization,
            date_published=date_published,
            language=language,
            rating=rating,
            review_text=review_text if review_text else None,
            source_type="climafacts",
            source_name=self.name,
        )

    def _extract_claim(
        self, graph: Graph, review_uri: URIRef, review_url: str
    ) -> tuple[str, list[str]]:
        literal_candidates: list[Literal] = []
        claim_nodes: set[URIRef] = set()

        for obj in self._objects(graph, review_uri, "claimReviewed"):
            if isinstance(obj, Literal):
                literal_candidates.append(obj)
            elif isinstance(obj, URIRef):
                claim_nodes.add(obj)

        literal_candidates.extend(self._collect_text_literals(graph, review_uri))

        visited_nodes: set[URIRef] = set()
        nodes_to_visit = list(claim_nodes)
        while nodes_to_visit:
            node = nodes_to_visit.pop()
            if node in visited_nodes:
                continue
            visited_nodes.add(node)

            literal_candidates.extend(self._collect_text_literals(graph, node))

            for nested_claim in self._objects(graph, node, "claimReviewed"):
                if isinstance(nested_claim, URIRef) and nested_claim not in claim_nodes:
                    claim_nodes.add(nested_claim)
                    nodes_to_visit.append(nested_claim)

        claim_text = self._pick_best_literal(literal_candidates)
        if not claim_text:
            raise ValueError("claim review missing claim text")

        appearances = self._collect_appearances(
            graph, review_uri, claim_nodes, review_url
        )
        return claim_text, appearances

    def _collect_text_literals(self, graph: Graph, node: URIRef) -> list[Literal]:
        literals: list[Literal] = []
        for predicate_name in self.CLAIM_TEXT_PREDICATES:
            for value in self._objects(graph, node, predicate_name):
                if isinstance(value, Literal):
                    literals.append(value)
        return literals

    def _collect_appearances(
        self,
        graph: Graph,
        review_uri: URIRef,
        claim_nodes: Iterable[URIRef],
        review_url: str,
    ) -> list[str]:
        seen: set[str] = set()
        appearances: list[str] = []

        def add_url(candidate: str | None) -> None:
            if not candidate:
                return
            sanitized = sanitize_url(candidate)
            if not sanitized:
                return
            if sanitized in seen:
                return
            seen.add(sanitized)
            appearances.append(sanitized)

        add_url(review_url)

        nodes: list[Any] = [review_uri]
        nodes.extend(claim_nodes)

        for node in nodes:
            for predicate_name in self.APPEARANCE_PREDICATES:
                for value in self._objects(graph, node, predicate_name):
                    candidate = self._literal_to_str(value)
                    add_url(candidate)

        return appearances

    def _extract_organization(
        self, graph: Graph, review_uri: URIRef
    ) -> CanonicalOrganization | None:
        publisher = None
        for candidate in self._objects(graph, review_uri, "publisher"):
            publisher = candidate
            break
        if publisher is None:
            for candidate in self._objects(graph, review_uri, "author"):
                publisher = candidate
                break
        if publisher is None:
            return None

        name = self._resource_label(graph, publisher)
        website = None

        if isinstance(publisher, URIRef):
            website = self._literal_to_str(
                self._first_literal(graph, publisher, "url")
            ) or str(publisher)
        elif isinstance(publisher, Literal):
            if not name:
                name = str(publisher)

        sanitized = sanitize_url(website) if website else None

        if not name and sanitized:
            name = sanitized

        if not name:
            return None

        return CanonicalOrganization(name=name, website=sanitized)

    def _extract_rating(
        self, graph: Graph, review_uri: URIRef
    ) -> CanonicalRating | None:
        for rating_node in self._objects(graph, review_uri, "reviewRating"):
            if isinstance(rating_node, Literal):
                original_label = self._literal_to_str(rating_node)
                normalized_label = normalize_rating_label(original_label)
                if normalized_label:
                    return CanonicalRating(
                        label=normalized_label, original_label=original_label
                    )
                continue

            name_literal = self._first_literal(graph, rating_node, "name")
            original_label = self._literal_to_str(name_literal)
            normalized_label = normalize_rating_label(original_label)
            if normalized_label:
                return CanonicalRating(
                    label=normalized_label, original_label=original_label
                )
        return None

    def _resource_label(self, graph: Graph, resource: Any) -> str | None:
        if isinstance(resource, Literal):
            text = str(resource).strip()
            return text if text else None

        if isinstance(resource, (URIRef | BNode)):
            for predicate_name in ("name", "legalName", "alternateName"):
                literal = self._first_literal(graph, resource, predicate_name)
                if literal is not None:
                    text = self._literal_to_str(literal)
                    if text:
                        return text

            for predicate in (RDFS.label,):
                literal = graph.value(resource, predicate)
                if isinstance(literal, Literal):
                    text = str(literal).strip()
                    if text:
                        return text

        return None

    def _pick_best_literal(self, literals: list[Literal]) -> str | None:
        if not literals:
            return None

        preferred_languages = ("en", "en-us", "en-gb")

        for lang in preferred_languages:
            for literal in literals:
                if literal.language and literal.language.lower() == lang:
                    text = str(literal).strip()
                    if text:
                        return text

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
        for predicate in _schema_terms(predicate_name):
            yield from graph.objects(node, predicate)

    def _literal_to_str(self, value: Any) -> str | None:
        if isinstance(value, Literal):
            text = str(value).strip()
            return text if text else None
        if isinstance(value, URIRef):
            text = str(value).strip()
            return text if text else None
        return None
