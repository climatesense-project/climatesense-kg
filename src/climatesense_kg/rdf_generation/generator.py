"""RDF generator which converts canonical data to RDF format."""

import fcntl
import logging
import os
from pathlib import Path
import stat
import tempfile
from typing import Any
from urllib.parse import quote

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, XSD

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalRating,
)
from ..utils.ratings import VALID_NORMALIZED_RATINGS
from ..utils.text_processing import sanitize_url

logger = logging.getLogger(__name__)


class RDFGenerator:
    """RDF generator using rdflib."""

    # Supported output formats
    SUPPORTED_FORMATS = {
        "turtle",
        "ttl",
        "xml",
        "rdf",
        "rdf/xml",
        "n3",
        "nt",
        "json-ld",
        "jsonld",
        "trig",
        "pretty-xml",
        "nquads",
        "nq",
    }

    def __init__(self, base_uri: str, **kwargs: Any):
        """
        Initialize RDF generator.

        Args:
            base_uri: Base URI for generated RDF
            **kwargs: Additional configuration options
        """
        self.base_uri = base_uri.rstrip("/")
        self.config = kwargs
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self._setup_namespaces()

        self.graph = Graph()
        self._bind_namespaces()

    def _setup_namespaces(self) -> None:
        """Setup common RDF namespaces."""
        self.namespaces = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "owl": "http://www.w3.org/2002/07/owl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "dc": "http://purl.org/dc/elements/1.1/",
            "schema": "http://schema.org/",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "cimple": "http://data.cimple.eu/ontology#",
            "climatesense": f"{self.base_uri}/ontology#",
            "base": f"{self.base_uri}/",
        }

        self.SCHEMA = Namespace(self.namespaces["schema"])
        self.CIMPLE = Namespace(self.namespaces["cimple"])
        self.CLIMATESENSE = Namespace(self.namespaces["climatesense"])
        self.SKOS = Namespace(self.namespaces["skos"])

    def _bind_namespaces(self) -> None:
        """Bind namespaces to graph."""
        for prefix, uri in self.namespaces.items():
            self.graph.bind(prefix, Namespace(uri))

    def generate(
        self, claim_reviews: list[CanonicalClaimReview], output_format: str
    ) -> str:
        """
        Generate RDF from canonical claim reviews.

        Args:
            claim_reviews: List of canonical claim reviews
            output_format: Output format (turtle, xml, n3, nt, json-ld, etc.)

        Returns:
            str: RDF serialization in the specified format

        Raises:
            ValueError: If output format is not supported
        """
        rdf_content, _ = self._generate_with_results(claim_reviews, output_format)
        return rdf_content

    def _generate_with_results(
        self, claim_reviews: list[CanonicalClaimReview], output_format: str
    ) -> tuple[str, list[str]]:
        """Generate RDF and return its content and successfully generated review URIs."""
        if output_format.lower() not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format: {output_format}. "
                f"Supported formats: {sorted(self.SUPPORTED_FORMATS)}"
            )

        self.graph = Graph()
        self._bind_namespaces()

        # Track URIs for deduplication within this generation
        generated_uris: set[str] = set()
        successful_review_uris: list[str] = []

        for claim_review in claim_reviews:
            try:
                self._generate_claim_review_rdf(claim_review, generated_uris)
            except Exception as e:
                self.logger.error(
                    f"Error generating RDF for claim review {claim_review.uri}: {e}"
                )
                continue
            successful_review_uris.append(claim_review.uri)

        return self._serialize_graph(output_format), successful_review_uris

    def _generate_entity_enrichment_with_results(
        self,
        claim_reviews: list[CanonicalClaimReview],
        output_format: str,
        entity_sources: frozenset[str],
        property_keys: tuple[str, ...],
    ) -> tuple[str, list[str]]:
        """Generate entity-linking RDF and return contributing review URIs."""
        if output_format.lower() not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format: {output_format}. "
                f"Supported formats: {sorted(self.SUPPORTED_FORMATS)}"
            )

        self.graph = Graph()
        self._bind_namespaces()
        successful_review_uris: list[str] = []

        for claim_review in claim_reviews:
            if not self.has_entity_enrichment(claim_review, entity_sources):
                continue
            try:
                self._generate_entity_enrichment_rdf(
                    claim_review, entity_sources, property_keys
                )
            except Exception as exc:
                self.logger.error(
                    "Error generating entity enrichment RDF for claim review %s: %s",
                    claim_review.uri,
                    exc,
                )
                continue
            successful_review_uris.append(claim_review.uri)

        return self._serialize_graph(output_format), successful_review_uris

    def _serialize_graph(self, output_format: str) -> str:
        """Serialize the current graph using the public output format name."""
        rdflib_format = self._normalize_format_name(output_format)
        try:
            return self.graph.serialize(format=rdflib_format)
        except Exception as exc:
            raise ValueError(
                f"Failed to serialize RDF as {output_format}: {exc}"
            ) from exc

    def save(
        self,
        claim_reviews: list[CanonicalClaimReview],
        output_path: str | Path,
        output_format: str,
    ) -> list[str]:
        """
        Generate RDF and save to file.

        Args:
            claim_reviews: List of canonical claim reviews
            output_path: Output file path
            output_format: Output format (turtle, xml, n3, nt, json-ld, etc.)

        Returns:
            URIs of claim reviews that were successfully added to the RDF graph
        """
        rdf_content, successful_review_uris = self._generate_with_results(
            claim_reviews, output_format
        )

        self._save_rdf_content(rdf_content, output_path, output_format)
        return successful_review_uris

    def save_entity_enrichment(
        self,
        claim_reviews: list[CanonicalClaimReview],
        output_path: str | Path,
        output_format: str,
        *,
        entity_sources: frozenset[str],
        property_keys: tuple[str, ...],
    ) -> list[str]:
        """Generate and save RDF owned by one entity enrichment graph."""
        rdf_content, successful_review_uris = (
            self._generate_entity_enrichment_with_results(
                claim_reviews,
                output_format,
                entity_sources,
                property_keys,
            )
        )
        self._save_rdf_content(rdf_content, output_path, output_format)
        return successful_review_uris

    def _save_rdf_content(
        self, rdf_content: str, output_path: str | Path, output_format: str
    ) -> None:
        """Atomically save serialized RDF content."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lock_path = output_path.with_suffix(f"{output_path.suffix}.lock")
        with lock_path.open("a+b") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            temp_path: Path | None = None
            try:
                output_mode = (
                    stat.S_IMODE(output_path.stat().st_mode)
                    if output_path.exists()
                    else 0o644
                )
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    dir=output_path.parent,
                    prefix=f".{output_path.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as temp_file:
                    temp_path = Path(temp_file.name)
                    os.fchmod(temp_file.fileno(), output_mode)
                    temp_file.write(rdf_content)
                    temp_file.flush()
                    os.fsync(temp_file.fileno())

                os.replace(temp_path, output_path)
                temp_path = None
            finally:
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

        self.logger.info(
            f"RDF saved to {output_path} in {output_format} format with {len(self.graph)} triples"
        )

    @staticmethod
    def has_entity_enrichment(
        claim_review: CanonicalClaimReview, entity_sources: frozenset[str]
    ) -> bool:
        """Return whether a review contains an entity owned by the given sources."""
        entities = [*claim_review.claim.entities, *claim_review.entities_in_review]
        return any(entity.get("source") in entity_sources for entity in entities)

    def _generate_entity_enrichment_rdf(
        self,
        claim_review: CanonicalClaimReview,
        entity_sources: frozenset[str],
        property_keys: tuple[str, ...],
    ) -> None:
        """Generate entity mention and property triples for one review."""
        claim_uri = URIRef(self.get_full_uri(claim_review.claim.uri))
        review_uri = URIRef(self.get_full_uri(claim_review.uri))
        self._add_entity_mentions(
            claim_uri, claim_review.claim.entities, entity_sources, property_keys
        )
        self._add_entity_mentions(
            review_uri,
            claim_review.entities_in_review,
            entity_sources,
            property_keys,
        )

    def _add_entity_mentions(
        self,
        subject_uri: URIRef,
        entities: list[dict[str, Any]],
        entity_sources: frozenset[str],
        property_keys: tuple[str, ...],
    ) -> None:
        """Add provider-owned mention assertions and entity descriptions."""
        for entity in entities:
            if entity.get("source") not in entity_sources or not entity.get("uri"):
                continue
            entity_uri = URIRef(entity["uri"])
            self.graph.add((subject_uri, self.SCHEMA.mentions, entity_uri))
            for property_key in property_keys:
                self._add_entity_properties(entity_uri, entity.get(property_key))

    def _normalize_format_name(self, format_name: str) -> str:
        """
        Normalize format name for rdflib.

        Args:
            format_name: Input format name

        Returns:
            str: Normalized format name for rdflib
        """
        format_mapping = {
            "ttl": "turtle",
            "rdf": "xml",
            "rdf/xml": "xml",
            "jsonld": "json-ld",
            "nq": "nquads",
        }

        normalized = format_name.lower()
        return format_mapping.get(normalized, normalized)

    def _generate_people_rdf(
        self, people: list[CanonicalPerson], generated_uris: set[str]
    ) -> list[URIRef]:
        """Generate RDF nodes for canonical people and return their URIs."""

        person_nodes: list[URIRef] = []
        for person in people or []:
            node = self._generate_person_rdf(person, generated_uris)
            if node is not None:
                person_nodes.append(node)
        return person_nodes

    def _generate_person_rdf(
        self, person: CanonicalPerson, generated_uris: set[str]
    ) -> URIRef | None:
        """Create RDF triples for a person and return the node used."""

        if not person.name and not person.website:
            return None

        person_uri = URIRef(self.get_full_uri(person.uri))
        if str(person_uri) in generated_uris:
            return person_uri

        generated_uris.add(str(person_uri))

        self.graph.add((person_uri, RDF.type, self.SCHEMA.Person))
        if person.name:
            self.graph.add((person_uri, self.SCHEMA.name, Literal(person.name)))

        if person.website:
            sanitized = sanitize_url(person.website)
            if sanitized:
                self.graph.add((person_uri, self.SCHEMA.url, URIRef(sanitized)))

        if person.role:
            self.graph.add((person_uri, self.SCHEMA.jobTitle, Literal(person.role)))

        if person.source_uri:
            sanitized_source = sanitize_url(person.source_uri)
            if sanitized_source:
                self.graph.add(
                    (person_uri, self.SCHEMA.sameAs, URIRef(sanitized_source))
                )

        return person_uri

    def _generate_claim_review_rdf(
        self, claim_review: CanonicalClaimReview, generated_uris: set[str]
    ) -> None:
        """Generate RDF for a single claim review."""

        # Organization metadata lives exclusively in the curated organization graph.
        org_uri = self._get_organization_uri(claim_review.organization)

        # Generate people RDF
        person_uris = self._generate_people_rdf(claim_review.authors, generated_uris)

        # Generate claim RDF
        claim_uri = self._generate_claim_rdf(claim_review.claim, generated_uris)

        # Generate rating RDF
        rating_uri = None
        if claim_review.rating:
            rating_uri = self._generate_rating_rdf(
                claim_review.rating, generated_uris, org_uri
            )

        # Generate claim review RDF
        review_uri = URIRef(self.get_full_uri(claim_review.uri))

        # ClaimReview type and basic properties
        self.graph.add((review_uri, RDF.type, self.SCHEMA.ClaimReview))

        # Link to claim
        self.graph.add((review_uri, self.SCHEMA.itemReviewed, claim_uri))

        # Link to organization
        self.graph.add((review_uri, self.SCHEMA.author, org_uri))

        for person_uri in person_uris:
            self.graph.add((review_uri, self.SCHEMA.author, person_uri))

        # Review URL
        if claim_review.review_url:
            sanitized_url = sanitize_url(claim_review.review_url)
            if sanitized_url:
                self.graph.add((review_uri, self.SCHEMA.url, URIRef(sanitized_url)))

        # Date published
        if claim_review.date_published:
            try:
                date_literal = Literal(claim_review.date_published, datatype=XSD.date)
                self.graph.add((review_uri, self.SCHEMA.datePublished, date_literal))
            except Exception:
                # Fallback to string literal
                self.graph.add(
                    (
                        review_uri,
                        self.SCHEMA.datePublished,
                        Literal(claim_review.date_published),
                    )
                )

        # Language
        if claim_review.language:
            self.graph.add(
                (review_uri, self.SCHEMA.inLanguage, Literal(claim_review.language))
            )

        # Rating
        if rating_uri:
            self.graph.add((review_uri, self.SCHEMA.reviewRating, rating_uri))

            if (
                claim_review.rating
                and claim_review.rating.label
                and self._is_valid_normalized_rating(claim_review.rating.label)
            ):
                normalized_rating_uri = URIRef(
                    self.get_full_uri(f"rating/{claim_review.rating.label}")
                )
                self.graph.add(
                    (
                        review_uri,
                        self.CIMPLE.normalizedReviewRating,
                        normalized_rating_uri,
                    )
                )

        # Review body text
        if claim_review.review_text:
            self.graph.add(
                (review_uri, self.SCHEMA.text, Literal(claim_review.review_text))
            )

        # Description
        if claim_review.description:
            self.graph.add(
                (review_uri, self.SCHEMA.description, Literal(claim_review.description))
            )

        # Abstract
        if claim_review.abstract:
            self.graph.add(
                (review_uri, self.SCHEMA.abstract, Literal(claim_review.abstract))
            )

        # Review URL text if available
        if claim_review.review_url_text:
            self.graph.add(
                (
                    review_uri,
                    self.SCHEMA.text,
                    Literal(claim_review.review_url_text),
                )
            )

        # Keywords
        for keyword in claim_review.keywords:
            if not keyword:
                continue
            self.graph.add((review_uri, self.SCHEMA.keywords, Literal(keyword)))

        # License
        if claim_review.license_url:
            sanitized_license = sanitize_url(claim_review.license_url)
            if sanitized_license:
                self.graph.add(
                    (review_uri, self.SCHEMA.license, URIRef(sanitized_license))
                )
            else:
                self.graph.add(
                    (
                        review_uri,
                        self.SCHEMA.license,
                        Literal(claim_review.license_url),
                    )
                )

    def _get_organization_uri(self, organization: CanonicalOrganization) -> URIRef:
        """Return the catalog-assigned organization URI."""

        return URIRef(self.get_full_uri(organization.uri))

    def _generate_claim_rdf(
        self, claim: CanonicalClaim, generated_uris: set[str]
    ) -> URIRef:
        """Generate RDF for a claim."""
        claim_uri = URIRef(self.get_full_uri(claim.uri))

        if str(claim_uri) not in generated_uris:
            generated_uris.add(str(claim_uri))

            # Invariant claim properties only need to be emitted once.
            self.graph.add((claim_uri, RDF.type, self.SCHEMA.Claim))
            self.graph.add(
                (claim_uri, self.SCHEMA.text, Literal(claim.normalized_text))
            )

        # Headline
        if claim.headline:
            self.graph.add((claim_uri, self.SCHEMA.headline, Literal(claim.headline)))

        # Appearances
        for appearance_url in claim.appearances:
            sanitized_url = sanitize_url(appearance_url)
            if sanitized_url:
                self.graph.add(
                    (claim_uri, self.SCHEMA.appearance, URIRef(sanitized_url))
                )

        # Keywords
        for keyword in claim.keywords:
            if not keyword:
                continue
            self.graph.add((claim_uri, self.SCHEMA.keywords, Literal(keyword)))

        # Enrichment data
        if claim.emotion and claim.emotion != "None":
            emotion_uri = URIRef(
                f"{self.base_uri}/emotion/{self._encode_uri_segment(claim.emotion)}"
            )
            self.graph.add((claim_uri, self.CIMPLE.hasEmotion, emotion_uri))

        if claim.sentiment:
            sentiment_uri = URIRef(
                f"{self.base_uri}/sentiment/{self._encode_uri_segment(claim.sentiment)}"
            )
            self.graph.add((claim_uri, self.CIMPLE.hasSentiment, sentiment_uri))

        if claim.political_leaning:
            political_uri = URIRef(
                f"{self.base_uri}/political-leaning/"
                f"{self._encode_uri_segment(claim.political_leaning)}"
            )
            self.graph.add((claim_uri, self.CIMPLE.hasPoliticalLeaning, political_uri))

        if claim.climate_related is not None:
            self.graph.add(
                (
                    claim_uri,
                    self.CLIMATESENSE.isClimateRelated,
                    Literal(claim.climate_related, datatype=XSD.boolean),
                )
            )

        # Tropes
        for trope in claim.tropes:
            if not trope:
                continue
            slug = self._encode_uri_segment(trope)
            trope_uri = URIRef(f"{self.base_uri}/trope/{slug}")
            self.graph.add((claim_uri, self.CIMPLE.hasTrope, trope_uri))

        # Persuasion techniques
        for technique in claim.persuasion_techniques:
            if not technique:
                continue
            slug = self._encode_uri_segment(technique)
            technique_uri = URIRef(f"{self.base_uri}/persuasion-technique/{slug}")
            self.graph.add(
                (claim_uri, self.CIMPLE.hasPersuasionTechnique, technique_uri)
            )

        # Conspiracies
        for conspiracy in claim.conspiracies["mentioned"]:
            conspiracy_uri = URIRef(
                f"{self.base_uri}/conspiracy/{self._encode_uri_segment(conspiracy)}"
            )
            self.graph.add((claim_uri, self.CIMPLE.mentionsConspiracy, conspiracy_uri))
        for conspiracy in claim.conspiracies["promoted"]:
            conspiracy_uri = URIRef(
                f"{self.base_uri}/conspiracy/{self._encode_uri_segment(conspiracy)}"
            )
            self.graph.add((claim_uri, self.CIMPLE.promotesConspiracy, conspiracy_uri))

        # Readability score
        if claim.readability_score is not None:
            self.graph.add(
                (
                    claim_uri,
                    self.CIMPLE.readability_score,
                    Literal(claim.readability_score),
                )
            )

        return claim_uri

    def _generate_rating_rdf(
        self,
        rating: CanonicalRating,
        generated_uris: set[str],
        organization_uri: URIRef | None,
    ) -> URIRef:
        """Generate RDF for a rating."""
        rating_uri = URIRef(self.get_full_uri(rating.uri))

        # The rating identity is shared, but its organization context is not.
        if organization_uri:
            self.graph.add((rating_uri, self.SCHEMA.author, organization_uri))

        if str(rating_uri) in generated_uris:
            return rating_uri

        generated_uris.add(str(rating_uri))

        # Rating type
        self.graph.add((rating_uri, RDF.type, self.SCHEMA.Rating))

        # Rating properties
        label_for_name = rating.original_label or rating.label
        if label_for_name:
            self.graph.add((rating_uri, self.SCHEMA.name, Literal(label_for_name)))

        if rating.rating_value is not None:
            self.graph.add(
                (
                    rating_uri,
                    self.SCHEMA.ratingValue,
                    Literal(rating.rating_value, datatype=XSD.float),
                )
            )
        elif rating.label:
            self.graph.add((rating_uri, self.SCHEMA.ratingValue, Literal(rating.label)))

        if rating.best_rating is not None:
            self.graph.add(
                (
                    rating_uri,
                    self.SCHEMA.bestRating,
                    Literal(rating.best_rating, datatype=XSD.float),
                )
            )

        if rating.worst_rating is not None:
            self.graph.add(
                (
                    rating_uri,
                    self.SCHEMA.worstRating,
                    Literal(rating.worst_rating, datatype=XSD.float),
                )
            )

        if rating.explanation:
            self.graph.add(
                (rating_uri, self.SCHEMA.ratingExplanation, Literal(rating.explanation))
            )

        return rating_uri

    def _add_entity_properties(
        self,
        entity_uri: URIRef,
        properties: dict[str, list[dict[str, Any]]] | None,
    ) -> None:
        """Add additional DBpedia properties for a referenced entity."""
        if not properties:
            return

        for property_uri, values in properties.items():
            if not property_uri:
                continue

            try:
                predicate = URIRef(property_uri)
            except Exception:
                self.logger.warning(
                    "Invalid DBpedia property URI for entity %s: %s",
                    entity_uri,
                    property_uri,
                )
                continue

            for value in values or []:
                node = self._convert_property_value(value)
                if node is None:
                    continue
                self.graph.add((entity_uri, predicate, node))

    def _convert_property_value(self, value: dict[str, Any]) -> URIRef | Literal | None:
        """Convert a property value dictionary into an rdflib node."""
        raw_value = value.get("value")
        if raw_value is None:
            return None

        value_type = value.get("type", "literal")

        try:
            if value_type == "uri":
                return URIRef(raw_value)

            datatype = value.get("datatype")
            lang = value.get("lang")

            if datatype:
                return Literal(raw_value, datatype=URIRef(datatype))

            if lang:
                return Literal(raw_value, lang=lang)

            return Literal(raw_value)
        except Exception as exc:  # pragma: no cover - rdflib handles most conversions
            self.logger.warning(
                "Failed to convert DBpedia property value %s: %s", value, exc
            )
            return None

    def _is_valid_normalized_rating(self, label: str) -> bool:
        return label in VALID_NORMALIZED_RATINGS

    def _encode_uri_segment(self, value: str) -> str:
        """Convert a model label into one safe URI path segment."""
        normalized = value.strip().lower().replace(" ", "_")
        return quote(normalized, safe="-._~")

    def get_full_uri(self, relative_uri: str) -> str:
        """
        Get full URI from relative URI.

        Args:
            relative_uri: Relative URI

        Returns:
            str: Full URI
        """
        if relative_uri.startswith("http"):
            return relative_uri
        return f"{self.base_uri}/{relative_uri}"
