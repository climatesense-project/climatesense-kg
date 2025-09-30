"""Tests for RDF generator."""

from rdflib import Graph, Namespace, URIRef
from src.climatesense_kg.config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
)
from src.climatesense_kg.rdf_generation.generator import RDFGenerator

CIMPLE = Namespace("http://data.cimple.eu/ontology#")


def _build_review(label: str | None) -> CanonicalClaimReview:
    claim = CanonicalClaim(text="Example claim", appearances=["https://example.org"])
    organization = CanonicalOrganization(name="Org", website="https://example.org")
    rating = CanonicalRating(label=label, original_label=label) if label else None

    return CanonicalClaimReview(
        claim=claim,
        organization=organization,
        review_url="https://example.org/review",
        rating=rating,
        source_type="test",
        source_name="unit-test",
    )


def _generate_graph(review: CanonicalClaimReview) -> tuple[Graph, RDFGenerator]:
    generator = RDFGenerator(base_uri="http://data.cimple.eu")
    rdf_content = generator.generate([review], output_format="turtle")
    graph = Graph()
    graph.parse(data=rdf_content, format="turtle")
    return graph, generator


def test_generator_adds_normalized_rating_for_allowed_labels() -> None:
    review = _build_review("credible")
    graph, generator = _generate_graph(review)
    review_uri = URIRef(generator.get_full_uri(review.uri))
    expected_uri = URIRef(generator.get_full_uri("rating/credible"))

    triples = list(graph.triples((review_uri, CIMPLE.normalizedReviewRating, None)))
    assert len(triples) == 1
    assert triples[0][2] == expected_uri


def test_generator_skips_normalized_rating_for_unknown_labels() -> None:
    review = _build_review("unsupported_label")
    graph, generator = _generate_graph(review)
    review_uri = URIRef(generator.get_full_uri(review.uri))

    triples = list(graph.triples((review_uri, CIMPLE.normalizedReviewRating, None)))
    assert triples == []
