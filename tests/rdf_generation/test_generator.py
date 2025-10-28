"""Tests for RDF generator."""

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import XSD
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


def test_generator_adds_dbpedia_entity_properties() -> None:
    review = _build_review(None)
    entity_uri = "http://dbpedia.org/resource/Paris"
    review.claim.entities.append(
        {
            "uri": entity_uri,
            "dbpedia_properties": {
                "http://www.w3.org/2003/01/geo/wgs84_pos#lat": [
                    {
                        "value": "48.8566",
                        "type": "typed-literal",
                        "datatype": str(XSD.float),
                    }
                ],
                "http://www.w3.org/2003/01/geo/wgs84_pos#long": [
                    {
                        "value": "2.3522",
                        "type": "typed-literal",
                        "datatype": str(XSD.float),
                    }
                ],
                "http://www.opengis.net/ont/geosparql#geometry": [
                    {
                        "value": "POINT(2.3522 48.8566)",
                        "type": "literal",
                        "datatype": "http://www.opengis.net/ont/geosparql#wktLiteral",
                    }
                ],
            },
        }
    )

    graph, _ = _generate_graph(review)

    subject = URIRef(entity_uri)
    lat_predicate = URIRef("http://www.w3.org/2003/01/geo/wgs84_pos#lat")
    long_predicate = URIRef("http://www.w3.org/2003/01/geo/wgs84_pos#long")
    geometry_predicate = URIRef("http://www.opengis.net/ont/geosparql#geometry")

    expected_lat = Literal("48.8566", datatype=XSD.float)
    expected_long = Literal("2.3522", datatype=XSD.float)
    expected_geometry = Literal(
        "POINT(2.3522 48.8566)",
        datatype=URIRef("http://www.opengis.net/ont/geosparql#wktLiteral"),
    )

    assert (subject, lat_predicate, expected_lat) in graph
    assert (subject, long_predicate, expected_long) in graph
    assert (subject, geometry_predicate, expected_geometry) in graph


def test_generator_emits_climate_relatedness_boolean() -> None:
    review = _build_review(None)
    review.claim.climate_related = True

    graph, generator = _generate_graph(review)
    claim_uri = URIRef(generator.get_full_uri(review.claim.uri))

    expected_object = Literal(True, datatype=XSD.boolean)
    assert (claim_uri, CIMPLE.isClimateRelated, expected_object) in graph
