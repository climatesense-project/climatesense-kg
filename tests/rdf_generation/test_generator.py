"""Tests for RDF generator."""

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, XSD
from src.climatesense_kg.config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
)
from src.climatesense_kg.rdf_generation.generator import RDFGenerator

CIMPLE = Namespace("http://data.cimple.eu/ontology#")
SCHEMA = Namespace("http://schema.org/")


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


def test_rdf_xml_public_format_is_serialized_as_xml() -> None:
    generator = RDFGenerator(base_uri="http://data.cimple.eu")

    rdf_content = generator.generate([_build_review(None)], output_format="rdf/xml")

    graph = Graph()
    graph.parse(data=rdf_content, format="xml")
    assert len(graph) > 0


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


def test_generator_emits_parent_organization() -> None:
    parent = CanonicalOrganization(name="AFP", website="https://www.afp.com")
    child = CanonicalOrganization(
        name="AFP Factuel", website="https://factuel.afp.com", parent=parent
    )
    review = _build_review(None)
    review.organization = child

    graph, generator = _generate_graph(review)

    child_uri = URIRef(generator.get_full_uri(child.uri))
    parent_uri = URIRef(generator.get_full_uri(parent.uri))

    # Child links to parent
    assert (child_uri, SCHEMA.parentOrganization, parent_uri) in graph

    # Both are typed as Organization
    assert (child_uri, RDF.type, SCHEMA.Organization) in graph
    assert (parent_uri, RDF.type, SCHEMA.Organization) in graph

    # Parent has its own name and url
    assert (parent_uri, SCHEMA.name, Literal("AFP")) in graph
    assert (parent_uri, SCHEMA.url, URIRef("https://www.afp.com")) in graph


def test_generator_replaces_unpaired_surrogate_in_claim_text() -> None:
    review = _build_review(None)
    review.claim.text = (
        "Valid mathematical character: \U0001d48d; truncated character: \ud835"
    )

    graph, generator = _generate_graph(review)

    claim_uri = URIRef(generator.get_full_uri(review.claim.uri))
    expected_text = Literal(
        "Valid mathematical character: \U0001d48d; truncated character: \ufffd"
    )
    assert (claim_uri, SCHEMA.text, expected_text) in graph


def test_generator_merges_metadata_for_shared_claims() -> None:
    first = _build_review(None)
    first.claim.headline = "First headline"
    first.claim.keywords = ["first"]
    second = _build_review(None)
    second.review_url = "https://example.org/another-review"
    second.claim.headline = "Second headline"
    second.claim.keywords = ["second"]

    generator = RDFGenerator(base_uri="http://data.cimple.eu")
    rdf_content = generator.generate([first, second], output_format="turtle")
    graph = Graph().parse(data=rdf_content, format="turtle")
    claim_uri = URIRef(generator.get_full_uri(first.claim.uri))

    assert (claim_uri, SCHEMA.headline, Literal("First headline")) in graph
    assert (claim_uri, SCHEMA.headline, Literal("Second headline")) in graph
    assert (claim_uri, SCHEMA.keywords, Literal("first")) in graph
    assert (claim_uri, SCHEMA.keywords, Literal("second")) in graph
