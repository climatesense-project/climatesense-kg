"""Tests for RDF generator."""

import os
from pathlib import Path

import pytest
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import XSD
from src.climatesense_kg.config.graphs import DBPEDIA_ENTITY_SOURCES
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
    organization = CanonicalOrganization(
        name="Org",
        website="https://example.org",
        canonical_uri="http://data.climatesense-project.eu/organization/example",
    )
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


def test_save_atomically_replaces_existing_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path = tmp_path / "graph.ttl"
    output_path.write_text("previous RDF", encoding="utf-8")
    real_replace = os.replace
    replace_calls = 0

    def inspect_replace(source: str | Path, destination: str | Path) -> None:
        nonlocal replace_calls
        replace_calls += 1
        assert Path(source).parent == output_path.parent
        assert Path(destination) == output_path
        assert output_path.read_text(encoding="utf-8") == "previous RDF"
        real_replace(source, destination)

    monkeypatch.setattr(
        "src.climatesense_kg.rdf_generation.generator.os.replace", inspect_replace
    )
    generator = RDFGenerator(base_uri="http://data.cimple.eu")

    successful_uris = generator.save([_build_review(None)], output_path, "turtle")

    assert replace_calls == 1
    assert successful_uris
    assert len(Graph().parse(output_path, format="turtle")) > 0


def test_save_preserves_existing_output_when_replace_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path = tmp_path / "graph.ttl"
    output_path.write_text("previous RDF", encoding="utf-8")

    def fail_replace(source: str | Path, destination: str | Path) -> None:
        raise OSError("simulated atomic replacement failure")

    monkeypatch.setattr(
        "src.climatesense_kg.rdf_generation.generator.os.replace", fail_replace
    )
    generator = RDFGenerator(base_uri="http://data.cimple.eu")

    with pytest.raises(OSError, match="simulated atomic replacement failure"):
        generator.save([_build_review(None)], output_path, "turtle")

    assert output_path.read_text(encoding="utf-8") == "previous RDF"
    assert list(tmp_path.glob(".graph.ttl.*.tmp")) == []


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


def test_generator_moves_dbpedia_entity_data_out_of_source_graph() -> None:
    review = _build_review(None)
    entity_uri = "http://dbpedia.org/resource/Paris"
    review.claim.entities.append(
        {
            "uri": entity_uri,
            "source": "dbpedia_spotlight",
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

    source_graph, _ = _generate_graph(review)

    assert list(source_graph.triples((None, SCHEMA.mentions, None))) == []
    assert list(source_graph.triples((URIRef(entity_uri), None, None))) == []


def test_generator_adds_dbpedia_mentions_and_properties_to_enrichment_graph(
    tmp_path: Path,
) -> None:
    review = _build_review(None)
    entity_uri = "http://dbpedia.org/resource/Paris"
    review.claim.entities.append(
        {
            "uri": entity_uri,
            "source": "dbpedia_spotlight",
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
    output_path = tmp_path / "dbpedia-enricher.ttl"
    generator = RDFGenerator(base_uri="http://data.cimple.eu")

    review_uris = generator.save_entity_enrichment(
        [review],
        output_path,
        "turtle",
        entity_sources=DBPEDIA_ENTITY_SOURCES,
        property_keys=("dbpedia_properties",),
    )
    graph = Graph().parse(output_path, format="turtle")

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

    claim_uri = URIRef(generator.get_full_uri(review.claim.uri))
    assert review_uris == [review.uri]
    assert (claim_uri, SCHEMA.mentions, subject) in graph
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


def test_generator_emits_zero_readability_score() -> None:
    review = _build_review(None)
    review.claim.readability_score = 0.0

    graph, generator = _generate_graph(review)
    claim_uri = URIRef(generator.get_full_uri(review.claim.uri))

    assert (claim_uri, CIMPLE.readability_score, Literal(0.0)) in graph


def test_generator_only_references_catalog_organization() -> None:
    organization_uri = URIRef(
        "http://data.climatesense-project.eu/organization/afp-factuel"
    )
    organization = CanonicalOrganization(
        name="AFP Factuel",
        website="https://factuel.afp.com",
        canonical_uri=str(organization_uri),
    )
    review = _build_review(None)
    review.organization = organization

    graph, generator = _generate_graph(review)
    review_uri = URIRef(generator.get_full_uri(review.uri))

    assert (review_uri, SCHEMA.author, organization_uri) in graph
    assert list(graph.triples((organization_uri, None, None))) == []


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


def test_generator_preserves_organizations_for_shared_ratings() -> None:
    first = _build_review("credible")
    second = _build_review("credible")
    second.review_url = "https://example.org/another-review"
    second.organization = CanonicalOrganization(
        name="Another Org",
        website="https://another.example.org",
        canonical_uri=(
            "http://data.climatesense-project.eu/organization/another-example"
        ),
    )

    generator = RDFGenerator(base_uri="http://data.cimple.eu")
    rdf_content = generator.generate([first, second], output_format="turtle")
    graph = Graph().parse(data=rdf_content, format="turtle")
    rating_uri = URIRef(generator.get_full_uri(first.rating.uri))  # type: ignore[union-attr]
    first_org_uri = URIRef(generator.get_full_uri(first.organization.uri))  # type: ignore[union-attr]
    second_org_uri = URIRef(generator.get_full_uri(second.organization.uri))

    assert (rating_uri, SCHEMA.author, first_org_uri) in graph
    assert (rating_uri, SCHEMA.author, second_org_uri) in graph


def test_generator_percent_encodes_model_labels_in_uris() -> None:
    review = _build_review(None)
    review.claim.emotion = "Fear / Anxiety?#"
    review.claim.tropes = ["It's a hoax / scam"]

    graph, generator = _generate_graph(review)
    claim_uri = URIRef(generator.get_full_uri(review.claim.uri))

    assert (
        claim_uri,
        CIMPLE.hasEmotion,
        URIRef("http://data.cimple.eu/emotion/fear_%2F_anxiety%3F%23"),
    ) in graph
    assert (
        claim_uri,
        CIMPLE.hasTrope,
        URIRef("http://data.cimple.eu/trope/it%27s_a_hoax_%2F_scam"),
    ) in graph
