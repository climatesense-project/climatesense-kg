"""RDF contract tests for resolved claim reviews."""

from datetime import datetime
import gzip
from pathlib import Path
from unittest.mock import Mock
from uuid import UUID

import pytest
from rdflib import Graph, URIRef
from rdflib.namespace import RDF

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
    CanonicalReviewDocument,
    EntityMention,
    EntityPropertyValue,
)
from climatesense_kg.export import RdfExporter
from climatesense_kg.rdf_generation.generator import RDFGenerator

SCHEMA = "http://schema.org/"
BASE = "http://data.climatesense-project.eu"


def _review(organization: CanonicalOrganization) -> CanonicalClaimReview:
    return CanonicalClaimReview(
        id=UUID("550e8400-e29b-41d4-a716-446655440000"),
        claim=CanonicalClaim(text="A reviewed claim"),
        organization=organization,
        document=CanonicalReviewDocument(
            id=UUID("550e8400-e29b-41d4-a716-446655440001"),
            urls={
                "https://factual.ro/old-path",
                "https://factual.ro/current-path",
            },
            preferred_url="https://factual.ro/current-path",
            content="The extracted review text",
        ),
        source_record_keys={"record"},
        source_names={"claimreviewdata"},
        rating=CanonicalRating(label="not_credible", original_label="Fals"),
    )


def test_resolved_review_projects_one_uuid_iri_with_all_url_aliases() -> None:
    organization = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    review = _review(organization)

    graph = Graph().parse(
        data=RDFGenerator(BASE).generate([review], "turtle"), format="turtle"
    )
    review_uri = URIRef(f"{BASE}/{review.uri}")

    assert (review_uri, RDF.type, URIRef(f"{SCHEMA}ClaimReview")) in graph
    assert set(graph.objects(review_uri, URIRef(f"{SCHEMA}url"))) == {
        URIRef("https://factual.ro/old-path"),
        URIRef("https://factual.ro/current-path"),
    }


def test_source_rating_iri_is_scoped_to_organization() -> None:
    first_organization = CanonicalOrganization(
        uri=f"{BASE}/organization/first",
        name="First",
        website="https://first.example",
    )
    second_organization = CanonicalOrganization(
        uri=f"{BASE}/organization/second",
        name="Second",
        website="https://second.example",
    )
    first = _review(first_organization)
    second = _review(second_organization)
    second.id = UUID("550e8400-e29b-41d4-a716-446655440099")

    graph = Graph().parse(
        data=RDFGenerator(BASE).generate([first, second], "turtle"), format="turtle"
    )
    rating_nodes = set(graph.subjects(RDF.type, URIRef(f"{SCHEMA}Rating")))

    assert len(rating_nodes) == 2
    assert all(
        len(set(graph.objects(node, URIRef(f"{SCHEMA}author")))) == 1
        for node in rating_nodes
    )


def test_streamed_ntriples_is_graph_equivalent_to_buffered_generation(
    tmp_path: Path,
) -> None:
    organization = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    first = _review(organization)
    second = _review(organization)
    second.id = UUID("550e8400-e29b-41d4-a716-446655440099")
    second.document.id = UUID("550e8400-e29b-41d4-a716-446655440098")
    second.document.urls = {"https://factual.ro/another"}
    second.document.preferred_url = "https://factual.ro/another"

    expected = Graph().parse(
        data=RDFGenerator(BASE).generate([first, second], "nt"),
        format="nt",
    )
    reader = Mock()
    reader.count.return_value = 2
    reader.iter_batches.return_value = iter([[first], [second]])
    enrichment = Mock()
    exporter = RdfExporter(
        reader,
        enrichment,
        RDFGenerator(BASE),
        output_path_template=str(tmp_path / "{SOURCE}.nt.gz"),
        enrichment_graphs={},
        batch_size=1,
    )
    report = exporter.run(
        ("claimreviewdata",),
        datetime(2026, 8, 13),
    )

    serialized = gzip.decompress(report.artifacts[0].path.read_bytes()).decode("utf-8")
    actual = Graph().parse(data=serialized, format="nt")
    output_lines = serialized.splitlines()
    assert set(actual) == set(expected)
    assert output_lines == sorted(set(output_lines))
    assert report.reviews == 2
    assert report.successful_reviews == 2
    assert not list(tmp_path.glob("*.tmp"))


def test_entity_properties_can_be_projected_once_without_losing_mentions() -> None:
    organization = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    review = _review(organization)
    entity_uri = "http://dbpedia.org/resource/Colombia"
    latitude = "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
    review.claim.analysis.entities.append(
        EntityMention(
            uri=entity_uri,
            source="dbpedia_spotlight",
            properties={
                latitude: [
                    EntityPropertyValue(
                        value="4.583333492279053",
                        value_type="literal",
                        datatype="http://www.w3.org/2001/XMLSchema#float",
                    )
                ]
            },
        )
    )
    generator = RDFGenerator(BASE)
    sources = frozenset({"dbpedia_spotlight"})
    without_properties = Graph().parse(
        data=generator.project_entity_enrichment_nt(
            review,
            sources,
            property_entity_uris=frozenset(),
        ),
        format="nt",
    )
    with_properties = Graph().parse(
        data=generator.project_entity_enrichment_nt(
            review,
            sources,
            property_entity_uris=frozenset({entity_uri}),
        ),
        format="nt",
    )
    claim_uri = URIRef(f"{BASE}/{review.claim.uri}")
    entity = URIRef(entity_uri)
    mention = (claim_uri, URIRef(f"{SCHEMA}mentions"), entity)

    assert mention in without_properties
    assert mention in with_properties
    assert not list(without_properties.objects(entity, URIRef(latitude)))
    assert list(with_properties.objects(entity, URIRef(latitude)))


def test_streaming_projection_errors_are_summarized_per_graph(tmp_path: Path) -> None:
    organization = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    generator = RDFGenerator(BASE)
    generator.project_claim_review_nt = Mock(  # type: ignore[method-assign]
        side_effect=ValueError("invalid review")
    )
    reviews = [_review(organization) for _index in range(100)]
    reader = Mock()
    reader.count.return_value = 100
    reader.iter_batches.return_value = iter([reviews])
    exporter = RdfExporter(
        reader,
        Mock(),
        generator,
        output_path_template=str(tmp_path / "{SOURCE}.nt.gz"),
        enrichment_graphs={},
    )
    report = exporter.run(("claimreviewdata",), datetime(2026, 8, 13))

    assert report.failed_reviews == 100
    assert report.errors == (
        "claimreviewdata: 100 reviews failed projection; first error: invalid review",
    )
    assert not (tmp_path / "claimreviewdata.nt.gz").exists()


def test_incomplete_export_preserves_previous_complete_snapshot(tmp_path: Path) -> None:
    output = tmp_path / "claimreviewdata.nt.gz"
    output.write_bytes(gzip.compress(b"previous complete graph\n"))
    organization = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    reader = Mock()
    reader.count.return_value = 1
    reader.iter_batches.return_value = iter([[_review(organization)]])
    exporter = RdfExporter(
        reader,
        Mock(),
        RDFGenerator(BASE),
        output_path_template=str(tmp_path / "{SOURCE}.nt.gz"),
        enrichment_graphs={},
    )

    report = exporter.run(
        ("claimreviewdata",),
        datetime(2026, 8, 13),
        incomplete_stages_by_graph={"claimreviewdata": {"cimple.emotion"}},
    )

    assert not report.artifacts[0].complete
    assert gzip.decompress(output.read_bytes()) == b"previous complete graph\n"
    assert not list(tmp_path.glob("*.tmp"))


def test_deduplication_failure_preserves_previous_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "claimreviewdata.nt.gz"
    output.write_bytes(gzip.compress(b"previous complete graph\n"))
    organization = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    reader = Mock()
    reader.count.return_value = 1
    reader.iter_batches.return_value = iter([[_review(organization)]])
    exporter = RdfExporter(
        reader,
        Mock(),
        RDFGenerator(BASE),
        output_path_template=str(tmp_path / "{SOURCE}.nt.gz"),
        enrichment_graphs={},
    )
    monkeypatch.setattr("climatesense_kg.export.shutil.which", lambda _name: None)

    with pytest.raises(RuntimeError, match="requires the system sort command"):
        exporter.run(("claimreviewdata",), datetime(2026, 8, 13))

    assert gzip.decompress(output.read_bytes()) == b"previous complete graph\n"
    assert not list(tmp_path.glob("*.tmp"))
