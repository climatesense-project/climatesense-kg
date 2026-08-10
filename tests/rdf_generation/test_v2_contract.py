"""RDF contract tests for resolved v2 claim reviews."""

from uuid import UUID

from rdflib import Graph, URIRef
from rdflib.namespace import RDF

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
    CanonicalReviewDocument,
)
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
