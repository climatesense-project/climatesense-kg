"""Knowledge graph metrics endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from ..schemas.kg import (
    ClassDistribution,
    EnrichmentCoverage,
    EntityTypeCount,
    GraphTripleCount,
    ProvenanceCompleteness,
)
from ..services.sparql import sparql_select

router = APIRouter(prefix="/metrics/kg", tags=["knowledge-graph"])


@router.get("/triple-volume", response_model=list[GraphTripleCount])
async def triple_volume() -> list[GraphTripleCount]:
    rows = sparql_select("kg", "triple_volume.rq")
    return [
        GraphTripleCount(
            graph=row.get("graph"), triple_count=int(row.get("tripleCount", 0))
        )
        for row in rows
        if row.get("graph", "").startswith("http://data.climatesense-project.eu/graph/")
    ]


@router.get("/class-distribution", response_model=list[ClassDistribution])
async def class_distribution() -> list[ClassDistribution]:
    rows = sparql_select("kg", "class_distribution.rq")
    return [
        ClassDistribution(class_uri=row.get("class"), count=int(row.get("count", 0)))
        for row in rows
    ]


@router.get("/provenance", response_model=ProvenanceCompleteness)
async def provenance() -> ProvenanceCompleteness:
    rows = sparql_select("kg", "provenance_completeness.rq")
    row = rows[0] if rows else {}
    return ProvenanceCompleteness(
        total_reviews=int(row.get("totalReviews", 0)),
        reviews_with_author=int(row.get("reviewsWithAuthor", 0)),
        reviews_with_rating=int(row.get("reviewsWithRating", 0)),
        reviews_with_normalized_rating=int(row.get("reviewsWithNormalizedRating", 0)),
    )


@router.get("/enrichment-coverage", response_model=EnrichmentCoverage)
async def enrichment_coverage() -> EnrichmentCoverage:
    rows = sparql_select("kg", "enrichment_coverage.rq")
    row = rows[0] if rows else {}
    return EnrichmentCoverage(
        total_claims=int(row.get("totalClaims", 0)),
        claims_with_emotion=int(row.get("claimsWithEmotion", 0)),
        claims_with_sentiment=int(row.get("claimsWithSentiment", 0)),
        claims_with_political_leaning=int(row.get("claimsWithPoliticalLeaning", 0)),
        claims_with_conspiracy=int(row.get("claimsWithConspiracy", 0)),
    )


@router.get("/entity-types", response_model=list[EntityTypeCount])
async def entity_types() -> list[EntityTypeCount]:
    rows = sparql_select("kg", "entity_types.rq")
    return [
        EntityTypeCount(type_uri=row.get("entity"), count=int(row.get("count", 0)))
        for row in rows
    ]
