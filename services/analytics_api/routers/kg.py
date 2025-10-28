"""Knowledge graph metrics endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from ..schemas.kg import (
    ClaimFactorDistributions,
    ClassDistribution,
    CoreCounts,
    EnrichmentCoverage,
    EntityTypeCount,
    FactorDistributionItem,
    GraphTripleCount,
)
from ..services.sparql import sparql_select

router = APIRouter(prefix="/metrics/kg", tags=["knowledge-graph"])


def _format_factor_label(value: str, category: str | None = None) -> str:
    """Convert a slug string into a human friendly label."""
    if not value:
        return "Unknown"
    if category == "climate_related":
        lowered = value.lower()
        if lowered == "true":
            return "Climate Related"
        if lowered == "false":
            return "Not Climate Related"
    return value.replace("_", " ").replace("-", " ").title()


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


@router.get("/core-counts", response_model=CoreCounts)
async def core_counts() -> CoreCounts:
    rows = sparql_select("kg", "core_counts.rq")
    row = rows[0] if rows else {}
    return CoreCounts(
        total_claim_reviews=int(row.get("totalClaimReviews", 0)),
        total_claims=int(row.get("totalClaims", 0)),
        total_ratings=int(row.get("totalRatings", 0)),
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
        claims_with_tropes=int(row.get("claimsWithTropes", 0)),
        claims_with_persuasion_techniques=int(
            row.get("claimsWithPersuasionTechniques", 0)
        ),
        claims_with_climate_relatedness=int(row.get("claimsWithClimateRelatedness", 0)),
    )


@router.get("/entity-types", response_model=list[EntityTypeCount])
async def entity_types() -> list[EntityTypeCount]:
    rows = sparql_select("kg", "entity_types.rq")
    return [
        EntityTypeCount(type_uri=row.get("entity"), count=int(row.get("count", 0)))
        for row in rows
    ]


@router.get("/claim-factors", response_model=ClaimFactorDistributions)
async def claim_factors() -> ClaimFactorDistributions:
    rows = sparql_select("kg", "claim_factors_distribution.rq")

    buckets: dict[str, list[FactorDistributionItem]] = {
        "sentiment": [],
        "political_leaning": [],
        "climate_related": [],
        "emotion": [],
        "tropes": [],
        "persuasion_techniques": [],
        "conspiracies_mentioned": [],
        "conspiracies_promoted": [],
    }

    for row in rows:
        category = row.get("category")
        if category not in buckets:
            continue

        raw_value = (row.get("value") or "").strip()
        count = int(row.get("count", 0))
        item = FactorDistributionItem(
            value=raw_value,
            label=_format_factor_label(raw_value, category),
            count=count,
        )
        buckets[category].append(item)

    for values in buckets.values():
        values.sort(key=lambda item: item.count, reverse=True)

    return ClaimFactorDistributions(**buckets)
