"""Pydantic schemas for knowledge-graph analytics endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field


class GraphTripleCount(BaseModel):
    graph: str | None = None
    triple_count: int = Field(ge=0)


class ClassDistribution(BaseModel):
    class_uri: str | None = None
    count: int = Field(ge=0)


class CoreCounts(BaseModel):
    total_claim_reviews: int = Field(ge=0)
    total_claims: int = Field(ge=0)
    total_ratings: int = Field(ge=0)


class EnrichmentCoverage(BaseModel):
    total_claims: int = Field(ge=0)
    claims_with_emotion: int = Field(ge=0)
    claims_with_sentiment: int = Field(ge=0)
    claims_with_political_leaning: int = Field(ge=0)
    claims_with_conspiracy: int = Field(ge=0)
    claims_with_tropes: int = Field(ge=0)
    claims_with_persuasion_techniques: int = Field(ge=0)


class EntityTypeCount(BaseModel):
    type_uri: str | None = None
    count: int = Field(ge=0)


class FactorDistributionItem(BaseModel):
    value: str
    label: str
    count: int = Field(ge=0)


class ClaimFactorDistributions(BaseModel):
    sentiment: list[FactorDistributionItem] = Field(
        default_factory=list[FactorDistributionItem]
    )
    political_leaning: list[FactorDistributionItem] = Field(
        default_factory=list[FactorDistributionItem]
    )
    emotion: list[FactorDistributionItem] = Field(
        default_factory=list[FactorDistributionItem]
    )
    tropes: list[FactorDistributionItem] = Field(
        default_factory=list[FactorDistributionItem]
    )
    persuasion_techniques: list[FactorDistributionItem] = Field(
        default_factory=list[FactorDistributionItem]
    )
    conspiracies_mentioned: list[FactorDistributionItem] = Field(
        default_factory=list[FactorDistributionItem]
    )
    conspiracies_promoted: list[FactorDistributionItem] = Field(
        default_factory=list[FactorDistributionItem]
    )
