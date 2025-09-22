"""Pydantic schemas for knowledge-graph analytics endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field


class GraphTripleCount(BaseModel):
    graph: str | None = None
    triple_count: int = Field(ge=0)


class ClassDistribution(BaseModel):
    class_uri: str | None = None
    count: int = Field(ge=0)


class ProvenanceCompleteness(BaseModel):
    total_reviews: int = Field(ge=0)
    reviews_with_author: int = Field(ge=0)
    reviews_with_rating: int = Field(ge=0)
    reviews_with_normalized_rating: int = Field(ge=0)


class EnrichmentCoverage(BaseModel):
    total_claims: int = Field(ge=0)
    claims_with_emotion: int = Field(ge=0)
    claims_with_sentiment: int = Field(ge=0)
    claims_with_political_leaning: int = Field(ge=0)
    claims_with_conspiracy: int = Field(ge=0)


class EntityTypeCount(BaseModel):
    type_uri: str | None = None
    count: int = Field(ge=0)
