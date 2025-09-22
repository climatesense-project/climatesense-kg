"""Pydantic schemas for pipeline analytics endpoints."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class EnricherSuccessRate(BaseModel):
    step: str
    total_entries: int = Field(ge=0)
    successful: int = Field(ge=0)
    failed: int = Field(ge=0)
    success_rate_percent: float = Field(ge=0, le=100)


class EnricherErrorBreakdown(BaseModel):
    step: str
    error_type: str | None
    error_count: int = Field(ge=0)


class EnricherDomainFailure(BaseModel):
    step: str
    domain: str
    failure_count: int = Field(ge=0)


class EnricherRecentActivity(BaseModel):
    step: str
    recent_entries: int = Field(ge=0)
    earliest: datetime | None
    latest: datetime | None
    successful: int = Field(ge=0)
    failed: int = Field(ge=0)
