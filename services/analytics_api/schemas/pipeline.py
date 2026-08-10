"""Pydantic schemas for pipeline analytics endpoints."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class StageSuccessRate(BaseModel):
    stage_name: str
    stage_version: str
    total_results: int = Field(ge=0)
    successful: int = Field(ge=0)
    failed: int = Field(ge=0)
    success_rate_percent: float = Field(ge=0, le=100)


class StageErrorBreakdown(BaseModel):
    stage_name: str
    stage_version: str
    error_type: str | None
    error_count: int = Field(ge=0)


class StageDomainFailure(BaseModel):
    stage_name: str
    stage_version: str
    domain: str
    failure_count: int = Field(ge=0)


class StageRecentActivity(BaseModel):
    stage_name: str
    stage_version: str
    recent_results: int = Field(ge=0)
    earliest: datetime | None
    latest: datetime | None
    successful: int = Field(ge=0)
    failed: int = Field(ge=0)
