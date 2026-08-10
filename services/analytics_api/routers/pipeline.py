"""Pipeline stage metrics endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TypedDict

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from ..schemas.pipeline import (
    StageDomainFailure,
    StageErrorBreakdown,
    StageRecentActivity,
    StageSuccessRate,
)
from ..services.sql import run_query

router = APIRouter(prefix="/metrics/stages", tags=["pipeline"])


class BaseStageParams(TypedDict, total=False):
    """Shared parameters for semantic-stage queries."""

    stage_name: str | None
    from_ts: datetime | None
    to_ts: datetime | None


class StageParamsWithLimit(BaseStageParams, total=False):
    """Semantic-stage parameters with an optional result limit."""

    limit: int


def _default_from_ts(hours: int = 24) -> datetime:
    return datetime.now(UTC) - timedelta(hours=hours)


@router.get("/success-rate", response_model=list[StageSuccessRate])
async def success_rate(
    stage_name: str | None = Query(
        default=None, description="Filter by semantic stage name"
    ),
    from_ts: datetime | None = Query(
        default=None, description="ISO timestamp lower bound (inclusive)"
    ),
    to_ts: datetime | None = Query(
        default=None, description="ISO timestamp upper bound (inclusive)"
    ),
    session: AsyncSession = Depends(get_session),
) -> list[StageSuccessRate]:
    params: BaseStageParams = {
        "stage_name": stage_name,
        "from_ts": from_ts,
        "to_ts": to_ts,
    }
    rows = await run_query(session, "pipeline", "stages_success_rate.sql", dict(params))
    return [
        StageSuccessRate(
            stage_name=row["stage_name"],
            stage_version=row["stage_version"],
            total_results=row["total_results"],
            successful=row["successful"],
            failed=row["failed"],
            success_rate_percent=float(row["success_rate_percent"] or 0.0),
        )
        for row in rows
    ]


@router.get("/error-types", response_model=list[StageErrorBreakdown])
async def error_types(
    stage_name: str | None = Query(default=None),
    from_ts: datetime | None = Query(default=None),
    to_ts: datetime | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
) -> list[StageErrorBreakdown]:
    params: StageParamsWithLimit = {
        "stage_name": stage_name,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "limit": limit,
    }
    rows = await run_query(session, "pipeline", "stages_error_types.sql", dict(params))
    return [
        StageErrorBreakdown(
            stage_name=row["stage_name"],
            stage_version=row["stage_version"],
            error_type=row.get("error_type"),
            error_count=row["error_count"],
        )
        for row in rows
    ]


@router.get("/domain-failures", response_model=list[StageDomainFailure])
async def domain_failures(
    stage_name: str | None = Query(default="document.extract"),
    from_ts: datetime | None = Query(default=None),
    to_ts: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> list[StageDomainFailure]:
    params: StageParamsWithLimit = {
        "stage_name": stage_name,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "limit": limit,
    }
    rows = await run_query(
        session, "pipeline", "stages_domain_failures.sql", dict(params)
    )
    return [
        StageDomainFailure(
            stage_name=row["stage_name"],
            stage_version=row["stage_version"],
            domain=row.get("domain", "unknown"),
            failure_count=row["failure_count"],
        )
        for row in rows
    ]


@router.get("/recent-activity", response_model=list[StageRecentActivity])
async def recent_activity(
    stage_name: str | None = Query(default=None),
    from_ts: datetime | None = Query(
        default=None,
        description="Lower bound on updated_at; defaults to last 24 hours if omitted",
    ),
    to_ts: datetime | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[StageRecentActivity]:
    effective_from = from_ts or _default_from_ts()
    params: StageParamsWithLimit = {
        "stage_name": stage_name,
        "from_ts": effective_from,
        "to_ts": to_ts,
        "limit": limit,
    }
    rows = await run_query(
        session, "pipeline", "stages_recent_activity.sql", dict(params)
    )
    return [
        StageRecentActivity(
            stage_name=row["stage_name"],
            stage_version=row["stage_version"],
            recent_results=row["recent_results"],
            earliest=row.get("earliest"),
            latest=row.get("latest"),
            successful=row["successful"],
            failed=row["failed"],
        )
        for row in rows
    ]
