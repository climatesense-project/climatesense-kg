"""Pipeline metrics endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TypedDict

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from ..schemas.pipeline import (
    EnricherDomainFailure,
    EnricherErrorBreakdown,
    EnricherRecentActivity,
    EnricherSuccessRate,
)
from ..services.sql import run_query

router = APIRouter(prefix="/metrics/enrichers", tags=["pipeline"])


class BaseEnricherParams(TypedDict, total=False):
    """Base parameters for enricher queries."""

    step: str | None
    from_ts: datetime | None
    to_ts: datetime | None


class EnricherParamsWithLimit(BaseEnricherParams, total=False):
    """Enricher parameters with optional limit."""

    limit: int


def _default_from_ts(hours: int = 24) -> datetime:
    return datetime.now(UTC) - timedelta(hours=hours)


@router.get("/success-rate", response_model=list[EnricherSuccessRate])
async def success_rate(
    step: str | None = Query(default=None, description="Filter by enricher step name"),
    from_ts: datetime | None = Query(
        default=None, description="ISO timestamp lower bound (inclusive)"
    ),
    to_ts: datetime | None = Query(
        default=None, description="ISO timestamp upper bound (inclusive)"
    ),
    session: AsyncSession = Depends(get_session),
) -> list[EnricherSuccessRate]:
    params: BaseEnricherParams = {
        "step": step,
        "from_ts": from_ts,
        "to_ts": to_ts,
    }
    rows = await run_query(
        session, "pipeline", "enrichers_success_rate.sql", dict(params)
    )
    return [
        EnricherSuccessRate(
            step=row["step"],
            total_entries=row["total_entries"],
            successful=row["successful"],
            failed=row["failed"],
            success_rate_percent=float(row["success_rate_percent"] or 0.0),
        )
        for row in rows
    ]


@router.get("/error-types", response_model=list[EnricherErrorBreakdown])
async def error_types(
    step: str | None = Query(default=None),
    from_ts: datetime | None = Query(default=None),
    to_ts: datetime | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
) -> list[EnricherErrorBreakdown]:
    params: EnricherParamsWithLimit = {
        "step": step,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "limit": limit,
    }
    rows = await run_query(
        session, "pipeline", "enrichers_error_types.sql", dict(params)
    )
    return [
        EnricherErrorBreakdown(
            step=row["step"],
            error_type=row.get("error_type"),
            error_count=row["error_count"],
        )
        for row in rows
    ]


@router.get("/domain-failures", response_model=list[EnricherDomainFailure])
async def domain_failures(
    step: str | None = Query(default="enricher.url_text_extractor"),
    from_ts: datetime | None = Query(default=None),
    to_ts: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> list[EnricherDomainFailure]:
    params: EnricherParamsWithLimit = {
        "step": step,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "limit": limit,
    }
    rows = await run_query(
        session, "pipeline", "enrichers_domain_failures.sql", dict(params)
    )
    return [
        EnricherDomainFailure(
            step=row["step"],
            domain=row.get("domain", "unknown"),
            failure_count=row["failure_count"],
        )
        for row in rows
    ]


@router.get("/recent-activity", response_model=list[EnricherRecentActivity])
async def recent_activity(
    step: str | None = Query(default=None),
    from_ts: datetime | None = Query(
        default=None,
        description="Lower bound on created_at; defaults to last 24 hours if omitted",
    ),
    to_ts: datetime | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[EnricherRecentActivity]:
    effective_from = from_ts or _default_from_ts()
    params: EnricherParamsWithLimit = {
        "step": step,
        "from_ts": effective_from,
        "to_ts": to_ts,
        "limit": limit,
    }
    rows = await run_query(
        session, "pipeline", "enrichers_recent_activity.sql", dict(params)
    )
    return [
        EnricherRecentActivity(
            step=row["step"],
            recent_entries=row["recent_entries"],
            earliest=row.get("earliest"),
            latest=row.get("latest"),
            successful=row["successful"],
            failed=row["failed"],
        )
        for row in rows
    ]
