"""Cache management endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from ..routers import kg as kg_router
from ..routers import pipeline as pipeline_router
from ..services import sparql, sql

router = APIRouter(prefix="/cache", tags=["cache"])


class CacheStats(BaseModel):
    """Cache statistics response model."""

    sql_cache_entries: int
    sparql_cache_entries: int
    sql_cached_queries: list[str]
    sparql_cached_queries: list[str]


class CacheOperationResult(BaseModel):
    """Result of a cache operation."""

    success: bool
    message: str
    sql_entries_cleared: int | None = None
    sparql_entries_cleared: int | None = None
    sql_entries_warmed: int | None = None
    sparql_entries_warmed: int | None = None


@router.get("/status", response_model=CacheStats)
async def cache_status() -> CacheStats:
    """Get current cache statistics.

    Returns:
        Statistics about SQL and SPARQL cache state
    """
    sql_stats = sql.get_cache_stats()
    sparql_stats = sparql.get_cache_stats()

    return CacheStats(
        sql_cache_entries=sql_stats["entry_count"],
        sparql_cache_entries=sparql_stats["entry_count"],
        sql_cached_queries=sql_stats["cached_queries"],
        sparql_cached_queries=sparql_stats["cached_queries"],
    )


@router.post("/clear", response_model=CacheOperationResult)
async def clear_cache() -> CacheOperationResult:
    """Clear all cached query results (SQL and SPARQL).

    This endpoint is useful for debugging or when you want to force
    fresh data to be fetched on the next request.

    Returns:
        Result indicating how many cache entries were cleared
    """
    sql_cleared = sql.clear_cache()
    sparql_cleared = sparql.clear_cache()

    return CacheOperationResult(
        success=True,
        message=f"Cleared {sql_cleared} SQL and {sparql_cleared} SPARQL cache entries",
        sql_entries_cleared=sql_cleared,
        sparql_entries_cleared=sparql_cleared,
    )


@router.post("/refresh", response_model=CacheOperationResult)
async def refresh_cache(
    session: AsyncSession = Depends(get_session),
) -> CacheOperationResult:
    """Refresh all caches by clearing and re-executing all queries.

    This endpoint should be called after the pipeline completes to ensure
    the UI displays fresh data. It executes all known queries to pre-warm
    the cache.

    Returns:
        Result indicating cache refresh status
    """
    # Clear existing caches
    sql_cleared = sql.clear_cache()
    sparql_cleared = sparql.clear_cache()

    # Re-execute all SQL queries to warm the cache
    sql_queries_warmed = 0
    try:
        # Pipeline metrics queries
        await pipeline_router.success_rate(
            step=None, from_ts=None, to_ts=None, session=session
        )
        sql_queries_warmed += 1

        await pipeline_router.error_types(
            step=None, from_ts=None, to_ts=None, limit=200, session=session
        )
        sql_queries_warmed += 1

        await pipeline_router.domain_failures(
            step=None, from_ts=None, to_ts=None, limit=50, session=session
        )
        sql_queries_warmed += 1

        await pipeline_router.recent_activity(
            step=None, from_ts=None, to_ts=None, limit=20, session=session
        )
        sql_queries_warmed += 1

    except Exception as e:
        return CacheOperationResult(
            success=False,
            message=f"Failed to warm SQL cache: {e!s}",
            sql_entries_cleared=sql_cleared,
            sparql_entries_cleared=sparql_cleared,
        )

    # Re-execute all SPARQL queries to warm the cache
    sparql_queries_warmed = 0
    try:
        await kg_router.triple_volume()
        sparql_queries_warmed += 1

        await kg_router.class_distribution()
        sparql_queries_warmed += 1

        await kg_router.core_counts()
        sparql_queries_warmed += 1

        await kg_router.enrichment_coverage()
        sparql_queries_warmed += 1

        await kg_router.entity_types()
        sparql_queries_warmed += 1

        await kg_router.claim_factors()
        sparql_queries_warmed += 1

    except Exception as e:
        return CacheOperationResult(
            success=False,
            message=f"Failed to warm SPARQL cache: {e!s}",
            sql_entries_cleared=sql_cleared,
            sparql_entries_cleared=sparql_cleared,
            sql_entries_warmed=sql_queries_warmed,
        )

    return CacheOperationResult(
        success=True,
        message=f"Cache refreshed: warmed {sql_queries_warmed} SQL and {sparql_queries_warmed} SPARQL queries",
        sql_entries_cleared=sql_cleared,
        sparql_entries_cleared=sparql_cleared,
        sql_entries_warmed=sql_queries_warmed,
        sparql_entries_warmed=sparql_queries_warmed,
    )
