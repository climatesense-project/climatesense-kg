"""Tests for analytics SQL result caching."""

import asyncio
from unittest.mock import AsyncMock, Mock

from services.analytics_api.services import sql


def _query_result(rows: list[dict[str, object]]) -> Mock:
    result = Mock()
    result.mappings.return_value.all.return_value = rows
    return result


def test_result_cache_distinguishes_bound_parameters() -> None:
    sql.clear_cache()
    session = Mock()
    session.execute = AsyncMock(
        side_effect=[
            _query_result([{"error_count": 1}]),
            _query_result([{"error_count": 2}]),
        ]
    )

    first = asyncio.run(
        sql.run_query(
            session,
            "pipeline",
            "stages_error_types.sql",
            {"limit": 1},
        )
    )
    second = asyncio.run(
        sql.run_query(
            session,
            "pipeline",
            "stages_error_types.sql",
            {"limit": 2},
        )
    )
    cached_first = asyncio.run(
        sql.run_query(
            session,
            "pipeline",
            "stages_error_types.sql",
            {"limit": 1},
        )
    )

    assert first == [{"error_count": 1}]
    assert second == [{"error_count": 2}]
    assert cached_first == first
    assert session.execute.await_count == 2


def test_expired_sql_result_is_refreshed(monkeypatch) -> None:
    sql.clear_cache()
    monkeypatch.setattr(sql, "_RESULT_CACHE_TTL_SECONDS", 10)
    session = Mock()
    session.execute = AsyncMock(
        side_effect=[
            _query_result([{"error_count": 1}]),
            _query_result([{"error_count": 2}]),
        ]
    )

    first = asyncio.run(sql.run_query(session, "pipeline", "stages_error_types.sql"))
    cache_key = sql._result_cache_key("pipeline", "stages_error_types.sql", None)
    inserted_at, rows = sql._RESULT_CACHE[cache_key]
    sql._RESULT_CACHE[cache_key] = (inserted_at - 11, rows)
    refreshed = asyncio.run(
        sql.run_query(session, "pipeline", "stages_error_types.sql")
    )

    assert first == [{"error_count": 1}]
    assert refreshed == [{"error_count": 2}]
    assert session.execute.await_count == 2


def test_clear_discards_in_flight_query_result() -> None:
    async def run_race() -> list[dict[str, object]]:
        sql.clear_cache()
        query_started = asyncio.Event()
        release_query = asyncio.Event()

        async def delayed_result(*_args: object) -> Mock:
            query_started.set()
            await release_query.wait()
            return _query_result([{"error_count": 1}])

        session = Mock()
        session.execute = AsyncMock(side_effect=delayed_result)
        task = asyncio.create_task(
            sql.run_query(session, "pipeline", "stages_error_types.sql")
        )

        await query_started.wait()
        sql.clear_cache()
        release_query.set()
        return await task

    result = asyncio.run(run_race())

    assert result == [{"error_count": 1}]
    assert sql.get_cache_stats()["entry_count"] == 0
