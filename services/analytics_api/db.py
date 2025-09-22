"""Database utilities for the analytics API."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Create (or return cached) async SQLAlchemy engine."""
    global _engine

    if _engine is None:
        _engine = create_async_engine(settings.database_dsn, future=True)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return an async session factory bound to the engine."""
    global _session_factory

    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(),
            expire_on_commit=False,
        )
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
    """Provide an async transactional scope."""

    session_factory = get_session_factory()
    session = session_factory()
    try:
        yield session
    finally:
        await session.close()


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an async session."""

    async with session_scope() as session:
        yield session
