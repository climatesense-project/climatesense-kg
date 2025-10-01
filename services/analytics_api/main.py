"""Entry point for the analytics FastAPI application."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .routers import cache, kg, pipeline

app = FastAPI(
    title="ClimateSense Analytics API",
    version="0.1.0",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.allowed_origins if origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(cache.router)
app.include_router(pipeline.router)
app.include_router(kg.router)


@app.get("/health")
async def health() -> dict[str, str]:
    """Simple health-check endpoint."""

    return {"status": "ok"}
