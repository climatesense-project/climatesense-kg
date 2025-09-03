#!/usr/bin/env python3

from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
import os
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pyodbc
import uvicorn


@dataclass
class AppConfig:
    host: str
    port: int
    virtuoso_connection_string: str
    connection_timeout: int


class QueryRequest(BaseModel):
    query: str


class HealthResponse(BaseModel):
    status: str
    message: str | None = None


class QueryResponse(BaseModel):
    results: list[Any]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("api-server.log")],
)
logger = logging.getLogger(__name__)

# Configuration
config = AppConfig(
    host=os.getenv("HOST", "127.0.0.1"),
    port=int(os.getenv("PORT", "50118")),
    virtuoso_connection_string=(
        f"DRIVER=/usr/lib/x86_64-linux-gnu/odbc/virtodbc.so;"
        f"HOST={os.getenv('VIRTUOSO_HOST')};"
        f"PORT={os.getenv('VIRTUOSO_ISQL_PORT')};"
        f"UID={os.getenv('VIRTUOSO_USER', 'dba')};"
        f"PWD={os.getenv('VIRTUOSO_PASSWORD', 'dba')};"
    ),
    connection_timeout=30,
)

# Global connection variable
connection_pool: pyodbc.Connection | None = None


def get_connection() -> pyodbc.Connection:
    """Get a new database connection."""
    return pyodbc.connect(
        config.virtuoso_connection_string, timeout=config.connection_timeout
    )


async def initialize_connection_pool() -> bool:
    """Initialize the database connection."""
    global connection_pool
    try:
        connection_pool = get_connection()
        logger.info("Successfully connected to Virtuoso triplestore")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to Virtuoso triplestore: {e}")
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app."""
    # Startup
    await startup_event()
    yield
    # Shutdown
    await shutdown_event()


app = FastAPI(title="ISQL Service", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def exception_handler(request: Request, exc: Exception):
    """Global exception handler."""
    logger.error(f"Error occurred: {exc}")
    return JSONResponse(
        status_code=500, content={"error": "Internal server error", "message": str(exc)}
    )


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    if connection_pool:
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return HealthResponse(status="healthy", message="Connection successful")
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise HTTPException(
                status_code=503, detail={"status": "unhealthy", "message": str(e)}
            ) from e
    else:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "message": "Connection pool not initialized",
            },
        )


@app.post("/sparql", response_model=QueryResponse)
async def execute_sparql_query(request: QueryRequest):
    """Execute SPARQL query."""
    if not request.query:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Query is required",
                "message": "The query parameter is missing in the request body",
            },
        )

    try:
        if not connection_pool:
            raise Exception("Connection pool not initialized")

        conn = get_connection()
        cursor = conn.cursor()
        prefixed_query = f"SPARQL {request.query}"

        logger.info(f"Executing SPARQL query: {prefixed_query}")
        cursor.execute(prefixed_query)
        results = cursor.fetchall()

        # Convert results to list of dictionaries
        columns = [column[0] for column in cursor.description]
        result_list = [dict(zip(columns, row, strict=False)) for row in results]

        cursor.close()
        conn.close()

        return QueryResponse(results=result_list)
    except Exception as e:
        logger.error(f"Error executing SPARQL query: {e}")
        raise HTTPException(
            status_code=400,
            detail={"error": "Query execution failed", "message": str(e)},
        ) from e


@app.post("/sql", response_model=QueryResponse)
async def execute_sql_query(request: QueryRequest):
    """Execute SQL query."""
    if not request.query:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Query is required",
                "message": "The query parameter is missing in the request body",
            },
        )

    try:
        if not connection_pool:
            raise Exception("Connection pool not initialized")

        conn = get_connection()
        cursor = conn.cursor()

        logger.info(f"Executing SQL query: {request.query}")
        cursor.execute(request.query)

        # Check if this is a query that returns results or just a statement
        result_list = []
        try:
            results = cursor.fetchall()
            # Convert results to list of dictionaries
            columns = [column[0] for column in cursor.description]
            result_list = [dict(zip(columns, row, strict=False)) for row in results]
        except Exception:
            # This is expected for non-query statements like DELETE, INSERT, UPDATE, etc.
            # Just return empty results and log success
            logger.info(f"SQL statement executed successfully: {request.query}")

        cursor.close()
        conn.close()

        return QueryResponse(results=result_list)
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        raise HTTPException(
            status_code=400,
            detail={"error": "Query execution failed", "message": str(e)},
        ) from e


async def startup_event():
    """Initialize connection pool on startup."""
    success = await initialize_connection_pool()
    if not success:
        logger.error("Failed to initialize connection pool")
    else:
        logger.info(f"API server starting on port {config.port}")


async def shutdown_event():
    """Handle graceful shutdown."""
    logger.info("Shutting down server...")

    global connection_pool
    if connection_pool:
        try:
            connection_pool.close()
            logger.info("Database connection pool closed")
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")


if __name__ == "__main__":
    uvicorn.run("server:app", host=config.host, port=config.port, log_level="info")
