#!/usr/bin/env python3

"""
Internal FastAPI service for controlled RDF loader operations against Virtuoso.

Provides REST endpoints for query execution with connection management,
retry logic, and health monitoring.
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
import os
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pyodbc
from security import is_valid_bearer_token
import uvicorn


@dataclass
class AppConfig:
    """Configuration for the ISQL service application."""

    host: str
    port: int
    virtuoso_connection_string: str
    connection_timeout: int
    api_token: str


class ConnectionManager:
    """Manages the database connection."""

    def __init__(self, connection_string: str, timeout: int = 30):
        self.connection_string = connection_string
        self.timeout = timeout
        self.last_failure_time = 0
        self.failure_count = 0
        self.max_failures = 5
        self.base_retry_delay = 1  # seconds
        self.max_retry_delay = 30  # seconds

    def _calculate_retry_delay(self) -> float:
        """Calculate exponential backoff delay based on failure count."""
        if self.failure_count == 0:
            return 0
        delay = min(
            self.base_retry_delay * (2 ** (self.failure_count - 1)),
            self.max_retry_delay,
        )
        return delay

    def _should_retry(self) -> bool:
        """Check if enough time has passed since last failure to allow retry."""
        if self.failure_count == 0:
            return True

        current_time = time.time()
        time_since_failure = current_time - self.last_failure_time
        retry_delay = self._calculate_retry_delay()

        return time_since_failure >= retry_delay

    def get_connection(self) -> pyodbc.Connection:
        """Get a database connection."""
        if not self._should_retry():
            retry_delay = self._calculate_retry_delay()
            time_since_failure = time.time() - self.last_failure_time
            remaining_delay = retry_delay - time_since_failure
            raise ConnectionError(
                f"Connection failed recently. Retry in {remaining_delay:.1f} seconds. "
                f"Failure count: {self.failure_count}/{self.max_failures}"
            )

        try:
            connection = pyodbc.connect(
                self.connection_string,
                timeout=self.timeout,
                autocommit=True,
            )
            self.failure_count = 0
            self.last_failure_time = 0
            logger.info("Successfully established database connection")
            return connection
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.max_failures:
                logger.error(
                    f"Maximum connection failures reached ({self.max_failures}). "
                    f"Virtuoso may be down or misconfigured."
                )
            else:
                next_retry = self._calculate_retry_delay()
                logger.warning(
                    f"Connection attempt {self.failure_count} failed: {e}. "
                    f"Next retry in {next_retry} seconds."
                )

            raise ConnectionError(f"Failed to connect to database: {e}") from e

    def test_connection(self) -> bool:
        """Test database connectivity with a simple query."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True
        except Exception:
            return False


class QueryRequest(BaseModel):
    """Request model for SPARQL/SQL queries."""

    query: str


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""

    status: str
    message: str | None = None


class QueryResponse(BaseModel):
    """Response model for query execution results."""

    results: list[Any]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("api-server.log")],
)
logger = logging.getLogger(__name__)


def _required_secret(name: str) -> str:
    """Read a required secret without supplying a weak fallback."""
    value = os.getenv(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


# Configuration
virtuoso_password = _required_secret("VIRTUOSO_PASSWORD")
config = AppConfig(
    host=os.getenv("HOST", "127.0.0.1"),
    port=int(os.getenv("PORT", "50118")),
    virtuoso_connection_string=(
        f"DRIVER=/usr/lib/odbc/virtodbc.so;"
        f"HOST={os.getenv('VIRTUOSO_HOST')};"
        f"PORT={os.getenv('VIRTUOSO_ISQL_PORT')};"
        f"UID={os.getenv('VIRTUOSO_USER', 'dba')};"
        f"PWD={virtuoso_password};"
    ),
    connection_timeout=30,
    api_token=_required_secret("ISQL_SERVICE_TOKEN"),
)

connection_manager = ConnectionManager(
    config.virtuoso_connection_string, config.connection_timeout
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app startup and shutdown."""
    await startup_event()
    yield
    await shutdown_event()


app = FastAPI(title="ISQL Service", version="1.0.0", lifespan=lifespan)


@app.exception_handler(Exception)
async def exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors."""
    logger.error(f"Error occurred: {exc}")
    return JSONResponse(
        status_code=500, content={"error": "Internal server error", "message": str(exc)}
    )


@app.get("/health", response_model=HealthResponse)
def health_check():
    """Health check endpoint to verify database connectivity."""
    try:
        is_healthy = connection_manager.test_connection()
        if is_healthy:
            return HealthResponse(status="healthy", message="Connection successful")
        else:
            failure_count = connection_manager.failure_count
            max_failures = connection_manager.max_failures
            raise HTTPException(
                status_code=503,
                detail={
                    "status": "unhealthy",
                    "message": f"Database connection failed (attempts: {failure_count}/{max_failures})",
                },
            )
    except ConnectionError as e:
        raise HTTPException(
            status_code=503,
            detail={"status": "unhealthy", "message": str(e)},
        ) from e
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503, detail={"status": "unhealthy", "message": str(e)}
        ) from e


@app.post("/sql", response_model=QueryResponse)
def execute_sql_query(query_request: QueryRequest, http_request: Request):
    """Execute an authenticated SQL query against the database."""
    authorization = http_request.headers.get("Authorization", "")
    if not is_valid_bearer_token(authorization, config.api_token):
        raise HTTPException(
            status_code=401,
            detail={"error": "Unauthorized"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not query_request.query:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Query is required",
                "message": "The query parameter is missing in the request body",
            },
        )

    conn = None
    cursor = None
    try:
        conn = connection_manager.get_connection()
        conn.timeout = 7200
        cursor = conn.cursor()

        logger.info(f"Executing SQL query: {query_request.query}")
        cursor.execute(query_request.query)

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
            logger.info(f"SQL statement executed successfully: {query_request.query}")

        return QueryResponse(results=result_list)
    except ConnectionError as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(
            status_code=503,
            detail={"error": "Database connection failed", "message": str(e)},
        ) from e
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        raise HTTPException(
            status_code=400,
            detail={"error": "Query execution failed", "message": str(e)},
        ) from e
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                logger.warning(f"Failed to close database cursor: {e}")
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Failed to close database connection: {e}")


async def startup_event():
    """Initialize connection and test database connectivity on startup."""
    logger.info(f"API server starting on port {config.port}")
    logger.info("Testing initial database connection...")

    try:
        if connection_manager.test_connection():
            logger.info("Successfully connected to Virtuoso triplestore")
        else:
            logger.warning("Virtuoso not available at startup - will retry on demand")
    except Exception as e:
        logger.warning(f"Initial connection test failed: {e} - will retry on demand")


async def shutdown_event():
    """Handle graceful server shutdown."""
    logger.info("Shutting down server...")
    logger.info("Server shutdown complete")


if __name__ == "__main__":
    uvicorn.run("server:app", host=config.host, port=config.port, log_level="info")
