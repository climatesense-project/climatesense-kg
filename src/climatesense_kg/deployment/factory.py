"""Deployment handler construction."""

import os

from ..config.schemas import DeploymentConfig
from .base import DeploymentHandler
from .qlever import QLeverDeploymentHandler
from .virtuoso import VirtuosoDeploymentHandler


def create_deployment_handler(
    config: DeploymentConfig,
) -> DeploymentHandler | None:
    """Create the configured deployment backend from config and environment."""
    if config.backend == "none":
        return None

    if config.backend == "virtuoso":
        return VirtuosoDeploymentHandler(
            host=os.getenv("VIRTUOSO_HOST", "localhost"),
            port=int(os.getenv("VIRTUOSO_PORT", "8890")),
            user=os.getenv("VIRTUOSO_USER", "dba"),
            password=os.getenv("VIRTUOSO_PASSWORD", ""),
            graph_template=config.graph_template,
            isql_service_url=os.getenv(
                "VIRTUOSO_ISQL_SERVICE_URL", "http://isql-service:8080"
            ),
            isql_service_token=os.getenv("ISQL_SERVICE_TOKEN", ""),
        )

    if config.backend == "qlever":
        return QLeverDeploymentHandler(
            endpoint=os.getenv("QLEVER_ENDPOINT", "http://localhost:7019"),
            access_token=os.getenv("QLEVER_ACCESS_TOKEN", ""),
            graph_template=config.graph_template,
            timeout=int(os.getenv("QLEVER_UPLOAD_TIMEOUT_SECONDS", "7200")),
        )

    raise ValueError(f"Unsupported deployment backend: {config.backend}")
