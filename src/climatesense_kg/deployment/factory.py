"""Deployment handler construction."""

import os

from ..config.schemas import DeploymentConfig
from .base import DeploymentHandler
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

    raise ValueError(f"Unsupported deployment backend: {config.backend}")
