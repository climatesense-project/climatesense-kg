"""Security assertions for the Docker Compose development stack."""

from pathlib import Path
from typing import Any

import yaml

_REPOSITORY_ROOT = Path(__file__).parents[2]
_COMPOSE_PATH = _REPOSITORY_ROOT / "docker" / "docker-compose.yml"
_ENV_EXAMPLE_PATH = _REPOSITORY_ROOT / "docker" / ".env.example"
_PIPELINE_DOCKERFILE_PATH = _REPOSITORY_ROOT / "docker" / "Dockerfile"


def _compose_services() -> dict[str, dict[str, Any]]:
    compose = yaml.safe_load(_COMPOSE_PATH.read_text())
    return compose["services"]


def test_database_ports_are_loopback_only() -> None:
    services = _compose_services()

    assert services["postgres"]["ports"] == [
        "${POSTGRES_BIND_ADDRESS:-127.0.0.1}:${POSTGRES_HOST_PORT:-5432}:5432"
    ]
    assert services["virtuoso"]["ports"] == [
        "${VIRTUOSO_BIND_ADDRESS:-127.0.0.1}:${VIRTUOSO_PORT:-8890}:8890"
    ]
    assert "ports" not in services["isql-service"]


def test_database_passwords_have_no_defaults() -> None:
    services = _compose_services()

    assert services["postgres"]["environment"]["POSTGRES_PASSWORD"] == (
        "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set}"  # noqa: S105
    )
    assert services["virtuoso"]["environment"]["DBA_PASSWORD"] == (
        "${VIRTUOSO_PASSWORD:?VIRTUOSO_PASSWORD must be set}"  # noqa: S105
    )

    env_example = _ENV_EXAMPLE_PATH.read_text().splitlines()
    assert "POSTGRES_PASSWORD=" in env_example
    assert "VIRTUOSO_PASSWORD=" in env_example


def test_pipeline_runs_as_host_mapped_non_root_user() -> None:
    pipeline = _compose_services()["pipeline"]
    dockerfile = _PIPELINE_DOCKERFILE_PATH.read_text()
    env_example = _ENV_EXAMPLE_PATH.read_text().splitlines()

    assert pipeline["build"]["args"] == {
        "APP_UID": "${PIPELINE_UID:-1000}",
        "APP_GID": "${PIPELINE_GID:-1000}",
    }
    assert "USER app" in dockerfile
    assert "PIPELINE_UID=1000" in env_example
    assert "PIPELINE_GID=1000" in env_example
