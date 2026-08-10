"""Best-effort deployment of complete RDF graph snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..config.graphs import GRAPH_CATALOG_PATH, GRAPH_CATALOG_SOURCE_NAME
from ..config.organizations import ORGANIZATION_CATALOG_PATH, ORGANIZATION_SOURCE_NAME
from ..rdf_generation.artifacts import RdfArtifact
from .base import DeploymentHandler


@dataclass(frozen=True)
class DeploymentTarget:
    """One file and named graph in a deployment plan."""

    path: Path
    graph_name: str
    kind: str


@dataclass(frozen=True)
class DeploymentOutcome:
    """Observed result for one attempted target."""

    target: DeploymentTarget
    success: bool


@dataclass(frozen=True)
class ArtifactDeploymentReport:
    """Aggregate best-effort deployment outcome."""

    success: bool
    outcomes: list[DeploymentOutcome]
    total_files: int

    @property
    def files_deployed(self) -> int:
        return sum(outcome.success for outcome in self.outcomes)


class ArtifactDeployer:
    """Replace each curated and generated named graph with a full snapshot."""

    def __init__(self, handler: DeploymentHandler | None) -> None:
        self.handler = handler

    def deploy(self, artifacts: list[RdfArtifact]) -> ArtifactDeploymentReport:
        return self.deploy_files(
            [(artifact.path, artifact.graph_name) for artifact in artifacts]
        )

    def deploy_files(
        self, generated_files: list[tuple[Path, str]]
    ) -> ArtifactDeploymentReport:
        """Execute the shared full-snapshot plan used by run and redeploy."""

        targets = [
            DeploymentTarget(
                GRAPH_CATALOG_PATH,
                GRAPH_CATALOG_SOURCE_NAME,
                "catalog",
            ),
            DeploymentTarget(
                ORGANIZATION_CATALOG_PATH,
                ORGANIZATION_SOURCE_NAME,
                "catalog",
            ),
            *(
                DeploymentTarget(path, graph_name, "generated")
                for path, graph_name in generated_files
            ),
        ]
        if self.handler is None:
            return ArtifactDeploymentReport(True, [], len(targets))

        outcomes = [
            DeploymentOutcome(
                target,
                self.handler.deploy(
                    target.path,
                    target.graph_name,
                    replace=True,
                ),
            )
            for target in targets
        ]
        return ArtifactDeploymentReport(
            success=all(outcome.success for outcome in outcomes),
            outcomes=outcomes,
            total_files=len(targets),
        )
