"""Best-effort deployment of complete RDF graph snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from ..config.graphs import GRAPH_CATALOG_PATH, GRAPH_CATALOG_SOURCE_NAME
from ..config.organizations import ORGANIZATION_CATALOG_PATH, ORGANIZATION_SOURCE_NAME
from ..export import RdfArtifact
from .base import DeploymentHandler


@dataclass(frozen=True)
class DeploymentTarget:
    """One file and named graph in a deployment plan."""

    path: Path
    graph_name: str
    kind: Literal["catalog", "generated"]


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
    skipped_graphs: tuple[str, ...] = ()

    @property
    def files_deployed(self) -> int:
        return sum(outcome.success for outcome in self.outcomes)

    @property
    def skipped_files(self) -> int:
        return len(self.skipped_graphs)


@dataclass(frozen=True)
class ArtifactDeploymentPlan:
    """Deployment targets plus graphs that must retain their current snapshot."""

    targets: tuple[DeploymentTarget, ...]
    skipped_graphs: tuple[str, ...] = ()

    @property
    def total_files(self) -> int:
        return len(self.targets) + len(self.skipped_graphs)

    @property
    def skipped_files(self) -> int:
        return len(self.skipped_graphs)


def plan_artifact_deployment(
    artifacts: list[RdfArtifact],
) -> ArtifactDeploymentPlan:
    """Build the same completeness-aware plan for dry and real deployment."""

    deployable = [artifact for artifact in artifacts if artifact.complete]
    skipped_graphs = tuple(
        artifact.graph_name for artifact in artifacts if not artifact.complete
    )
    return ArtifactDeploymentPlan(
        targets=(
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
                DeploymentTarget(artifact.path, artifact.graph_name, "generated")
                for artifact in deployable
            ),
        ),
        skipped_graphs=skipped_graphs,
    )


class ArtifactDeployer:
    """Replace each curated and generated named graph with a full snapshot."""

    def __init__(self, handler: DeploymentHandler | None) -> None:
        self.handler = handler

    def deploy(self, artifacts: list[RdfArtifact]) -> ArtifactDeploymentReport:
        return self._deploy_plan(plan_artifact_deployment(artifacts))

    def deploy_files(
        self, generated_files: list[tuple[Path, str]]
    ) -> ArtifactDeploymentReport:
        """Execute the shared full-snapshot plan used by run and redeploy."""

        plan = ArtifactDeploymentPlan(
            targets=(
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
            )
        )
        return self._deploy_plan(plan)

    def _deploy_plan(self, plan: ArtifactDeploymentPlan) -> ArtifactDeploymentReport:
        if self.handler is None:
            return ArtifactDeploymentReport(
                True,
                [],
                plan.total_files,
                skipped_graphs=plan.skipped_graphs,
            )

        outcomes = [
            DeploymentOutcome(
                target,
                self.handler.deploy(
                    target.path,
                    target.graph_name,
                    replace=True,
                ),
            )
            for target in plan.targets
        ]
        return ArtifactDeploymentReport(
            success=all(outcome.success for outcome in outcomes),
            outcomes=outcomes,
            total_files=plan.total_files,
            skipped_graphs=plan.skipped_graphs,
        )
