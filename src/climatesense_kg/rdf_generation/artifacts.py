"""Build complete RDF graph artifacts from resolved reviews."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

from ..domain import CanonicalClaimReview
from .generator import RDFGenerator

ArtifactKind = Literal["source", "enrichment"]


@dataclass(frozen=True)
class RdfArtifact:
    """One complete named-graph snapshot ready for deployment."""

    graph_name: str
    kind: ArtifactKind
    path: Path
    items: int
    failed_items: int
    file_size: int
    review_uris: list[str]
    incomplete_stages: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        """Return whether the snapshot is safe to deploy."""

        return self.failed_items == 0 and not self.incomplete_stages


@dataclass(frozen=True)
class RdfBuildReport:
    """Outcome of projecting all source and enrichment graphs."""

    artifacts: list[RdfArtifact]
    input_items: int
    successful_items: int
    failed_items: int
    output_format: str
    total_file_size: int
    source_errors: list[str]
    enrichment_errors: list[str]


class RdfArtifactBuilder:
    """Group resolved reviews and write complete RDF snapshots."""

    def __init__(
        self,
        generator: RDFGenerator,
        *,
        output_path_template: str,
        output_format: str,
        enrichment_graphs: dict[str, frozenset[str]],
    ) -> None:
        self.generator = generator
        self.output_path_template = output_path_template
        self.output_format = output_format
        self.enrichment_graphs = enrichment_graphs

    def build(
        self,
        reviews: list[CanonicalClaimReview],
        *,
        successful_sources: list[str],
        run_datetime: datetime,
        incomplete_stages_by_graph: dict[str, set[str]] | None = None,
    ) -> RdfBuildReport:
        incomplete_stages_by_graph = incomplete_stages_by_graph or {}
        reviews_by_graph: dict[str, list[CanonicalClaimReview]] = {
            source: [] for source in successful_sources
        }
        for review in reviews:
            for source in review.source_graphs():
                if source in reviews_by_graph:
                    reviews_by_graph[source].append(review)

        graph_count = len(reviews_by_graph) + len(self.enrichment_graphs)
        if graph_count > 1 and "{SOURCE}" not in self.output_path_template:
            raise ValueError(
                "Multi-graph RDF output requires {SOURCE} in output.output_path"
            )

        artifacts: list[RdfArtifact] = []
        source_errors: list[str] = []
        enrichment_errors: list[str] = []
        failed_review_uris: set[str] = set()
        for graph_name, graph_reviews in reviews_by_graph.items():
            artifact, failed, error = self._build_graph(
                graph_name,
                "source",
                [review.for_source(graph_name) for review in graph_reviews],
                run_datetime,
                self.generator.save,
                incomplete_stages=incomplete_stages_by_graph.get(graph_name, set()),
            )
            if artifact is not None:
                artifacts.append(artifact)
            failed_review_uris.update(failed)
            if error:
                source_errors.append(error)

        for graph_name, entity_sources in self.enrichment_graphs.items():
            entity_reviews = [
                review
                for review in reviews
                if self.generator.has_entity_enrichment(review, entity_sources)
            ]
            artifact, failed, error = self._build_graph(
                graph_name,
                "enrichment",
                entity_reviews,
                run_datetime,
                lambda items, path, output_format, sources=entity_sources: (
                    self.generator.save_entity_enrichment(
                        items,
                        path,
                        output_format,
                        entity_sources=sources,
                    )
                ),
                incomplete_stages=incomplete_stages_by_graph.get(graph_name, set()),
            )
            if artifact is not None:
                artifacts.append(artifact)
            failed_review_uris.update(failed)
            if error:
                enrichment_errors.append(error)

        return RdfBuildReport(
            artifacts=artifacts,
            input_items=len(reviews),
            successful_items=len(reviews) - len(failed_review_uris),
            failed_items=len(failed_review_uris),
            output_format=self.output_format,
            total_file_size=sum(artifact.file_size for artifact in artifacts),
            source_errors=source_errors,
            enrichment_errors=enrichment_errors,
        )

    def _build_graph(
        self,
        graph_name: str,
        kind: ArtifactKind,
        reviews: list[CanonicalClaimReview],
        run_datetime: datetime,
        save: Callable[[list[CanonicalClaimReview], Path, str], list[str]],
        *,
        incomplete_stages: set[str] | None = None,
    ) -> tuple[RdfArtifact | None, set[str], str | None]:
        incomplete_stages = incomplete_stages or set()
        path = self._output_path(graph_name, run_datetime)
        try:
            successful = save(reviews, path, self.output_format)
        except Exception as exc:
            failed = {review.uri for review in reviews}
            return None, failed, f"{graph_name}: {exc}"
        successful_set = set(successful)
        failed = {review.uri for review in reviews if review.uri not in successful_set}
        error = (
            f"{graph_name}: {len(failed)} reviews failed projection" if failed else None
        )
        return (
            RdfArtifact(
                graph_name=graph_name,
                kind=kind,
                path=path,
                items=len(successful),
                failed_items=len(failed),
                file_size=path.stat().st_size,
                review_uris=successful,
                incomplete_stages=tuple(sorted(incomplete_stages)),
            ),
            failed,
            error,
        )

    def _output_path(self, source: str, run_datetime: datetime) -> Path:
        replacements = {
            "{DATE}": run_datetime.strftime("%Y-%m-%d"),
            "{TIME}": run_datetime.strftime("%H%M%S"),
            "{DATETIME}": run_datetime.strftime("%Y-%m-%d_%H%M%S"),
            "{TIMESTAMP}": run_datetime.strftime("%Y%m%d%H%M%S"),
            "{SOURCE}": source,
        }
        path = self.output_path_template
        for placeholder, value in replacements.items():
            path = path.replace(placeholder, value)
        return Path(path)
