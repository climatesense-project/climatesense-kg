"""Build complete RDF graph artifacts from resolved reviews."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
import fcntl
import logging
import os
from pathlib import Path
import stat
import tempfile
from typing import BinaryIO, Literal

from ..domain import CanonicalClaimReview
from ..utils.memory import format_process_rss
from .generator import RDFGenerator

ArtifactKind = Literal["source", "enrichment"]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RdfArtifact:
    """One complete named-graph snapshot ready for deployment."""

    graph_name: str
    kind: ArtifactKind
    path: Path
    items: int
    failed_items: int
    file_size: int
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

    def start(
        self,
        *,
        successful_sources: list[str],
        run_datetime: datetime,
    ) -> RdfBuildSession:
        """Start a bounded artifact build for the configured output format."""

        graph_count = len(successful_sources) + len(self.enrichment_graphs)
        if graph_count > 1 and "{SOURCE}" not in self.output_path_template:
            raise ValueError(
                "Multi-graph RDF output requires {SOURCE} in output.output_path"
            )
        if self.output_format == "nt":
            return _StreamingNTriplesSession(
                self,
                successful_sources=successful_sources,
                run_datetime=run_datetime,
            )
        return _BufferedRdfSession(
            self,
            successful_sources=successful_sources,
            run_datetime=run_datetime,
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


class RdfBuildSession:
    """Incremental artifact build contract used by the pipeline."""

    def add(self, reviews: list[CanonicalClaimReview]) -> None:
        raise NotImplementedError

    def finish(
        self, *, incomplete_stages_by_graph: dict[str, set[str]] | None = None
    ) -> RdfBuildReport:
        raise NotImplementedError

    def abort(self) -> None:
        raise NotImplementedError


class _BufferedRdfSession(RdfBuildSession):
    """Buffer RDF formats whose documents cannot be safely concatenated."""

    def __init__(
        self,
        builder: RdfArtifactBuilder,
        *,
        successful_sources: list[str],
        run_datetime: datetime,
    ) -> None:
        self.builder = builder
        self.successful_sources = successful_sources
        self.run_datetime = run_datetime
        self.reviews: list[CanonicalClaimReview] = []

    def add(self, reviews: list[CanonicalClaimReview]) -> None:
        self.reviews.extend(reviews)

    def finish(
        self, *, incomplete_stages_by_graph: dict[str, set[str]] | None = None
    ) -> RdfBuildReport:
        return self.builder.build(
            self.reviews,
            successful_sources=self.successful_sources,
            run_datetime=self.run_datetime,
            incomplete_stages_by_graph=incomplete_stages_by_graph,
        )

    def abort(self) -> None:
        self.reviews.clear()


@dataclass
class _GraphStream:
    graph_name: str
    kind: ArtifactKind
    path: Path
    temp_path: Path
    handle: BinaryIO
    items: int = 0
    failed_items: int = 0
    first_error: str | None = None


class _StreamingNTriplesSession(RdfBuildSession):
    """Write independent review fragments to atomic graph snapshots."""

    def __init__(
        self,
        builder: RdfArtifactBuilder,
        *,
        successful_sources: list[str],
        run_datetime: datetime,
    ) -> None:
        self.builder = builder
        self.successful_sources = successful_sources
        self.run_datetime = run_datetime
        self.streams = {
            graph_name: self._open_stream(graph_name, "source")
            for graph_name in successful_sources
        }
        self.streams.update(
            {
                graph_name: self._open_stream(graph_name, "enrichment")
                for graph_name in builder.enrichment_graphs
            }
        )
        self.input_items = 0
        self.successful_items = 0
        self.failed_items = 0
        self.source_errors: list[str] = []
        self.enrichment_errors: list[str] = []
        self.closed = False

    def _open_stream(self, graph_name: str, kind: ArtifactKind) -> _GraphStream:
        path = self.builder._output_path(graph_name, self.run_datetime)
        path.parent.mkdir(parents=True, exist_ok=True)
        output_mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
        handle = tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        )
        os.fchmod(handle.fileno(), output_mode)
        return _GraphStream(
            graph_name=graph_name,
            kind=kind,
            path=path,
            temp_path=Path(handle.name),
            handle=handle,
        )

    def add(self, reviews: list[CanonicalClaimReview]) -> None:
        if self.closed:
            raise RuntimeError("RDF build session is already closed")
        for review in reviews:
            self.input_items += 1
            failed = False
            for source in review.source_graphs():
                stream = self.streams.get(source)
                if stream is None:
                    continue
                try:
                    fragment = self.builder.generator.project_claim_review_nt(
                        review.for_source(source)
                    )
                    stream.handle.write(fragment)
                    stream.items += 1
                except Exception as exc:
                    stream.failed_items += 1
                    stream.first_error = stream.first_error or str(exc)
                    failed = True

            for graph_name, entity_sources in self.builder.enrichment_graphs.items():
                if not self.builder.generator.has_entity_enrichment(
                    review, entity_sources
                ):
                    continue
                stream = self.streams[graph_name]
                try:
                    fragment = self.builder.generator.project_entity_enrichment_nt(
                        review,
                        entity_sources,
                    )
                    stream.handle.write(fragment)
                    stream.items += 1
                except Exception as exc:
                    stream.failed_items += 1
                    stream.first_error = stream.first_error or str(exc)
                    failed = True
            if failed:
                self.failed_items += 1
            else:
                self.successful_items += 1

    def finish(
        self, *, incomplete_stages_by_graph: dict[str, set[str]] | None = None
    ) -> RdfBuildReport:
        if self.closed:
            raise RuntimeError("RDF build session is already closed")
        incomplete_stages_by_graph = incomplete_stages_by_graph or {}
        artifacts: list[RdfArtifact] = []
        try:
            for stream in self.streams.values():
                stream.handle.flush()
                os.fsync(stream.handle.fileno())
                stream.handle.close()
                lock_path = stream.path.with_suffix(f"{stream.path.suffix}.lock")
                with lock_path.open("a+b") as lock_file:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                    os.replace(stream.temp_path, stream.path)
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                artifacts.append(
                    RdfArtifact(
                        graph_name=stream.graph_name,
                        kind=stream.kind,
                        path=stream.path,
                        items=stream.items,
                        failed_items=stream.failed_items,
                        file_size=stream.path.stat().st_size,
                        incomplete_stages=tuple(
                            sorted(
                                incomplete_stages_by_graph.get(stream.graph_name, set())
                            )
                        ),
                    )
                )
        except Exception:
            self.abort()
            raise
        self.closed = True
        self.source_errors = [
            self._error_summary(stream)
            for stream in self.streams.values()
            if stream.kind == "source" and stream.failed_items
        ]
        self.enrichment_errors = [
            self._error_summary(stream)
            for stream in self.streams.values()
            if stream.kind == "enrichment" and stream.failed_items
        ]
        logger.info(
            "RDF projection finished: %d reviews, %d failed; RSS=%s",
            self.input_items,
            self.failed_items,
            format_process_rss(),
        )
        return RdfBuildReport(
            artifacts=artifacts,
            input_items=self.input_items,
            successful_items=self.successful_items,
            failed_items=self.failed_items,
            output_format="nt",
            total_file_size=sum(artifact.file_size for artifact in artifacts),
            source_errors=self.source_errors,
            enrichment_errors=self.enrichment_errors,
        )

    @staticmethod
    def _error_summary(stream: _GraphStream) -> str:
        summary = (
            f"{stream.graph_name}: {stream.failed_items} reviews failed projection"
        )
        return (
            f"{summary}; first error: {stream.first_error}"
            if stream.first_error
            else summary
        )

    def abort(self) -> None:
        if self.closed:
            return
        for stream in self.streams.values():
            try:
                stream.handle.close()
            finally:
                stream.temp_path.unlink(missing_ok=True)
        self.closed = True
