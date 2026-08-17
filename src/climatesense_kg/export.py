"""Streaming N-Triples export from canonical database projections."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import fcntl
import gzip
import logging
import os
from pathlib import Path
import shutil
import stat
import subprocess  # nosec B404
import tempfile
from typing import BinaryIO, Literal, cast

from .domain import CanonicalClaimReview
from .enrichment import EnrichmentService
from .projection import ReviewProjectionReader
from .rdf_generation.generator import RDFGenerator
from .utils.progress import ProgressLogger

logger = logging.getLogger(__name__)

ArtifactKind = Literal["source", "enrichment"]


@dataclass(frozen=True)
class RdfArtifact:
    graph_name: str
    kind: ArtifactKind
    path: Path
    items: int
    failed_items: int
    file_size: int
    incomplete_stages: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return self.failed_items == 0 and not self.incomplete_stages


@dataclass(frozen=True)
class ExportSummary:
    artifacts: tuple[RdfArtifact, ...]
    reviews: int
    successful_reviews: int
    failed_reviews: int
    total_file_size: int
    errors: tuple[str, ...]


@dataclass
class _Stream:
    graph_name: str
    kind: ArtifactKind
    path: Path
    temp_path: Path
    handle: BinaryIO
    items: int = 0
    failed_items: int = 0
    first_error: str | None = None


class RdfExporter:
    """Write one atomic N-Triples snapshot per managed graph."""

    def __init__(
        self,
        reader: ReviewProjectionReader,
        enrichment: EnrichmentService,
        generator: RDFGenerator,
        *,
        output_path_template: str,
        enrichment_graphs: dict[str, frozenset[str]],
        batch_size: int = 500,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("Export batch size must be positive")
        if "{SOURCE}" not in output_path_template:
            raise ValueError("RDF output path must contain {SOURCE}")
        self.reader = reader
        self.enrichment = enrichment
        self.generator = generator
        self.output_path_template = output_path_template
        self.enrichment_graphs = enrichment_graphs
        self.batch_size = batch_size
        self.progress_interval_seconds = progress_interval_seconds

    def run(
        self,
        source_graphs: tuple[str, ...],
        run_datetime: datetime,
        *,
        incomplete_stages_by_graph: dict[str, set[str]] | None = None,
    ) -> ExportSummary:
        incomplete = incomplete_stages_by_graph or {}
        streams = {
            graph: self._open(graph, "source", run_datetime) for graph in source_graphs
        }
        streams.update(
            {
                graph: self._open(graph, "enrichment", run_datetime)
                for graph in self.enrichment_graphs
            }
        )
        total = self.reader.count()
        progress = ProgressLogger(
            logger,
            "RDF export",
            total,
            interval_seconds=self.progress_interval_seconds,
        )
        processed = 0
        successful = 0
        failed = 0
        emitted_entity_properties = {graph: set() for graph in self.enrichment_graphs}
        try:
            for reviews in self.reader.iter_batches(batch_size=self.batch_size):
                self.enrichment.apply_stored(reviews)
                for review in reviews:
                    if self._write_review(
                        review,
                        streams,
                        emitted_entity_properties,
                    ):
                        successful += 1
                    else:
                        failed += 1
                processed += len(reviews)
                progress.update(processed, {"failed": failed})
            artifacts = self._finish(streams, incomplete)
        except BaseException:
            self._abort(streams)
            raise
        progress.update(processed, {"failed": failed}, force=True)
        errors = tuple(
            self._error_summary(stream)
            for stream in streams.values()
            if stream.failed_items
        )
        return ExportSummary(
            artifacts=tuple(artifacts),
            reviews=processed,
            successful_reviews=successful,
            failed_reviews=failed,
            total_file_size=sum(artifact.file_size for artifact in artifacts),
            errors=errors,
        )

    def _write_review(
        self,
        review: CanonicalClaimReview,
        streams: dict[str, _Stream],
        emitted_entity_properties: dict[str, set[str]],
    ) -> bool:
        failed = False
        for source in review.source_graphs():
            stream = streams.get(source)
            if stream is None:
                continue
            try:
                stream.handle.write(
                    self.generator.project_claim_review_nt(review.for_source(source))
                )
                stream.items += 1
            except Exception as exc:
                stream.failed_items += 1
                stream.first_error = stream.first_error or str(exc)
                failed = True
        for graph, entity_sources in self.enrichment_graphs.items():
            entity_uris = self.generator.entity_uris(review, entity_sources)
            if not entity_uris:
                continue
            stream = streams[graph]
            new_entity_uris = entity_uris.difference(emitted_entity_properties[graph])
            try:
                stream.handle.write(
                    self.generator.project_entity_enrichment_nt(
                        review,
                        entity_sources,
                        property_entity_uris=frozenset(new_entity_uris),
                    )
                )
                stream.items += 1
                emitted_entity_properties[graph].update(new_entity_uris)
            except Exception as exc:
                stream.failed_items += 1
                stream.first_error = stream.first_error or str(exc)
                failed = True
        return not failed

    def _open(
        self,
        graph: str,
        kind: ArtifactKind,
        run_datetime: datetime,
    ) -> _Stream:
        path = self._output_path(graph, run_datetime)
        path.parent.mkdir(parents=True, exist_ok=True)
        mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
        handle = tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        )
        os.fchmod(handle.fileno(), mode)
        return _Stream(
            graph,
            kind,
            path,
            Path(handle.name),
            cast(BinaryIO, handle),
        )

    def _finish(
        self,
        streams: dict[str, _Stream],
        incomplete: dict[str, set[str]],
    ) -> list[RdfArtifact]:
        artifacts: list[RdfArtifact] = []
        for stream in streams.values():
            stream.handle.flush()
            os.fsync(stream.handle.fileno())
            stream.handle.close()
            raw_size = stream.temp_path.stat().st_size
            file_size = raw_size
            incomplete_stages = tuple(sorted(incomplete.get(stream.graph_name, set())))
            if stream.failed_items or incomplete_stages:
                stream.temp_path.unlink()
            else:
                unique_path: Path | None = None
                compressed_path: Path | None = None
                try:
                    logger.info(
                        "RDF deduplication started: %s; raw=%d bytes",
                        stream.graph_name,
                        raw_size,
                    )
                    unique_path = self._deduplicate_nt(stream.temp_path)
                    unique_size = unique_path.stat().st_size
                    saved = raw_size - unique_size
                    logger.info(
                        "RDF deduplication: %s; raw=%d bytes, unique=%d bytes, "
                        "removed=%d bytes (%.1f%%)",
                        stream.graph_name,
                        raw_size,
                        unique_size,
                        saved,
                        100 * saved / raw_size if raw_size else 0.0,
                    )
                    compressed_path = self._compress_nt(unique_path)
                    file_size = compressed_path.stat().st_size
                    logger.info(
                        "RDF compression: %s; unique=%d bytes, gzip=%d bytes (%.1f%%)",
                        stream.graph_name,
                        unique_size,
                        file_size,
                        100 * file_size / unique_size if unique_size else 0.0,
                    )
                    lock_path = stream.path.with_suffix(f"{stream.path.suffix}.lock")
                    with lock_path.open("a+b") as lock:
                        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                        os.replace(compressed_path, stream.path)
                        compressed_path = None
                        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                finally:
                    stream.temp_path.unlink(missing_ok=True)
                    if unique_path is not None:
                        unique_path.unlink(missing_ok=True)
                    if compressed_path is not None:
                        compressed_path.unlink(missing_ok=True)
            artifacts.append(
                RdfArtifact(
                    graph_name=stream.graph_name,
                    kind=stream.kind,
                    path=stream.path,
                    items=stream.items,
                    failed_items=stream.failed_items,
                    file_size=file_size,
                    incomplete_stages=incomplete_stages,
                )
            )
        return artifacts

    @staticmethod
    def _deduplicate_nt(raw_path: Path) -> Path:
        """Create a lexically sorted, duplicate-free N-Triples file on disk."""

        mode = stat.S_IMODE(raw_path.stat().st_mode)
        unique_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w+b",
                dir=raw_path.parent,
                prefix=f".{raw_path.name}.",
                suffix=".unique.tmp",
                delete=False,
            ) as output:
                unique_path = Path(output.name)
                os.fchmod(output.fileno(), mode)
                environment = os.environ.copy()
                environment["LC_ALL"] = "C"
                environment["TMPDIR"] = str(raw_path.parent.resolve())
                sort_command = shutil.which("sort")
                if sort_command is None:
                    raise RuntimeError(
                        "N-Triples deduplication requires the system sort command"
                    )
                try:
                    completed = subprocess.run(  # noqa: S603 # nosec B603
                        [sort_command, "-u", str(raw_path)],
                        stdout=output,
                        stderr=subprocess.PIPE,
                        env=environment,
                        check=False,
                    )
                except FileNotFoundError as exc:
                    raise RuntimeError(
                        "N-Triples deduplication requires the system sort command"
                    ) from exc
                if completed.returncode:
                    error = completed.stderr.decode(errors="replace").strip()
                    raise RuntimeError(
                        f"N-Triples deduplication failed for {raw_path}: {error}"
                    )
                output.flush()
                os.fsync(output.fileno())
            result = unique_path
            unique_path = None
            return result
        finally:
            if unique_path is not None:
                unique_path.unlink(missing_ok=True)

    @staticmethod
    def _compress_nt(raw_path: Path) -> Path:
        """Gzip-compress a deduplicated N-Triples file into a sibling temp file."""

        mode = stat.S_IMODE(raw_path.stat().st_mode)
        compressed_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w+b",
                dir=raw_path.parent,
                prefix=f".{raw_path.name}.",
                suffix=".gz.tmp",
                delete=False,
            ) as output:
                compressed_path = Path(output.name)
                os.fchmod(output.fileno(), mode)
                with raw_path.open("rb") as source:
                    with gzip.GzipFile(fileobj=output, mode="wb") as compressed:
                        shutil.copyfileobj(source, compressed)
                output.flush()
                os.fsync(output.fileno())
            result = compressed_path
            compressed_path = None
            return result
        finally:
            if compressed_path is not None:
                compressed_path.unlink(missing_ok=True)

    @staticmethod
    def _abort(streams: dict[str, _Stream]) -> None:
        for stream in streams.values():
            try:
                stream.handle.close()
            finally:
                stream.temp_path.unlink(missing_ok=True)

    def _output_path(self, source: str, run_datetime: datetime) -> Path:
        replacements = {
            "{DATE}": run_datetime.strftime("%Y-%m-%d"),
            "{TIME}": run_datetime.strftime("%H%M%S"),
            "{DATETIME}": run_datetime.strftime("%Y-%m-%d_%H%M%S"),
            "{TIMESTAMP}": run_datetime.strftime("%Y%m%d%H%M%S"),
            "{SOURCE}": source,
        }
        value = self.output_path_template
        for placeholder, replacement in replacements.items():
            value = value.replace(placeholder, replacement)
        return Path(value)

    @staticmethod
    def _error_summary(stream: _Stream) -> str:
        result = f"{stream.graph_name}: {stream.failed_items} reviews failed projection"
        return (
            f"{result}; first error: {stream.first_error}"
            if stream.first_error
            else result
        )


def retain_latest_snapshots(rdf_root: Path, keep_latest: int) -> tuple[Path, ...]:
    """Delete oldest snapshot run directories, keeping the newest ones.

    ``keep_latest`` of 0 disables deletion. Directories still containing
    scratch files (``*.tmp``, ``*.lock``) are protected from deletion.

    Returns the removed directories.
    """
    if keep_latest <= 0:
        return ()
    if not rdf_root.is_dir():
        raise RuntimeError(f"RDF snapshot root is not a directory: {rdf_root}")
    candidates = sorted(
        (path for path in rdf_root.iterdir() if path.is_dir()),
        key=lambda path: path.name,
        reverse=True,
    )
    protected = set(candidates[:keep_latest])
    protected.update(
        path
        for path in candidates
        if any(child.suffix in (".tmp", ".lock") for child in path.iterdir())
    )
    removed: list[Path] = []
    failures: list[str] = []
    for path in candidates:
        if path in protected:
            continue
        try:
            shutil.rmtree(path)
        except OSError as exc:
            failures.append(f"{path}: {exc}")
            continue
        logger.info("Retention removed old snapshot directory: %s", path)
        removed.append(path)
    if failures:
        raise RuntimeError(
            "Failed to remove old RDF snapshot directories: " + "; ".join(failures)
        )
    return tuple(removed)
