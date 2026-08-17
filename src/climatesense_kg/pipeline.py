"""ClimateSense KG pipeline coordination."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import logging
from pathlib import Path
import time

from dotenv import load_dotenv

from .bootstrap import PipelineServices, build_services
from .config import PipelineConfig
from .database import PipelineRun
from .export import ExportSummary, retain_latest_snapshots
from .identity import IdentitySummary
from .ingestion import IngestionSummary
from .processing import StageSummary, stable_hash
from .utils.logging import configure_external_loggers, setup_logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineSummary:
    success: bool
    degraded: bool
    duration_seconds: float
    ingestion: IngestionSummary | None = None
    extraction: StageSummary | None = None
    identity: IdentitySummary | None = None
    enrichments: tuple[StageSummary, ...] = ()
    export: ExportSummary | None = None
    error: str | None = None

    @property
    def reviews(self) -> int:
        return self.export.reviews if self.export else 0


class Pipeline:
    """Run the ordered services and finalize one durable execution."""

    def __init__(
        self,
        config: PipelineConfig,
        services: PipelineServices | None = None,
    ) -> None:
        load_dotenv()
        self.config = config
        setup_logging(config.logging)
        configure_external_loggers()
        self.services = services or build_services(config)

    def run(
        self,
        *,
        cached_sources_only: bool = False,
        offline_extraction: bool = False,
        offline_enrichment: bool = False,
        force: bool = False,
    ) -> PipelineSummary:
        started = time.monotonic()
        run: PipelineRun | None = None
        result: PipelineSummary | None = None
        try:
            run = self.services.database.start_run(stable_hash(asdict(self.config)))
            ingestion = self.services.ingestion.run(
                run.id,
                self.config,
                cached_only=cached_sources_only,
            )
            if not ingestion.successful_sources:
                raise RuntimeError("Every enabled source failed ingestion")
            extraction = (
                self.services.extraction.run(
                    offline=offline_extraction,
                    force=force,
                )
                if self.services.extraction
                else None
            )
            identity = self.services.identity.run()
            enrichments = tuple(
                self.services.enrichment.run(
                    offline=offline_enrichment,
                    force=force,
                )
            )
            incomplete = self._incomplete_graph_stages(
                ingestion.successful_sources,
                enrichments,
            )
            exported = self.services.exporter.run(
                ingestion.successful_sources,
                datetime.now(),
                incomplete_stages_by_graph=incomplete,
            )
            keep_latest = self.config.output.retention.keep_latest
            if keep_latest > 0:
                rdf_root = Path(self.config.output.output_path).parent.parent
                removed = retain_latest_snapshots(rdf_root, keep_latest)
                if removed:
                    logger.info(
                        "Retention removed %d old snapshot dir(s) from %s",
                        len(removed),
                        rdf_root,
                    )
            degraded = bool(
                ingestion.failed_sources
                or (extraction and not extraction.complete)
                or any(not stage.complete for stage in enrichments)
                or exported.errors
            )
            result = PipelineSummary(
                success=True,
                degraded=degraded,
                duration_seconds=time.monotonic() - started,
                ingestion=ingestion,
                extraction=extraction,
                identity=identity,
                enrichments=enrichments,
                export=exported,
            )
            return result
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            logger.exception("Pipeline failed")
            result = PipelineSummary(
                success=False,
                degraded=False,
                duration_seconds=time.monotonic() - started,
                error=str(exc),
            )
            return result
        finally:
            if run is not None:
                error = result.error if result else "Pipeline interrupted"
                self.services.database.finish_run(
                    run,
                    status="complete" if result and result.success else "failed",
                    error=error,
                    summary=self._run_summary(result),
                )

    @staticmethod
    def _run_summary(result: PipelineSummary | None) -> dict[str, object]:
        if result is None:
            return {}
        return {
            "success": result.success,
            "degraded": result.degraded,
            "duration_seconds": result.duration_seconds,
            "reviews": result.reviews,
            "failed_sources": (
                list(result.ingestion.failed_sources) if result.ingestion else []
            ),
            "extraction": (result.extraction.to_dict() if result.extraction else None),
            "enrichments": [stage.to_dict() for stage in result.enrichments],
            "export_errors": result.export.errors if result.export else 0,
        }

    def _incomplete_graph_stages(
        self,
        source_graphs: tuple[str, ...],
        enrichments: tuple[StageSummary, ...],
    ) -> dict[str, set[str]]:
        incomplete = {stage.name for stage in enrichments if not stage.complete}
        source_incomplete = incomplete & self.services.source_enrichments
        result = {graph: set(source_incomplete) for graph in source_graphs}
        for graph, required in self.services.graph_enrichments.items():
            result[graph] = incomplete & required
        return result

    def close(self) -> None:
        self.services.close()

    def __enter__(self) -> Pipeline:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
