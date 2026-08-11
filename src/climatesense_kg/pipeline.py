"""Typed ClimateSense knowledge-graph pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import logging
import time
from typing import TypedDict

from dotenv import load_dotenv

from .config import PipelineConfig
from .config.graphs import (
    DBPEDIA_ENRICHER_SOURCE_NAME,
    ENRICHMENT_GRAPH_ENTITY_SOURCES,
)
from .config.organizations import ORGANIZATION_CATALOG_PATH, OrganizationCatalog
from .data_manager import DataManager
from .deployment import ArtifactDeployer, plan_artifact_deployment
from .deployment.factory import create_deployment_handler
from .domain import CanonicalClaimReview, SourceReviewRecord
from .enrichers import (
    CimpleModelEnricher,
    DBpediaPropertyEnricher,
    DBpediaSpotlightEnricher,
)
from .identity import IdentityResolver
from .persistence import (
    PostgresDatabase,
    PostgresIdentityRegistry,
    PostgresStageResultStore,
)
from .rdf_generation import RdfArtifactBuilder, RdfBuildReport, RDFGenerator
from .stages import (
    DocumentExtractor,
    EnrichmentRunner,
    StageExecutionReport,
    StageExecutionSummary,
)
from .utils.logging import configure_external_loggers, setup_logging

logger = logging.getLogger(__name__)


class DataSourceResults(TypedDict):
    total_items: int
    sources_processed: int
    sources_failed: int
    successful_sources: list[str]
    failed_sources: list[str]


class IngestionResults(TypedDict):
    items: list[SourceReviewRecord]
    successful_sources: list[str]
    failed_sources: list[str]


class EnrichmentResults(TypedDict):
    input_items: int
    output_items: int
    complete: bool
    stages: list[StageExecutionSummary]


class GeneratedFileInfo(TypedDict):
    graph_name: str
    kind: str
    path: str
    items: int
    failed_items: int
    file_size: int
    review_uris: list[str]
    complete: bool
    incomplete_stages: list[str]


class RDFGenerationResults(TypedDict):
    generated_files: list[GeneratedFileInfo]
    total_files: int
    input_items: int
    successful_items: int
    failed_items: int
    output_format: str
    total_file_size: int
    error: str | None
    warnings: list[str]


class DeploymentResults(TypedDict):
    success: bool
    files_deployed: int
    total_files: int
    skipped_files: int
    skipped_graphs: list[str]


class PipelineResults(TypedDict):
    start_time: float
    end_time: float | None
    duration: float | None
    data_sources: DataSourceResults | None
    document_extraction: StageExecutionSummary | None
    enrichment: EnrichmentResults | None
    rdf_generation: RDFGenerationResults | None
    deployment: DeploymentResults | None
    total_processed: int
    success: bool
    degraded: bool
    error: str | None


@dataclass
class PipelineDependencies:
    """Explicit runtime services consumed by the orchestrator."""

    data_manager: DataManager
    organization_catalog: OrganizationCatalog
    document_extractor: DocumentExtractor | None
    identity_resolver: IdentityResolver
    enrichment_runner: EnrichmentRunner
    rdf_artifact_builder: RdfArtifactBuilder
    artifact_deployer: ArtifactDeployer
    database: PostgresDatabase | None = None
    source_graph_requirements: frozenset[str] = frozenset()
    enrichment_graph_requirements: dict[str, frozenset[str]] = field(
        default_factory=dict
    )

    def close(self) -> None:
        if self.database is not None:
            self.database.close()


def build_pipeline_dependencies(config: PipelineConfig) -> PipelineDependencies:
    """Construct runtime infrastructure outside orchestration control flow."""

    database = PostgresDatabase.from_environment()
    stage_store = PostgresStageResultStore(database.pool)
    document_extractor = None
    if config.document_extraction.enabled:
        document_extractor = DocumentExtractor(
            stage_store,
            rate_limit_delay=config.document_extraction.rate_limit_delay,
            timeout=config.document_extraction.timeout,
            max_retries=config.document_extraction.max_retries,
            checkpoint_size=config.document_extraction.checkpoint_size,
            progress_interval_seconds=(
                config.document_extraction.progress_interval_seconds
            ),
        )

    enrichers = []
    source_graph_requirements: set[str] = set()
    dbpedia_graph_requirements: set[str] = set()
    enrichment = config.enrichment
    if enrichment.dbpedia_spotlight.enabled:
        spotlight = enrichment.dbpedia_spotlight
        spotlight_enrichers = [
            DBpediaSpotlightEnricher(
                target=target,
                store=stage_store,
                api_url=spotlight.api_url,
                model_id=spotlight.model_id,
                confidence=spotlight.confidence,
                support=spotlight.support,
                timeout=spotlight.timeout,
                rate_limit_delay=spotlight.rate_limit_delay,
            )
            for target in ("claim", "review")
        ]
        enrichers.extend(spotlight_enrichers)
        dbpedia_graph_requirements.update(
            stage.stage_name for stage in spotlight_enrichers
        )
    if enrichment.dbpedia_entity_properties.enabled:
        properties = enrichment.dbpedia_entity_properties
        property_enricher = DBpediaPropertyEnricher(
            store=stage_store,
            sparql_endpoint=properties.sparql_endpoint,
            properties=properties.properties,
            timeout=properties.timeout,
            rate_limit_delay=properties.rate_limit_delay,
            max_retries=properties.max_retries,
        )
        enrichers.append(property_enricher)
        dbpedia_graph_requirements.add(property_enricher.stage_name)
    if enrichment.cimple.enabled:
        cimple = enrichment.cimple
        cimple_enrichers = [
            CimpleModelEnricher(
                model=model,
                store=stage_store,
                model_version=cimple.model_versions.get(model, "1"),
                batch_size=cimple.batch_size,
                max_length=cimple.max_length,
                timeout=cimple.timeout,
                rate_limit_delay=cimple.rate_limit_delay,
            )
            for model in CimpleModelEnricher.MODEL_KEYS
        ]
        enrichers.extend(cimple_enrichers)
        source_graph_requirements.update(stage.stage_name for stage in cimple_enrichers)

    rdf_generator = RDFGenerator(base_uri=config.output.base_uri)
    return PipelineDependencies(
        data_manager=DataManager(
            cache_dir=config.cache.cache_dir,
            default_ttl_hours=config.cache.default_ttl_hours,
        ),
        organization_catalog=OrganizationCatalog(ORGANIZATION_CATALOG_PATH),
        document_extractor=document_extractor,
        identity_resolver=IdentityResolver(PostgresIdentityRegistry(database.pool)),
        enrichment_runner=EnrichmentRunner(enrichers),
        rdf_artifact_builder=RdfArtifactBuilder(
            rdf_generator,
            output_path_template=str(config.output.output_path),
            output_format=config.output.format,
            enrichment_graphs=(
                dict(ENRICHMENT_GRAPH_ENTITY_SOURCES)
                if config.enrichment.dbpedia_spotlight.enabled
                else {}
            ),
        ),
        artifact_deployer=ArtifactDeployer(
            create_deployment_handler(config.deployment)
        ),
        database=database,
        source_graph_requirements=frozenset(source_graph_requirements),
        enrichment_graph_requirements=(
            {DBPEDIA_ENRICHER_SOURCE_NAME: frozenset(dbpedia_graph_requirements)}
            if config.enrichment.dbpedia_spotlight.enabled
            else {}
        ),
    )


class Pipeline:
    """Coordinate typed stages without implementing their domain behavior."""

    def __init__(
        self,
        config: PipelineConfig,
        dependencies: PipelineDependencies | None = None,
    ) -> None:
        load_dotenv()
        self.config = config
        setup_logging(config.logging)
        configure_external_loggers()
        self.logger = logging.getLogger(__name__)
        self.dependencies = dependencies or build_pipeline_dependencies(config)
        self._run_datetime: datetime | None = None

    def close(self) -> None:
        self.dependencies.close()

    def __enter__(self) -> Pipeline:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def run(
        self,
        skip_download: bool = False,
        force_regenerate: bool = False,
        skip_extraction: bool = False,
        skip_enrichment: bool = False,
        skip_deployment: bool = False,
    ) -> PipelineResults:
        """Execute ingestion, identity, enrichment, RDF, and deployment."""

        started = time.time()
        self._run_datetime = datetime.now()
        results = self._empty_results(started)
        try:
            ingestion = self._run_ingestion(skip_download=skip_download)
            records = ingestion["items"]
            results["data_sources"] = {
                "total_items": len(records),
                "sources_processed": len(ingestion["successful_sources"]),
                "sources_failed": len(ingestion["failed_sources"]),
                "successful_sources": ingestion["successful_sources"],
                "failed_sources": ingestion["failed_sources"],
            }
            if ingestion["failed_sources"] and not ingestion["successful_sources"]:
                raise RuntimeError(
                    "All enabled data sources failed ingestion: "
                    + ", ".join(ingestion["failed_sources"])
                )
            reviews, extraction_report = self._resolve_records(
                records,
                force=force_regenerate,
                skip_extraction=skip_extraction,
            )
            if extraction_report is not None:
                results["document_extraction"] = extraction_report.to_dict()
            results["data_sources"]["total_items"] = len(reviews)

            enrichment_report = self.dependencies.enrichment_runner.run(
                reviews,
                stored_only=skip_enrichment,
                force=force_regenerate,
            )
            results["enrichment"] = {
                "input_items": len(reviews),
                "output_items": len(enrichment_report.items),
                "complete": enrichment_report.complete,
                "stages": [stage.to_dict() for stage in enrichment_report.stages],
            }
            results["total_processed"] = len(enrichment_report.items)
            results["degraded"] = (
                bool(ingestion["failed_sources"])
                or (extraction_report is not None and not extraction_report.complete)
                or not enrichment_report.complete
            )

            incomplete_by_graph = {
                graph_name: requirements & enrichment_report.incomplete_stage_names
                for graph_name, requirements in (
                    self.dependencies.enrichment_graph_requirements.items()
                )
            }
            source_incomplete = (
                self.dependencies.source_graph_requirements
                & enrichment_report.incomplete_stage_names
            )
            incomplete_by_graph.update(
                {
                    source_name: set(source_incomplete)
                    for source_name in ingestion["successful_sources"]
                }
            )

            build_report = self.dependencies.rdf_artifact_builder.build(
                enrichment_report.items,
                successful_sources=ingestion["successful_sources"],
                run_datetime=self._run_datetime,
                incomplete_stages_by_graph=incomplete_by_graph,
            )
            rdf_results = self._rdf_results(build_report)
            results["rdf_generation"] = rdf_results
            if rdf_results["error"]:
                raise RuntimeError(rdf_results["error"])

            deployment_plan = plan_artifact_deployment(build_report.artifacts)
            if skip_deployment:
                deployment = {
                    "success": True,
                    "files_deployed": 0,
                    "total_files": deployment_plan.total_files,
                    "skipped_files": deployment_plan.skipped_files,
                    "skipped_graphs": list(deployment_plan.skipped_graphs),
                }
            else:
                deployment_report = self.dependencies.artifact_deployer.deploy(
                    build_report.artifacts
                )
                deployment = {
                    "success": deployment_report.success,
                    "files_deployed": deployment_report.files_deployed,
                    "total_files": deployment_report.total_files,
                    "skipped_files": deployment_report.skipped_files,
                    "skipped_graphs": list(deployment_report.skipped_graphs),
                }
            results["deployment"] = deployment
            if build_report.enrichment_errors or deployment["skipped_files"]:
                results["degraded"] = True
            if not deployment["success"]:
                raise RuntimeError("One or more RDF graphs failed deployment")

            results["success"] = True
        except Exception as exc:
            self.logger.error("Pipeline failed: %s", exc)
            results["error"] = str(exc)
        finally:
            ended = time.time()
            results["end_time"] = ended
            results["duration"] = ended - started
            self._run_datetime = None
        return results

    @staticmethod
    def _empty_results(started: float) -> PipelineResults:
        return {
            "start_time": started,
            "end_time": None,
            "duration": None,
            "data_sources": None,
            "document_extraction": None,
            "enrichment": None,
            "rdf_generation": None,
            "deployment": None,
            "total_processed": 0,
            "success": False,
            "degraded": False,
            "error": None,
        }

    def _run_ingestion(self, *, skip_download: bool) -> IngestionResults:
        records: list[SourceReviewRecord] = []
        successful_sources: list[str] = []
        failed_sources: list[str] = []
        for source in self.config.data_sources:
            if not source.enabled:
                continue
            try:
                source_records = list(
                    self.dependencies.data_manager.get_data(
                        source, skip_download=skip_download
                    )
                )
                records.extend(source_records)
                successful_sources.append(source.name)
            except Exception as exc:
                self.logger.error("Ingestion failed for %s: %s", source.name, exc)
                failed_sources.append(source.name)
        return {
            "items": records,
            "successful_sources": successful_sources,
            "failed_sources": failed_sources,
        }

    def _resolve_records(
        self,
        records: list[SourceReviewRecord],
        *,
        force: bool,
        skip_extraction: bool,
    ) -> tuple[list[CanonicalClaimReview], StageExecutionReport | None]:
        resolvable = []
        unresolved: set[tuple[str, str, str]] = set()
        for record in records:
            organization = self.dependencies.organization_catalog.resolve(
                record.organization
            )
            if organization is None:
                unresolved.add(
                    (
                        record.source.source_name,
                        record.organization.name,
                        record.organization.website,
                    )
                )
                continue
            resolvable.append((record, organization))
        if unresolved:
            details = "; ".join(
                f"{source}: {name!r} ({website})"
                for source, name, website in sorted(unresolved)
            )
            raise RuntimeError(
                "Organizations are missing from the curated catalog: " + details
            )
        extraction_report = None
        if self.dependencies.document_extractor is not None:
            extraction_report = self.dependencies.document_extractor.extract_many(
                [record for record, _organization in resolvable],
                force=force,
                stored_only=skip_extraction,
            )
        return (
            self.dependencies.identity_resolver.resolve_many(resolvable),
            extraction_report,
        )

    @staticmethod
    def _rdf_results(report: RdfBuildReport) -> RDFGenerationResults:
        generated_files: list[GeneratedFileInfo] = [
            {
                "graph_name": artifact.graph_name,
                "kind": artifact.kind,
                "path": str(artifact.path),
                "items": artifact.items,
                "failed_items": artifact.failed_items,
                "file_size": artifact.file_size,
                "review_uris": artifact.review_uris,
                "complete": artifact.complete,
                "incomplete_stages": list(artifact.incomplete_stages),
            }
            for artifact in report.artifacts
        ]
        return {
            "generated_files": generated_files,
            "total_files": len(generated_files),
            "input_items": report.input_items,
            "successful_items": report.successful_items,
            "failed_items": report.failed_items,
            "output_format": report.output_format,
            "total_file_size": report.total_file_size,
            "error": "; ".join(report.source_errors) or None,
            "warnings": report.enrichment_errors,
        }

    def clear_cache(self, source_name: str | None = None) -> None:
        """Clear only downloaded source artifacts."""

        self.dependencies.data_manager.clear_cache(source_name)
