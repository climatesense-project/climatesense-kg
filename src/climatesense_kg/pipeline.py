"""Typed ClimateSense knowledge-graph pipeline."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
import logging
import time
from typing import TypedDict
from uuid import UUID

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
from .domain import CanonicalClaimReview, CanonicalOrganization, SourceReviewRecord
from .enrichers import (
    CimpleModelEnricher,
    DBpediaPropertyEnricher,
    DBpediaSpotlightEnricher,
)
from .identity import IdentityResolver
from .persistence import (
    InMemoryObservationStore,
    ObservationStore,
    PostgresDatabase,
    PostgresIdentityRegistry,
    PostgresObservationStore,
    PostgresPublicationReader,
    PostgresStageResultStore,
    PublicationReader,
    stable_hash,
)
from .rdf_generation import RdfArtifactBuilder, RdfBuildReport, RDFGenerator
from .stages import (
    DocumentExtractor,
    DocumentRetryPolicy,
    EnrichmentRunner,
    StageExecutionReport,
    StageExecutionSummary,
)
from .utils.logging import configure_external_loggers, setup_logging
from .utils.memory import format_process_rss
from .utils.progress import format_duration

logger = logging.getLogger(__name__)


class DataSourceResults(TypedDict):
    total_items: int
    sources_processed: int
    sources_failed: int
    successful_sources: list[str]
    failed_sources: list[str]


class IngestionResults(TypedDict):
    run_id: UUID
    total_items: int
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
    observation_store: ObservationStore = field(
        default_factory=InMemoryObservationStore
    )
    publication_reader: PublicationReader | None = None
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
            max_workers=config.document_extraction.max_workers,
            rate_limit_delay=config.document_extraction.rate_limit_delay,
            timeout=config.document_extraction.timeout,
            max_retries=config.document_extraction.max_retries,
            retry_policy=DocumentRetryPolicy(
                transient_delay=timedelta(
                    hours=config.document_extraction.transient_retry_delay_hours
                ),
                blocked_delay=timedelta(
                    hours=config.document_extraction.blocked_retry_delay_hours
                ),
                dns_delay=timedelta(
                    hours=config.document_extraction.dns_retry_delay_hours
                ),
                content_delay=timedelta(
                    hours=config.document_extraction.content_retry_delay_hours
                ),
            ),
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
                checkpoint_size=enrichment.checkpoint_size,
                progress_interval_seconds=(enrichment.progress_interval_seconds),
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
            checkpoint_size=enrichment.checkpoint_size,
            progress_interval_seconds=enrichment.progress_interval_seconds,
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
                checkpoint_size=enrichment.checkpoint_size,
                progress_interval_seconds=enrichment.progress_interval_seconds,
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
        identity_resolver=IdentityResolver(
            PostgresIdentityRegistry(database.pool),
            batch_size=config.identity_resolution.batch_size,
            progress_interval_seconds=(
                config.identity_resolution.progress_interval_seconds
            ),
        ),
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
        observation_store=PostgresObservationStore(database.pool),
        publication_reader=PostgresPublicationReader(database.pool),
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
        observation_run = None
        rdf_session = None
        try:
            signature = stable_hash(asdict(self.config))
            observation_run = self.dependencies.observation_store.start_run(signature)
            ingestion = self._run_ingestion(
                observation_run.id,
                skip_download=skip_download,
            )
            results["data_sources"] = {
                "total_items": ingestion["total_items"],
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
            extraction_report = self._extract_observations(
                observation_run.id,
                force=force_regenerate,
                skip_extraction=skip_extraction,
            )
            if extraction_report is not None:
                results["document_extraction"] = extraction_report.to_dict()
            fallback_reviews = self._resolve_observations(observation_run.id)

            self.dependencies.enrichment_runner.start_run()
            rdf_session = self.dependencies.rdf_artifact_builder.start(
                successful_sources=ingestion["successful_sources"],
                run_datetime=self._run_datetime,
            )
            reports_by_stage: dict[str, StageExecutionReport] = {}
            processed = 0
            publication_total = self._publication_count(
                observation_run.id,
                fallback_reviews=fallback_reviews,
            )
            publication_started = time.monotonic()
            publication_last_logged = publication_started
            for reviews in self._publication_batches(
                observation_run.id,
                fallback_reviews=fallback_reviews,
            ):
                enrichment_report = self.dependencies.enrichment_runner.run(
                    reviews,
                    stored_only=skip_enrichment,
                    force=force_regenerate,
                    report_progress=False,
                )
                for report in enrichment_report.stages:
                    previous = reports_by_stage.get(report.stage_name)
                    reports_by_stage[report.stage_name] = (
                        report
                        if previous is None
                        else StageExecutionReport.combine([previous, report])
                    )
                rdf_session.add(enrichment_report.items)
                processed += len(enrichment_report.items)
                now = time.monotonic()
                if (
                    processed == publication_total
                    or now - publication_last_logged >= 10
                ):
                    elapsed = now - publication_started
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta = (publication_total - processed) / rate if rate > 0 else None
                    self.logger.info(
                        "Enrichment and RDF projection: %d/%d reviews (%.1f%%); "
                        "rate=%.2f/s; ETA=%s; RSS=%s",
                        processed,
                        publication_total,
                        (
                            100.0
                            if not publication_total
                            else 100 * processed / publication_total
                        ),
                        rate,
                        format_duration(eta),
                        format_process_rss(),
                    )
                    publication_last_logged = now

            combined_enrichment = list(reports_by_stage.values())
            for report in combined_enrichment:
                self.logger.info(
                    "Enrichment finished: %s; eligible=%d, restored=%d, "
                    "computed=%d, failed=%d, missing=%d",
                    report.stage_name,
                    report.eligible_subjects,
                    report.stored_successes,
                    report.computed_successes,
                    report.computed_failures,
                    report.missing_results,
                )
            results["enrichment"] = {
                "input_items": processed,
                "output_items": processed,
                "complete": all(report.complete for report in combined_enrichment),
                "stages": [stage.to_dict() for stage in combined_enrichment],
            }
            results["total_processed"] = processed
            results["data_sources"]["total_items"] = processed
            incomplete_stage_names = {
                report.stage_name
                for report in combined_enrichment
                if not report.complete
            }
            results["degraded"] = (
                bool(ingestion["failed_sources"])
                or (extraction_report is not None and not extraction_report.healthy)
                or bool(incomplete_stage_names)
            )

            incomplete_by_graph = {
                graph_name: requirements & incomplete_stage_names
                for graph_name, requirements in self.dependencies.enrichment_graph_requirements.items()
            }
            source_incomplete = (
                self.dependencies.source_graph_requirements & incomplete_stage_names
            )
            incomplete_by_graph.update(
                {
                    source_name: set(source_incomplete)
                    for source_name in ingestion["successful_sources"]
                }
            )

            build_report = rdf_session.finish(
                incomplete_stages_by_graph=incomplete_by_graph,
            )
            rdf_session = None
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
            if rdf_session is not None:
                rdf_session.abort()
            if observation_run is not None:
                try:
                    self.dependencies.observation_store.finish_run(
                        observation_run.id,
                        status="complete" if results["success"] else "failed",
                        error=results["error"],
                    )
                except Exception as exc:
                    self.logger.error("Failed to finalize pipeline run: %s", exc)
                    if results["success"]:
                        results["success"] = False
                        results["error"] = f"Failed to finalize pipeline run: {exc}"
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

    def _run_ingestion(self, run_id: UUID, *, skip_download: bool) -> IngestionResults:
        total_items = 0
        successful_sources: list[str] = []
        failed_sources: list[str] = []
        for source in self.config.data_sources:
            if not source.enabled:
                continue
            try:
                source_items = self.dependencies.observation_store.ingest_source(
                    run_id,
                    source.name,
                    self.dependencies.data_manager.get_data(
                        source,
                        skip_download=skip_download,
                    ),
                    batch_size=self.config.identity_resolution.batch_size,
                )
                total_items += source_items
                successful_sources.append(source.name)
            except Exception as exc:
                self.logger.error("Ingestion failed for %s: %s", source.name, exc)
                failed_sources.append(source.name)
        return {
            "run_id": run_id,
            "total_items": total_items,
            "successful_sources": successful_sources,
            "failed_sources": failed_sources,
        }

    def _extract_observations(
        self,
        run_id: UUID,
        *,
        force: bool,
        skip_extraction: bool,
    ) -> StageExecutionReport | None:
        extractor = self.dependencies.document_extractor
        if extractor is None:
            return None
        extractor.start_run()
        combined: StageExecutionReport | None = None
        total = self.dependencies.observation_store.count(run_id)
        processed = 0
        started = time.monotonic()
        last_logged = started
        for batch in self.dependencies.observation_store.iter_batches(
            run_id,
            batch_size=self.config.identity_resolution.batch_size,
            order_by_url=True,
        ):
            report = extractor.extract_many(
                batch,
                force=force,
                stored_only=skip_extraction,
                report_progress=False,
            )
            combined = (
                report
                if combined is None
                else StageExecutionReport.combine([combined, report])
            )
            processed += len(batch)
            now = time.monotonic()
            if processed == total or now - last_logged >= 10:
                elapsed = now - started
                rate = processed / elapsed if elapsed > 0 else 0
                eta = (total - processed) / rate if rate > 0 else None
                self.logger.info(
                    "Document extraction pass: %d/%d observations (%.1f%%); "
                    "unique=%d, restored=%d, fetched=%d, failed=%d; "
                    "rate=%.2f/s; ETA=%s; RSS=%s",
                    processed,
                    total,
                    100.0 if not total else 100 * processed / total,
                    combined.eligible_subjects,
                    combined.stored_successes,
                    combined.computed_successes,
                    combined.computed_failures,
                    rate,
                    format_duration(eta),
                    format_process_rss(),
                )
                last_logged = now
        if combined is None:
            combined = StageExecutionReport.combine([], stage_name=extractor.name)
        self.logger.info(
            "Document extraction pass finished: eligible=%d, restored=%d, "
            "fetched=%d, failed=%d; RSS=%s",
            combined.eligible_subjects,
            combined.stored_successes,
            combined.computed_successes,
            combined.computed_failures,
            format_process_rss(),
        )
        return combined

    def _resolve_observations(
        self,
        run_id: UUID,
    ) -> dict[str, CanonicalClaimReview] | None:
        fallback = {} if self.dependencies.publication_reader is None else None
        total = self.dependencies.observation_store.count(run_id)
        committed = 0
        started = time.monotonic()
        last_logged = started
        for records in self.dependencies.observation_store.iter_batches(
            run_id,
            batch_size=self.config.identity_resolution.batch_size,
        ):
            if self.dependencies.document_extractor is not None:
                self.dependencies.document_extractor.extract_many(
                    records,
                    stored_only=True,
                    report_progress=False,
                )
            reviews = self.dependencies.identity_resolver.resolve_many(
                self._resolvable_records(records),
                report_progress=False,
            )
            if fallback is not None:
                for review in reviews:
                    existing = fallback.get(review.key)
                    if existing is None:
                        fallback[review.key] = review
                    else:
                        IdentityResolver._merge(existing, review)
            committed += len(records)
            now = time.monotonic()
            if committed == total or now - last_logged >= 10:
                elapsed = now - started
                rate = committed / elapsed if elapsed > 0 else 0
                remaining = max(0, total - committed)
                eta = remaining / rate if rate > 0 else None
                self.logger.info(
                    "Identity resolution: %d/%d committed (%.1f%%); "
                    "rate=%.2f/s; ETA=%s; RSS=%s",
                    committed,
                    total,
                    100.0 if not total else 100 * committed / total,
                    rate,
                    format_duration(eta),
                    format_process_rss(),
                )
                last_logged = now
        return fallback

    def _resolvable_records(
        self, records: list[SourceReviewRecord]
    ) -> list[tuple[SourceReviewRecord, CanonicalOrganization]]:
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
        return resolvable

    def _publication_batches(
        self,
        run_id: UUID,
        *,
        fallback_reviews: dict[str, CanonicalClaimReview] | None,
    ) -> Iterator[list[CanonicalClaimReview]]:
        reader = self.dependencies.publication_reader
        if reader is not None:
            yield from reader.iter_batches(
                run_id,
                batch_size=self.config.identity_resolution.batch_size,
                resolve_organization=self.dependencies.organization_catalog.resolve,
            )
            return
        reviews = list((fallback_reviews or {}).values())
        batch_size = self.config.identity_resolution.batch_size
        for start in range(0, len(reviews), batch_size):
            yield reviews[start : start + batch_size]

    def _publication_count(
        self,
        run_id: UUID,
        *,
        fallback_reviews: dict[str, CanonicalClaimReview] | None,
    ) -> int:
        reader = self.dependencies.publication_reader
        if reader is not None:
            return reader.count(run_id)
        return len(fallback_reviews or {})

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
