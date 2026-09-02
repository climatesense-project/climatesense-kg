"""Construct concrete runtime services from pipeline configuration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .config import PipelineConfig
from .config.graphs import ENRICHMENT_GRAPH_ENTITY_SOURCES
from .config.organizations import ORGANIZATION_CATALOG_PATH, OrganizationCatalog
from .data_manager import DataManager
from .database import Database
from .enrichers import (
    CimpleModelEnricher,
    DBpediaPropertyEnricher,
    DBpediaSpotlightEnricher,
    Enricher,
)
from .enrichment import EnrichmentService
from .export import RdfExporter
from .extraction import DocumentExtractionService, RetryPolicy
from .identity import IdentityService
from .ingestion import IngestionService
from .projection import ReviewProjectionReader
from .rdf_generation import RDFGenerator


@dataclass
class PipelineServices:
    database: Database
    ingestion: IngestionService
    extraction: DocumentExtractionService | None
    identity: IdentityService
    enrichment: EnrichmentService
    exporter: RdfExporter

    def close(self) -> None:
        self.database.close()


def build_services(config: PipelineConfig) -> PipelineServices:
    database = Database.from_environment()
    organizations = OrganizationCatalog(ORGANIZATION_CATALOG_PATH)
    reader = ReviewProjectionReader(database.pool, organizations.resolve)
    extraction = _build_extraction(config, database)
    enrichers = _build_enrichers(config)
    enrichment = EnrichmentService(
        database.pool,
        reader,
        enrichers,
        batch_size=config.batch_size,
        progress_interval_seconds=config.enrichment.progress_interval_seconds,
    )
    enrichment_graphs = (
        dict(ENRICHMENT_GRAPH_ENTITY_SOURCES)
        if config.enrichment.dbpedia_spotlight.enabled
        else {}
    )
    return PipelineServices(
        database=database,
        ingestion=IngestionService(
            database.pool,
            DataManager(
                cache_dir=config.cache.cache_dir,
                default_ttl_hours=config.cache.default_ttl_hours,
            ),
            organizations,
            batch_size=config.batch_size,
            progress_interval_seconds=config.progress_interval_seconds,
        ),
        extraction=extraction,
        identity=IdentityService(
            database.pool,
            batch_size=config.batch_size,
            progress_interval_seconds=config.progress_interval_seconds,
        ),
        enrichment=enrichment,
        exporter=RdfExporter(
            reader,
            enrichment,
            RDFGenerator(base_uri=config.output.base_uri),
            output_path_template=str(config.output.output_path),
            enrichment_graphs=enrichment_graphs,
            batch_size=config.batch_size,
            progress_interval_seconds=config.progress_interval_seconds,
        ),
    )


def _build_extraction(
    config: PipelineConfig,
    database: Database,
) -> DocumentExtractionService | None:
    extraction = config.document_extraction
    if not extraction.enabled:
        return None
    return DocumentExtractionService(
        database.pool,
        batch_size=config.batch_size,
        max_workers=extraction.max_workers,
        rate_limit_delay=extraction.rate_limit_delay,
        timeout=extraction.timeout,
        max_retries=extraction.max_retries,
        retry_policy=RetryPolicy(
            transient_delay=timedelta(hours=extraction.transient_retry_delay_hours),
            blocked_delay=timedelta(hours=extraction.blocked_retry_delay_hours),
            dns_delay=timedelta(hours=extraction.dns_retry_delay_hours),
            content_delay=timedelta(hours=extraction.content_retry_delay_hours),
        ),
        progress_interval_seconds=extraction.progress_interval_seconds,
    )


def _build_enrichers(config: PipelineConfig) -> list[Enricher]:
    enrichers: list[Enricher] = []
    enrichment = config.enrichment
    if enrichment.dbpedia_spotlight.enabled:
        spotlight = enrichment.dbpedia_spotlight
        enrichers.extend(
            DBpediaSpotlightEnricher(
                target=target,
                api_url=spotlight.api_url,
                model_id=spotlight.model_id,
                confidence=spotlight.confidence,
                support=spotlight.support,
                timeout=spotlight.timeout,
                max_workers=spotlight.max_workers,
            )
            for target in ("claim", "review")
        )
    if enrichment.dbpedia_entity_properties.enabled:
        properties = enrichment.dbpedia_entity_properties
        enrichers.append(
            DBpediaPropertyEnricher(
                sparql_endpoint=properties.sparql_endpoint,
                properties=properties.properties,
                timeout=properties.timeout,
                rate_limit_delay=properties.rate_limit_delay,
                max_retries=properties.max_retries,
            )
        )
    if enrichment.cimple.enabled:
        cimple = enrichment.cimple
        enrichers.extend(
            CimpleModelEnricher(
                model=model,
                model_version=cimple.model_versions.get(model, "1"),
                batch_size=cimple.batch_size,
                max_length=cimple.max_length,
                timeout=cimple.timeout,
                rate_limit_delay=cimple.rate_limit_delay,
                max_workers=cimple.max_workers,
            )
            for model in CimpleModelEnricher.MODEL_KEYS
        )
    return enrichers
