"""ClimateSense Knowledge Graph Pipeline."""

from datetime import datetime
import logging
import os
from pathlib import Path
import time
from typing import TypedDict

from dotenv import load_dotenv

from .cache.interface import CacheInterface
from .cache.postgres_cache import PostgresCache
from .config import PipelineConfig
from .config.graphs import (
    DBPEDIA_ENRICHER_SOURCE_NAME,
    DBPEDIA_ENTITY_SOURCES,
    GRAPH_CATALOG_PATH,
    GRAPH_CATALOG_SOURCE_NAME,
)
from .config.models import CanonicalClaimReview
from .config.organizations import (
    ORGANIZATION_CATALOG_PATH,
    ORGANIZATION_SOURCE_NAME,
    OrganizationCatalog,
)
from .data_manager import DataManager
from .deployment.base import DeploymentHandler
from .deployment.factory import create_deployment_handler
from .enrichers.base import Enricher as BaseEnricher
from .enrichers.bert_factors_enricher import BertFactorsEnricher
from .enrichers.dbpedia_enricher import DBpediaEnricher
from .enrichers.dbpedia_property_enricher import DBpediaPropertyEnricher
from .enrichers.url_text_enricher import URLTextEnricher
from .rdf_generation.generator import RDFGenerator
from .utils.logging import configure_external_loggers, setup_logging

logger = logging.getLogger(__name__)


class DataSourceResults(TypedDict):
    total_items: int
    sources_processed: int
    sources_failed: int
    successful_sources: list[str]
    failed_sources: list[str]


class IngestionResults(TypedDict):
    items: list[CanonicalClaimReview]
    successful_sources: list[str]
    failed_sources: list[str]


class EnrichmentResults(TypedDict):
    input_items: int
    output_items: int


class GeneratedFileInfo(TypedDict):
    graph_name: str
    kind: str
    path: str
    items: int
    failed_items: int
    file_size: int
    review_uris: list[str]


class ReviewOutputInfo(TypedDict):
    source_name: str
    source_path: str
    required_graphs: list[str]


class RDFGenerationResults(TypedDict):
    generated_files: list[GeneratedFileInfo]
    total_files: int
    input_items: int
    successful_items: int
    failed_items: int
    output_format: str
    total_file_size: int
    error: str | None
    review_outputs: dict[str, ReviewOutputInfo]


class DeploymentResults(TypedDict):
    success: bool
    files_deployed: int
    total_files: int


class PipelineResults(TypedDict):
    start_time: float
    end_time: float | None
    duration: float | None
    data_sources: DataSourceResults | None
    enrichment: EnrichmentResults | None
    rdf_generation: RDFGenerationResults | None
    deployment: DeploymentResults | None
    total_processed: int
    success: bool
    error: str | None


class Pipeline:
    """Main pipeline orchestrator."""

    def _get_fully_processed_uris(self, uris: list[str]) -> set[str]:
        """Return URIs that completed pipeline output and all enricher steps."""
        if not self.cache or not uris:
            return set()

        pipeline_cached = self.cache.get_many(uris, "pipeline.processed_items")
        processed_uris = set(pipeline_cached.keys())

        if not processed_uris:
            return set()

        for enricher in self.enrichers:
            if not enricher.is_available():
                continue
            step_names = enricher.required_cache_steps()
            if not step_names:
                continue

            for step_name in step_names:
                enricher_cached = self.cache.get_many(list(processed_uris), step_name)
                processed_uris &= set(enricher_cached.keys())
                if not processed_uris:
                    break
            if not processed_uris:
                break

        return processed_uris

    def __init__(self, config: PipelineConfig):
        load_dotenv()

        self.config = config

        # Setup logging
        setup_logging(config.logging)
        configure_external_loggers()
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.data_manager = DataManager(
            cache_dir=config.cache.cache_dir,
            default_ttl_hours=config.cache.default_ttl_hours,
        )
        self.enrichers: list[BaseEnricher] = []
        self.rdf_generator: RDFGenerator | None = None
        self.deployment_handler: DeploymentHandler | None = None
        self.cache: CacheInterface | None = None
        self._initialize_components()
        self._run_datetime: datetime | None = None

        self.organization_catalog = OrganizationCatalog(ORGANIZATION_CATALOG_PATH)

    def _initialize_components(self) -> None:
        """Initialize pipeline components from configuration."""

        # Initialize URI cache
        try:
            self.cache = PostgresCache(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "climatesense_cache"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD"),
            )
        except Exception as e:
            self.logger.warning(f"Failed to initialize URI cache: {e}")
            self.cache = None

        # Initialize enricher
        try:
            enrichers: list[BaseEnricher] = []
            config = self.config.enrichment

            # URL text enricher
            if config.url_text_extraction.enabled:
                url_enricher = URLTextEnricher(
                    cache=self.cache, **vars(config.url_text_extraction)
                )
                if url_enricher.is_available():
                    enrichers.append(url_enricher)

            # DBpedia enricher
            if config.dbpedia_spotlight.enabled:
                dbpedia_enricher = DBpediaEnricher(
                    cache=self.cache, **vars(config.dbpedia_spotlight)
                )
                if dbpedia_enricher.is_available():
                    enrichers.append(dbpedia_enricher)

            # DBpedia entity properties enricher
            if config.dbpedia_entity_properties.enabled:
                dbpedia_property_enricher = DBpediaPropertyEnricher(
                    cache=self.cache, **vars(config.dbpedia_entity_properties)
                )
                if dbpedia_property_enricher.is_available():
                    enrichers.append(dbpedia_property_enricher)

            # BERT factors enricher
            if config.bert_factors.enabled:
                bert_enricher = BertFactorsEnricher(
                    cache=self.cache, **vars(config.bert_factors)
                )
                if bert_enricher.is_available():
                    enrichers.append(bert_enricher)

            self.enrichers = enrichers

            self.logger.info("Initialized enricher")
        except Exception as e:
            self.logger.error(f"Failed to initialize enricher: {e}")
            raise

        # Initialize RDF generator
        try:
            output_config = self.config.output
            self.rdf_generator = RDFGenerator(
                base_uri=output_config.base_uri,
                format=output_config.format,
            )
            self.logger.info("Initialized RDF generator")
        except Exception as e:
            self.logger.error(f"Failed to initialize RDF generator: {e}")
            raise

        # Initialize deployment handler
        try:
            self.deployment_handler = create_deployment_handler(self.config.deployment)
            if self.deployment_handler:
                self.logger.info(
                    "Initialized %s deployment handler",
                    self.config.deployment.backend,
                )
        except Exception as e:
            self.logger.error(f"Failed to initialize deployment handler: {e}")
            raise

    def _mark_uris_processed(
        self, uri_source_rdf_tuples: list[tuple[str, str, str]]
    ) -> None:
        """Mark multiple URIs as successfully processed."""
        if not self.cache or not uri_source_rdf_tuples:
            return

        step_name = "pipeline.processed_items"
        processed_at = datetime.now().isoformat()

        batch_data = [
            (
                uri,
                step_name,
                {
                    "source": source_name,
                    "processed_at": processed_at,
                    "rdf_file_path": rdf_file_path,
                },
            )
            for uri, source_name, rdf_file_path in uri_source_rdf_tuples
        ]

        self.cache.set_many(batch_data)

    def run(
        self,
        skip_download: bool = False,
        force_regenerate: bool = False,
        skip_enrichment: bool = False,
        skip_deployment: bool = False,
    ) -> PipelineResults:
        """Execute the complete pipeline.

        Args:
            skip_download: Skip data downloads and use only cached/already downloaded data
            skip_enrichment: Apply cached enrichment data if present without running enrichers
            skip_deployment: Skip deployment step even if a handler is configured

        Returns:
            Pipeline execution results and statistics
        """
        start_time = time.time()
        self._run_datetime = datetime.now()
        self.logger.info("Starting ClimateSense KG Pipeline")

        results: PipelineResults = {
            "start_time": start_time,
            "end_time": None,
            "duration": None,
            "data_sources": None,
            "enrichment": None,
            "rdf_generation": None,
            "deployment": None,
            "total_processed": 0,
            "success": False,
            "error": None,
        }

        try:
            # Step 1: Data Ingestion
            self.logger.info("Step 1: Data Ingestion")
            ingestion_results = self._run_ingestion(
                skip_download=skip_download, force_regenerate=force_regenerate
            )
            canonical_reviews = ingestion_results["items"]
            successful_sources = ingestion_results["successful_sources"]
            failed_sources = ingestion_results["failed_sources"]
            results["data_sources"] = {
                "total_items": len(canonical_reviews),
                "sources_processed": len(successful_sources),
                "sources_failed": len(failed_sources),
                "successful_sources": successful_sources,
                "failed_sources": failed_sources,
            }

            if failed_sources and not successful_sources:
                raise RuntimeError(
                    "All enabled data sources failed ingestion: "
                    + ", ".join(failed_sources)
                )

            if not canonical_reviews:
                self.logger.info("No new items to process.")

            unresolved_organizations: set[tuple[str, str, str]] = set()
            for review in canonical_reviews:
                resolved_uri = self.organization_catalog.resolve(review.organization)
                if not resolved_uri:
                    unresolved_organizations.add(
                        (
                            review.source_name or "unknown",
                            review.organization.name,
                            review.organization.website,
                        )
                    )

            if unresolved_organizations:
                details = "; ".join(
                    f"{source}: {name!r} ({website})"
                    for source, name, website in sorted(unresolved_organizations)
                )
                raise RuntimeError(
                    "Organizations are missing from the curated catalog: " + details
                )

            # Step 2: Enrichment
            self.logger.info("Step 2: Data Enrichment")
            enriched_reviews = self._run_enrichment(
                canonical_reviews, skip_enrichment=skip_enrichment
            )
            results["enrichment"] = {
                "input_items": len(canonical_reviews),
                "output_items": len(enriched_reviews),
            }

            # Step 3: RDF Generation
            self.logger.info("Step 3: RDF Generation")
            rdf_stats = self._run_rdf_generation(
                enriched_reviews,
                mark_processed=not skip_deployment and not self.deployment_handler,
            )
            results["rdf_generation"] = rdf_stats

            # Step 4: Deployment
            deployment_success = True
            generated_files = rdf_stats.get("generated_files", [])
            total_files = len(generated_files) + 2

            if skip_deployment:
                self.logger.info("Step 4: Deployment skipped (--skip-deployment)")
                deployment_stats: DeploymentResults = {
                    "success": True,
                    "files_deployed": 0,
                    "total_files": total_files,
                }
            elif self.deployment_handler:
                self.logger.info("Step 4: Deploying RDF data")
                deployment_stats = self._run_deployment(rdf_stats)
                deployment_success = deployment_stats["success"]
            else:
                self.logger.info("Step 4: No deployment handler configured, skipping")
                deployment_stats = {
                    "success": True,
                    "files_deployed": 0,
                    "total_files": total_files,
                }

            results["deployment"] = deployment_stats

            # Final statistics
            rdf_success = rdf_stats["error"] is None
            results["total_processed"] = len(enriched_reviews)
            results["success"] = rdf_success and deployment_success
            if not rdf_success:
                results["error"] = rdf_stats["error"]

            end_time = time.time()
            results["end_time"] = end_time
            results["duration"] = end_time - start_time

            self._run_datetime = None
            if results["success"]:
                self.logger.info(
                    f"Pipeline completed successfully in {results['duration']:.2f} seconds"
                )
            else:
                self.logger.error(
                    f"Pipeline completed with errors in {results['duration']:.2f} seconds"
                )
            self.logger.info(f"Processed {results['total_processed']} claim reviews")

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            results["error"] = str(e)
            end_time = time.time()
            results["end_time"] = end_time
            results["duration"] = end_time - start_time

        self._run_datetime = None
        return results

    def _run_ingestion(
        self, skip_download: bool = False, force_regenerate: bool = False
    ) -> IngestionResults:
        """Run data ingestion using DataManager.

        Args:
            skip_download: Skip data downloads and use only cached data
        """
        all_items: list[CanonicalClaimReview] = []
        total_items_before_filtering = 0
        successful_sources: list[str] = []
        failed_sources: list[str] = []

        for source_config in self.config.data_sources:
            if not source_config.enabled:
                continue

            try:
                items = list(
                    self.data_manager.get_data(
                        source_config, skip_download=skip_download
                    )
                )
                total_items_before_filtering += len(items)

                # Filter out already-processed items
                if self.cache and not force_regenerate:
                    self.logger.info(
                        f"Filtering already processed items using cache for {source_config.name}..."
                    )
                    uris = [item.uri for item in items if item.uri]
                    fully_processed_uris = self._get_fully_processed_uris(uris)

                    new_items = [
                        item
                        for item in items
                        if not item.uri or item.uri not in fully_processed_uris
                    ]
                    skipped_count = len(items) - len(new_items)

                    self.logger.info(
                        f"Ingested {len(items)} items from {source_config.name}: "
                        f"{len(new_items)} to process, {skipped_count} already processed"
                    )
                    all_items.extend(new_items)
                else:
                    if force_regenerate:
                        self.logger.info(
                            f"Force regenerating all items from {source_config.name} (cache filtering disabled)"
                        )
                    self.logger.info(
                        f"Ingested {len(items)} items from {source_config.name} (no cache filtering)"
                    )
                    all_items.extend(items)

                successful_sources.append(source_config.name)

            except Exception as e:
                self.logger.error(f"Error ingesting from {source_config.name}: {e}")
                failed_sources.append(source_config.name)

        skipped_total = total_items_before_filtering - len(all_items)
        if skipped_total > 0:
            self.logger.info(
                f"Total: {len(all_items)} new items, {skipped_total} already processed"
            )
        else:
            self.logger.info(f"Total ingested items: {len(all_items)}")

        return {
            "items": all_items,
            "successful_sources": successful_sources,
            "failed_sources": failed_sources,
        }

    def _run_enrichment(
        self,
        canonical_reviews: list[CanonicalClaimReview],
        skip_enrichment: bool = False,
    ) -> list[CanonicalClaimReview]:
        """Run enrichment step, optionally using cache only."""
        if not canonical_reviews:
            return []

        if not self.enrichers:
            self.logger.warning("No enrichers available, skipping enrichment")
            return canonical_reviews

        enriched_reviews = canonical_reviews

        for enricher in self.enrichers:
            if skip_enrichment:
                if enricher.cache:
                    self.logger.info(
                        f"Applying cached enrichment for {enricher.name} (skip enabled)"
                    )
                    enriched_reviews = enricher.apply_cached_only(enriched_reviews)
                else:
                    self.logger.info(
                        f"Skipping {enricher.name}: no cache configured for cached-only enrichment"
                    )
                continue

            if enricher.is_available():
                try:
                    self.logger.info(f"Applying enricher: {enricher.name}")
                    enriched_reviews = enricher.enrich(enriched_reviews)
                except Exception as e:
                    self.logger.error(
                        f"Error in batch enrichment with {enricher.name}: {e}"
                    )
                    continue
            else:
                self.logger.warning(
                    f"Enricher {enricher.name} is not available, skipping"
                )

        self.logger.info(f"Enriched {len(enriched_reviews)} claim reviews")
        return enriched_reviews

    def _run_rdf_generation(
        self,
        canonical_reviews: list[CanonicalClaimReview],
        *,
        mark_processed: bool = True,
    ) -> RDFGenerationResults:
        """Run RDF generation step."""
        if not self.rdf_generator:
            self.logger.error("No RDF generator available")
            return {
                "generated_files": [],
                "total_files": 0,
                "input_items": len(canonical_reviews),
                "successful_items": 0,
                "failed_items": len(canonical_reviews),
                "output_format": self.config.output.format,
                "total_file_size": 0,
                "error": "No RDF generator available",
                "review_outputs": {},
            }

        reviews_by_source: dict[str, list[CanonicalClaimReview]] = {}
        for review in canonical_reviews:
            source_name = review.source_name or "unknown"
            if source_name not in reviews_by_source:
                reviews_by_source[source_name] = []
            reviews_by_source[source_name].append(review)

        output_path_template = str(self.config.output.output_path)
        has_dbpedia_entities = any(
            self.rdf_generator.has_entity_enrichment(review, DBPEDIA_ENTITY_SOURCES)
            for review in canonical_reviews
        )
        if (
            len(reviews_by_source) > 1 or has_dbpedia_entities
        ) and "{SOURCE}" not in output_path_template:
            raise ValueError(
                "Multi-graph RDF generation requires the {SOURCE} placeholder "
                f"in output.output_path: {output_path_template}"
            )

        generated_files: list[GeneratedFileInfo] = []
        total_input_items = len(canonical_reviews)
        total_file_size = 0
        source_errors: list[str] = []
        source_successful_reviews: list[CanonicalClaimReview] = []
        source_output_by_uri: dict[str, tuple[str, str]] = {}

        for source_name, source_reviews in reviews_by_source.items():
            self.logger.info(
                f"Generating RDF for source: {source_name} ({len(source_reviews)} reviews)"
            )

            output_path = Path(
                self._process_dynamic_path(output_path_template, source_name)
            )
            output_format = self.config.output.format

            try:
                successful_review_uris = self.rdf_generator.save(
                    source_reviews, output_path, output_format
                )
                file_size = output_path.stat().st_size if output_path.exists() else 0
            except Exception as e:
                self.logger.error(
                    "Error generating RDF for source %s: %s", source_name, e
                )
                source_errors.append(f"{source_name}: {e}")
                continue

            successful_uri_set = set(successful_review_uris)
            successful_items = len(successful_review_uris)
            failed_items = len(source_reviews) - successful_items
            if failed_items:
                self.logger.warning(
                    "RDF generation failed for %s/%s reviews from source %s",
                    failed_items,
                    len(source_reviews),
                    source_name,
                )

            total_file_size += file_size
            generated_files.append(
                {
                    "graph_name": source_name,
                    "kind": "source",
                    "path": str(output_path),
                    "items": successful_items,
                    "failed_items": failed_items,
                    "file_size": file_size,
                    "review_uris": [
                        review.uri
                        for review in source_reviews
                        if review.uri and review.uri in successful_uri_set
                    ],
                }
            )

            for review in source_reviews:
                if review.uri and review.uri in successful_uri_set:
                    source_successful_reviews.append(review)
                    source_output_by_uri[review.uri] = (
                        source_name,
                        str(output_path),
                    )

        dbpedia_reviews = [
            review
            for review in source_successful_reviews
            if self.rdf_generator.has_entity_enrichment(review, DBPEDIA_ENTITY_SOURCES)
        ]
        dbpedia_successful_uris: set[str] = set()

        if dbpedia_reviews:
            graph_name = DBPEDIA_ENRICHER_SOURCE_NAME
            output_path = Path(
                self._process_dynamic_path(output_path_template, graph_name)
            )
            output_format = self.config.output.format
            try:
                successful_review_uris = self.rdf_generator.save_entity_enrichment(
                    dbpedia_reviews,
                    output_path,
                    output_format,
                    entity_sources=DBPEDIA_ENTITY_SOURCES,
                    property_keys=("dbpedia_properties",),
                )
                file_size = output_path.stat().st_size if output_path.exists() else 0
            except Exception as exc:
                self.logger.error(
                    "Error generating RDF for graph %s: %s", graph_name, exc
                )
                source_errors.append(f"{graph_name}: {exc}")
            else:
                dbpedia_successful_uris = set(successful_review_uris)
                failed_items = len(dbpedia_reviews) - len(successful_review_uris)
                if failed_items:
                    self.logger.warning(
                        "RDF generation failed for %s/%s reviews in graph %s",
                        failed_items,
                        len(dbpedia_reviews),
                        graph_name,
                    )
                total_file_size += file_size
                generated_files.append(
                    {
                        "graph_name": graph_name,
                        "kind": "enrichment",
                        "path": str(output_path),
                        "items": len(successful_review_uris),
                        "failed_items": failed_items,
                        "file_size": file_size,
                        "review_uris": successful_review_uris,
                    }
                )

        dbpedia_required_uris = {review.uri for review in dbpedia_reviews}
        review_outputs: dict[str, ReviewOutputInfo] = {}
        for review in source_successful_reviews:
            if review.uri in dbpedia_required_uris and (
                review.uri not in dbpedia_successful_uris
            ):
                continue
            source_name, source_path = source_output_by_uri[review.uri]
            required_graphs = [source_name]
            if review.uri in dbpedia_required_uris:
                required_graphs.append(DBPEDIA_ENRICHER_SOURCE_NAME)
            review_outputs[review.uri] = {
                "source_name": source_name,
                "source_path": source_path,
                "required_graphs": required_graphs,
            }

        if mark_processed:
            self._mark_uris_processed(
                [
                    (
                        uri,
                        output_info["source_name"],
                        output_info["source_path"],
                    )
                    for uri, output_info in review_outputs.items()
                ]
            )

        total_successful_items = len(review_outputs)
        total_failed_items = total_input_items - total_successful_items

        return {
            "generated_files": generated_files,
            "total_files": len(generated_files),
            "input_items": total_input_items,
            "successful_items": total_successful_items,
            "failed_items": total_failed_items,
            "output_format": self.config.output.format,
            "total_file_size": total_file_size,
            "error": "; ".join(source_errors) or None,
            "review_outputs": review_outputs,
        }

    def _run_deployment(self, rdf_stats: RDFGenerationResults) -> DeploymentResults:
        """Run deployment step."""
        generated_files = rdf_stats.get("generated_files", [])
        total_files = len(generated_files) + 2
        if not self.deployment_handler:
            return {"success": True, "files_deployed": 0, "total_files": total_files}

        self.logger.info("Replacing graph catalog from %s", GRAPH_CATALOG_PATH)
        catalog_success = self.deployment_handler.deploy(
            GRAPH_CATALOG_PATH,
            GRAPH_CATALOG_SOURCE_NAME,
            replace=True,
        )
        if not catalog_success:
            self.logger.error("Graph catalog deployment failed")
            return {
                "success": False,
                "files_deployed": 0,
                "total_files": total_files,
            }

        self.logger.info(
            "Replacing curated organization graph from %s", ORGANIZATION_CATALOG_PATH
        )
        organization_success = self.deployment_handler.deploy(
            ORGANIZATION_CATALOG_PATH,
            ORGANIZATION_SOURCE_NAME,
            replace=True,
        )
        if not organization_success:
            self.logger.error("Curated organization graph deployment failed")
            return {
                "success": False,
                "files_deployed": 1,
                "total_files": total_files,
            }

        deployment_results: list[bool] = [catalog_success, organization_success]
        successful_graphs: set[str] = set()
        for file_info in generated_files:
            output_path = Path(file_info["path"])
            graph_name = file_info["graph_name"]

            self.logger.info(
                f"Deploying RDF file: {output_path} to graph: {graph_name}"
            )
            success = self.deployment_handler.deploy(output_path, graph_name)
            deployment_results.append(success)

            if success:
                successful_graphs.add(graph_name)
                self.logger.info(
                    f"RDF data deployed successfully: {output_path} (graph: {graph_name})"
                )
            else:
                self.logger.error(
                    f"RDF data deployment failed: {output_path} (graph: {graph_name})"
                )

        review_outputs = rdf_stats.get("review_outputs", {})
        completed_outputs = [
            (
                uri,
                output_info["source_name"],
                output_info["source_path"],
            )
            for uri, output_info in review_outputs.items()
            if set(output_info["required_graphs"]) <= successful_graphs
        ]
        self._mark_uris_processed(completed_outputs)

        return {
            "success": all(deployment_results),
            "files_deployed": sum(deployment_results),
            "total_files": total_files,
        }

    def _process_dynamic_path(
        self, path_template: str, source_name: str | None = None
    ) -> str:
        """Process dynamic path templates."""
        reference_dt = self._run_datetime or datetime.now()

        replacements = {
            "{DATE}": reference_dt.strftime("%Y-%m-%d"),
            "{TIME}": reference_dt.strftime("%H%M%S"),
            "{DATETIME}": reference_dt.strftime("%Y-%m-%d_%H%M%S"),
            "{TIMESTAMP}": reference_dt.strftime("%Y%m%d%H%M%S"),
        }

        processed_path = path_template
        for placeholder, value in replacements.items():
            processed_path = processed_path.replace(placeholder, value)

        if source_name:
            processed_path = processed_path.replace("{SOURCE}", source_name)

        return processed_path

    def clear_cache(self, source_name: str | None = None) -> None:
        """Clear cache for all sources or specific source."""
        self.data_manager.clear_cache(source_name)
