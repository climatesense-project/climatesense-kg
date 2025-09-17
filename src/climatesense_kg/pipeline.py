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
from .config.models import CanonicalClaimReview
from .data_manager import DataManager
from .deployment.virtuoso import VirtuosoDeploymentHandler
from .enrichers.base import Enricher as BaseEnricher
from .enrichers.bert_factors_enricher import BertFactorsEnricher
from .enrichers.dbpedia_enricher import DBpediaEnricher
from .enrichers.url_text_enricher import URLTextEnricher
from .rdf_generation.generator import RDFGenerator
from .utils.logging import configure_external_loggers, setup_logging

logger = logging.getLogger(__name__)


class DataSourceResults(TypedDict):
    total_items: int
    sources_processed: int


class EnrichmentResults(TypedDict):
    input_items: int
    output_items: int


class GeneratedFileInfo(TypedDict):
    source: str
    path: str
    items: int
    file_size: int


class RDFGenerationResults(TypedDict):
    generated_files: list[GeneratedFileInfo]
    total_files: int
    input_items: int
    output_format: str
    total_file_size: int
    error: str | None


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
        self.deployment_handler: VirtuosoDeploymentHandler | None = None
        self.cache: CacheInterface | None = None
        self._initialize_components()

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
            virtuoso_config = self.config.deployment.virtuoso
            if virtuoso_config.enabled:
                host = os.getenv("VIRTUOSO_HOST", "localhost")
                port = int(os.getenv("VIRTUOSO_PORT", "8890"))
                user = os.getenv("VIRTUOSO_USER", "dba")
                password = os.getenv("VIRTUOSO_PASSWORD", "dba")
                isql_service_url = os.getenv(
                    "VIRTUOSO_ISQL_SERVICE_URL", "http://isql-service:8080"
                )

                self.deployment_handler = VirtuosoDeploymentHandler(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    graph_template=virtuoso_config.graph_template,
                    isql_service_url=isql_service_url,
                )
                self.logger.info(
                    f"Initialized Virtuoso deployment handler (host: {host}:{port})"
                )
            else:
                self.deployment_handler = None
        except Exception as e:
            self.logger.error(f"Failed to initialize deployment handler: {e}")
            self.deployment_handler = None

    def _is_uri_processed(self, uri: str) -> bool:
        """Check if a URI has been successfully processed."""
        if not self.cache:
            return False

        # Check pipeline completion cache
        pipeline_data = self.cache.get(uri, "pipeline.processed_items")
        if pipeline_data is None:
            return False

        # Check all enricher steps
        for enricher in self.enrichers:
            if enricher.is_available():
                enricher_data = self.cache.get(uri, enricher.step_name)
                if enricher_data is None:
                    return False

        return True

    def _mark_uri_processed(
        self, uri: str, source_name: str, rdf_file_path: str
    ) -> None:
        """Mark a URI as successfully processed."""
        if not self.cache:
            return

        step_name = "pipeline.processed_items"
        payload = {
            "source": source_name,
            "processed_at": datetime.now().isoformat(),
            "rdf_file_path": rdf_file_path,
        }

        self.cache.set(uri, step_name, payload)

    def _get_fully_processed_uris(self, uris: list[str]) -> set[str]:
        """
        Find which URIs are fully processed using bulk cache queries.

        Returns:
            Set of URIs that have completed all processing steps
        """
        if not self.cache or not uris:
            return set()

        # Check pipeline completion step
        pipeline_cached = self.cache.get_many(uris, "pipeline.processed_items")
        pipeline_processed_uris = set(pipeline_cached.keys())

        if not pipeline_processed_uris:
            return set()

        # Check all enricher steps for pipeline-processed items
        fully_processed_uris = pipeline_processed_uris
        for enricher in self.enrichers:
            if enricher.is_available():
                enricher_cached = self.cache.get_many(
                    list(fully_processed_uris), enricher.step_name
                )
                # Only keep URIs that have this enricher step cached
                fully_processed_uris &= set(enricher_cached.keys())

        return fully_processed_uris

    def run(self, force_deployment: bool = False) -> PipelineResults:
        """Execute the complete pipeline.

        Args:
            force_deployment: Force deployment even when no RDF changes are detected

        Returns:
            Pipeline execution results and statistics
        """
        start_time = time.time()
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
            canonical_reviews = self._run_ingestion()
            results["data_sources"] = {
                "total_items": len(canonical_reviews),
                "sources_processed": len(
                    [s for s in self.config.data_sources if s.enabled]
                ),
            }

            if not canonical_reviews:
                self.logger.info("No new items to process.")

                # Check if force deployment is requested even with no new data
                if force_deployment and self.deployment_handler:
                    self.logger.info(
                        "Force deployment requested - deploying existing RDF files"
                    )
                    # Find existing RDF files to redeploy
                    existing_rdf_stats = self._find_existing_rdf_files()
                    if existing_rdf_stats.get("generated_files"):
                        deployment_success = self._run_deployment(
                            existing_rdf_stats, force_deployment
                        )
                        generated_files_count = len(
                            existing_rdf_stats["generated_files"]
                        )
                        results["deployment"] = {
                            "success": deployment_success,
                            "files_deployed": (
                                generated_files_count if deployment_success else 0
                            ),
                            "total_files": generated_files_count,
                        }
                        results["success"] = deployment_success
                    else:
                        self.logger.warning("No existing RDF files found to redeploy")
                        results["success"] = True
                else:
                    self.logger.info("Pipeline completed successfully.")
                    results["success"] = True

                results["total_processed"] = 0
                end_time = time.time()
                results["end_time"] = end_time
                results["duration"] = end_time - start_time
                return results

            # Step 2: Enrichment
            self.logger.info("Step 2: Data Enrichment")
            enriched_reviews = self._run_enrichment(canonical_reviews)
            results["enrichment"] = {
                "input_items": len(canonical_reviews),
                "output_items": len(enriched_reviews),
            }

            # Step 3: RDF Generation
            self.logger.info("Step 3: RDF Generation")
            rdf_stats = self._run_rdf_generation(enriched_reviews)
            results["rdf_generation"] = rdf_stats

            # Step 4: Deployment
            if self.deployment_handler:
                self.logger.info("Step 4: Deploying RDF data")
                deployment_success = self._run_deployment(rdf_stats, force_deployment)
            else:
                deployment_success = True
                results["deployment"] = {
                    "success": deployment_success,
                    "files_deployed": (
                        len(rdf_stats["generated_files"]) if deployment_success else 0
                    ),
                    "total_files": len(rdf_stats["generated_files"]),
                }

            # Final statistics
            results["total_processed"] = len(enriched_reviews)
            results["success"] = deployment_success

            end_time = time.time()
            results["end_time"] = end_time
            results["duration"] = end_time - start_time

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

        return results

    def _run_ingestion(self) -> list[CanonicalClaimReview]:
        """Run data ingestion using DataManager."""
        all_items: list[CanonicalClaimReview] = []
        total_items_before_filtering = 0

        for source_config in self.config.data_sources:
            if not source_config.enabled:
                continue

            try:
                items = list(self.data_manager.get_data(source_config))
                total_items_before_filtering += len(items)

                # Filter out already-processed items
                if self.cache:
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
                    self.logger.info(
                        f"Ingested {len(items)} items from {source_config.name} (no cache filtering)"
                    )
                    all_items.extend(items)

            except Exception as e:
                self.logger.error(f"Error ingesting from {source_config.name}: {e}")

        skipped_total = total_items_before_filtering - len(all_items)
        if skipped_total > 0:
            self.logger.info(
                f"Total: {len(all_items)} new items, {skipped_total} already processed"
            )
        else:
            self.logger.info(f"Total ingested items: {len(all_items)}")

        return all_items

    def _run_enrichment(
        self, canonical_reviews: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        """Run enrichment step."""
        if not self.enrichers:
            self.logger.warning("No enrichers available, skipping enrichment")
            return canonical_reviews

        enriched_reviews = canonical_reviews

        for enricher in self.enrichers:
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
        self, canonical_reviews: list[CanonicalClaimReview]
    ) -> RDFGenerationResults:
        """Run RDF generation step."""
        if not self.rdf_generator:
            self.logger.error("No RDF generator available")
            return {
                "generated_files": [],
                "total_files": 0,
                "input_items": len(canonical_reviews),
                "output_format": self.config.output.format,
                "total_file_size": 0,
                "error": "No RDF generator available",
            }

        try:
            reviews_by_source: dict[str, list[CanonicalClaimReview]] = {}
            for review in canonical_reviews:
                source_name = review.source_name or "unknown"
                if source_name not in reviews_by_source:
                    reviews_by_source[source_name] = []
                reviews_by_source[source_name].append(review)

            generated_files: list[GeneratedFileInfo] = []
            total_input_items = len(canonical_reviews)
            total_file_size = 0

            for source_name, source_reviews in reviews_by_source.items():
                self.logger.info(
                    f"Generating RDF for source: {source_name} ({len(source_reviews)} reviews)"
                )

                output_path = Path(
                    self._process_dynamic_path(
                        str(self.config.output.output_path), source_name
                    )
                )
                output_format = self.config.output.format

                self.rdf_generator.save(source_reviews, output_path, output_format)

                # Mark URIs as processed after successful RDF generation
                for review in source_reviews:
                    self._mark_uri_processed(review.uri, source_name, str(output_path))

                file_size = output_path.stat().st_size if output_path.exists() else 0
                total_file_size += file_size

                generated_files.append(
                    {
                        "source": source_name,
                        "path": str(output_path),
                        "items": len(source_reviews),
                        "file_size": file_size,
                    }
                )

            return {
                "generated_files": generated_files,
                "total_files": len(generated_files),
                "input_items": total_input_items,
                "output_format": self.config.output.format,
                "total_file_size": total_file_size,
                "error": None,
            }

        except Exception as e:
            self.logger.error(f"Error generating RDF: {e}")
            return {
                "generated_files": [],
                "total_files": 0,
                "input_items": len(canonical_reviews),
                "output_format": self.config.output.format,
                "total_file_size": 0,
                "error": str(e),
            }

    def _run_deployment(
        self, rdf_stats: RDFGenerationResults, force: bool = False
    ) -> bool:
        """Run deployment step."""
        if not self.deployment_handler:
            return True

        generated_files = rdf_stats.get("generated_files", [])
        if not generated_files:
            self.logger.warning("No RDF files to deploy")
            return False

        deployment_results: list[bool] = []
        for file_info in generated_files:
            output_path = Path(file_info["path"])
            source_name = file_info["source"]

            self.logger.info(
                f"Deploying RDF file: {output_path} for source: {source_name}"
            )
            success = self.deployment_handler.deploy(output_path, source_name)
            deployment_results.append(success)

            if success:
                self.logger.info(
                    f"RDF data deployed successfully: {output_path} (source: {source_name})"
                )
            else:
                self.logger.error(
                    f"RDF data deployment failed: {output_path} (source: {source_name})"
                )

        return all(deployment_results)

    def _find_existing_rdf_files(self) -> RDFGenerationResults:
        """Find existing RDF files for redeployment."""
        generated_files: list[GeneratedFileInfo] = []

        for source_config in self.config.data_sources:
            if not source_config.enabled:
                continue

            output_path = Path(
                self._process_dynamic_path(
                    str(self.config.output.output_path), source_config.name
                )
            )

            if output_path.exists():
                file_size = output_path.stat().st_size
                generated_files.append(
                    {
                        "source": source_config.name,
                        "path": str(output_path),
                        "items": 0,  # Unknown for existing files
                        "file_size": file_size,
                    }
                )
                self.logger.info(
                    f"Found existing RDF file for redeployment: {output_path}"
                )
            else:
                self.logger.debug(f"No existing RDF file found: {output_path}")

        return {
            "generated_files": generated_files,
            "total_files": len(generated_files),
            "input_items": 0,
            "output_format": self.config.output.format,
            "total_file_size": sum(f["file_size"] for f in generated_files),
            "error": None,
        }

    def _process_dynamic_path(
        self, path_template: str, source_name: str | None = None
    ) -> str:
        """Process dynamic path templates."""
        current_date = datetime.now().strftime("%Y-%m-%d")
        processed_path = path_template.replace("{DATE}", current_date)

        if source_name:
            processed_path = processed_path.replace("{SOURCE}", source_name)

        return processed_path

    def clear_cache(self, source_name: str | None = None) -> None:
        """Clear cache for all sources or specific source."""
        self.data_manager.clear_cache(source_name)
