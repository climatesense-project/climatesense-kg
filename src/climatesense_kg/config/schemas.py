"""Configuration schemas for the ClimateSense KG Pipeline."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


@dataclass
class ProviderConfig:
    """Configuration for data providers."""

    provider_type: Literal["file", "github", "graphql", "xwiki"]

    # Common parameters
    file_path: str | Path | None = None

    # GitHub provider parameters
    repository: str = ""
    asset_pattern: str = "*.json"
    extract_file: str | None = None

    # GraphQL provider parameters
    endpoint: str = ""
    query: str = ""
    variables: dict[str, Any] = field(default_factory=dict[str, Any])
    batch_size: int = 100
    max_retries: int = 3

    # REST API provider parameters
    base_url: str = ""
    tags: list[str] = field(default_factory=list[str])

    # Common network parameters
    rate_limit_delay: float = 1.0
    timeout: int = 30


@dataclass
class DataSourceConfig:
    """Configuration for a data source."""

    name: str
    type: Literal["misinfome", "euroclimatecheck", "dbkf", "defacto"]
    enabled: bool = True
    provider: ProviderConfig | None = None
    cache_ttl_hours: float | None = None


@dataclass
class UrlTextExtractionConfig:
    """Configuration for URL text extraction."""

    enabled: bool = False
    rate_limit_delay: float = 0.5
    timeout: int = 15
    max_retries: int = 2


@dataclass
class DbpediaSpotlightConfig:
    """Configuration for DBpedia Spotlight entity extraction."""

    enabled: bool = False
    api_url: str = "https://api.dbpedia-spotlight.org/en/annotate"
    confidence: float = 0.5
    support: int = 20
    timeout: int = 20
    rate_limit_delay: float = 0.1


@dataclass
class BertFactorsConfig:
    """Configuration for BERT factors enrichment."""

    enabled: bool = False
    models_path: str = "/data/cimple-factors-models"
    batch_size: int = 32
    device: Literal["auto", "cpu", "cuda"] = "auto"
    max_length: int = 128


@dataclass
class EnrichmentConfig:
    """Configuration for enrichment methods."""

    dbpedia_spotlight: DbpediaSpotlightConfig = field(
        default_factory=DbpediaSpotlightConfig
    )
    bert_factors: BertFactorsConfig = field(default_factory=BertFactorsConfig)
    url_text_extraction: UrlTextExtractionConfig = field(
        default_factory=UrlTextExtractionConfig
    )


@dataclass
class OutputConfig:
    """Configuration for RDF output."""

    format: Literal["turtle", "nt", "rdf/xml", "n3"] = "nt"
    output_path: str | Path = "output/knowledge_graph.nt"
    base_uri: str = "http://data.climatesense-project.eu"
    uri_cache_path: str | Path | None = None


@dataclass
class LoggingConfig:
    """Configuration for logging."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: str | Path | None = None
    max_file_size: str = "10MB"
    backup_count: int = 5


@dataclass
class VirtuosoConfig:
    """Configuration for Virtuoso triplestore deployment."""

    enabled: bool = False
    host: str = "localhost"
    port: int = 1111
    user: str = "dba"
    password: str = "dba"  # noqa: S105
    isql_service_url: str | None = None
    graph_template: str = "http://data.climatesense-project.eu/graph/{SOURCE}"


@dataclass
class DeploymentConfig:
    """Configuration for deployment settings."""

    virtuoso: VirtuosoConfig = field(default_factory=VirtuosoConfig)


@dataclass
class CacheConfig:
    """Configuration for data cache."""

    cache_dir: str | Path = "cache"
    default_ttl_hours: float = 24.0


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""

    data_sources: list[DataSourceConfig] = field(default_factory=list[DataSourceConfig])
    enrichment: EnrichmentConfig = field(default_factory=EnrichmentConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    deployment: DeploymentConfig = field(default_factory=DeploymentConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
