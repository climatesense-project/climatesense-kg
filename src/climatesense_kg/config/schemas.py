"""Configuration schemas for the ClimateSense KG Pipeline."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from .enrichment import default_cimple_model_versions


@dataclass
class FileProviderConfig:
    """Local-file provider configuration."""

    provider_type: Literal["file"]
    file_path: str | Path


@dataclass
class GitHubProviderConfig:
    """GitHub release-asset or repository-file configuration."""

    provider_type: Literal["github"]
    repository: str = ""
    asset_pattern: str = "*.json"
    extract_file: str | None = None
    mode: Literal["release", "repository"] = "release"
    repository_path: str = ""
    repository_ref: str = "main"
    max_download_size: str = "512MB"
    max_extract_size: str = "256MB"
    download_spool_threshold_bytes: int = 8 * 1024 * 1024

    timeout: int = 30

    def __post_init__(self) -> None:
        if self.download_spool_threshold_bytes <= 0:
            raise ValueError(
                "GitHub download spool threshold must be greater than zero"
            )


@dataclass
class GraphQLProviderConfig:
    """Paginated GraphQL provider configuration."""

    provider_type: Literal["graphql"]
    endpoint: str = ""
    query: str = ""
    variables: dict[str, Any] = field(default_factory=dict[str, Any])
    batch_size: int = 100
    max_retries: int = 3

    rate_limit_delay: float = 1.0
    timeout: int = 30

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("GraphQL provider batch_size must be greater than zero")


@dataclass
class XWikiProviderConfig:
    """XWiki fact-check API provider configuration."""

    provider_type: Literal["xwiki"]
    base_url: str = ""
    tags: list[str] = field(default_factory=list[str])
    rate_limit_delay: float = 1.0
    timeout: int = 30


@dataclass
class HttpProviderConfig:
    """Static HTTP download provider configuration."""

    provider_type: Literal["http"]
    url: str = ""
    timeout: int = 30


ProviderConfig = (
    FileProviderConfig
    | GitHubProviderConfig
    | GraphQLProviderConfig
    | XWikiProviderConfig
    | HttpProviderConfig
)


@dataclass
class DataSourceConfig:
    """Configuration for a data source."""

    name: str
    type: Literal[
        "claimreviewdata",
        "climafacts",
        "euroclimatecheck",
        "dbkf",
        "defacto",
        "desmog",
        "climate-fever",
    ]
    enabled: bool = True
    provider: ProviderConfig | None = None
    cache_ttl_hours: float | None = None


@dataclass
class DocumentExtractionConfig:
    """Configuration for pre-identity document extraction."""

    enabled: bool = True
    max_workers: int = 32
    rate_limit_delay: float = 0.5
    timeout: int = 15
    max_retries: int = 2
    transient_retry_delay_hours: float = 1
    blocked_retry_delay_hours: float = 24 * 30
    dns_retry_delay_hours: float = 24 * 7
    content_retry_delay_hours: float = 24 * 30
    progress_interval_seconds: float = 10.0

    def __post_init__(self) -> None:
        if self.max_workers <= 0:
            raise ValueError("Document extraction max_workers must be positive")
        if any(
            delay < 0
            for delay in (
                self.transient_retry_delay_hours,
                self.blocked_retry_delay_hours,
                self.dns_retry_delay_hours,
                self.content_retry_delay_hours,
            )
        ):
            raise ValueError("Document extraction retry delays must be non-negative")
        if self.progress_interval_seconds < 0:
            raise ValueError(
                "Document extraction progress_interval_seconds must be non-negative"
            )


@dataclass
class DuplicateAuditConfig:
    """Settings for the optional exact near-duplicate audit."""

    similarity_threshold: float = 0.9
    minimum_similarity_words: int = 50
    group_batch_size: int = 100

    def __post_init__(self) -> None:
        if not 0 <= self.similarity_threshold <= 1:
            raise ValueError("Duplicate audit threshold must be between zero and one")
        if self.minimum_similarity_words <= 0:
            raise ValueError("Duplicate audit minimum words must be positive")
        if self.group_batch_size <= 0:
            raise ValueError("Duplicate audit group_batch_size must be positive")


@dataclass
class DbpediaSpotlightConfig:
    """Configuration for DBpedia Spotlight entity extraction."""

    enabled: bool = False
    api_url: str = "https://dbpedia-spotlight.tools.eurecom.fr/rest/annotate"
    model_id: str = "dbpedia-spotlight-en"
    confidence: float = 0.5
    support: int = 20
    timeout: int = 20
    max_workers: int = 8

    def __post_init__(self) -> None:
        if self.max_workers <= 0:
            raise ValueError("DBpedia Spotlight max_workers must be positive")


@dataclass
class DbpediaEntityPropertiesConfig:
    """Configuration for DBpedia entity property enrichment."""

    enabled: bool = False
    sparql_endpoint: str = "https://dbpedia.org/sparql"
    properties: list[str] = field(default_factory=list[str])
    timeout: int = 20
    rate_limit_delay: float = 0.1
    max_retries: int = 2


@dataclass
class CimpleConfig:
    """Configuration for individually persisted CIMPLE models."""

    enabled: bool = False
    model_versions: dict[str, str] = field(
        default_factory=default_cimple_model_versions
    )
    batch_size: int = 32
    max_length: int = 128
    timeout: int = 60
    rate_limit_delay: float = 0.1
    max_workers: int = 1

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("CIMPLE batch_size must be positive")
        if self.max_workers <= 0:
            raise ValueError("CIMPLE max_workers must be positive")


@dataclass
class EnrichmentConfig:
    """Configuration for enrichment methods."""

    progress_interval_seconds: float = 10.0
    dbpedia_spotlight: DbpediaSpotlightConfig = field(
        default_factory=DbpediaSpotlightConfig
    )
    dbpedia_entity_properties: DbpediaEntityPropertiesConfig = field(
        default_factory=DbpediaEntityPropertiesConfig
    )
    cimple: CimpleConfig = field(default_factory=CimpleConfig)

    def __post_init__(self) -> None:
        if self.progress_interval_seconds < 0:
            raise ValueError(
                "Enrichment progress_interval_seconds must be non-negative"
            )
        if (
            self.dbpedia_entity_properties.enabled
            and not self.dbpedia_spotlight.enabled
        ):
            raise ValueError(
                "DBpedia entity properties require DBpedia Spotlight to be enabled"
            )


@dataclass
class SnapshotRetentionConfig:
    """Retention policy for full RDF snapshot runs."""

    keep_latest: int = 0


@dataclass
class OutputConfig:
    """Configuration for RDF output."""

    output_path: str | Path = "data/rdf/{DATETIME}/{SOURCE}.nt.gz"
    base_uri: str = "http://data.climatesense-project.eu"
    retention: SnapshotRetentionConfig = field(default_factory=SnapshotRetentionConfig)

    def __post_init__(self) -> None:
        if self.retention.keep_latest < 0:
            raise ValueError("Output retention keep_latest must be non-negative")


@dataclass
class LoggingConfig:
    """Configuration for logging."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: str | Path | None = None
    max_file_size: str = "10MB"
    backup_count: int = 5


@dataclass
class DeploymentConfig:
    """Configuration for deployment settings."""

    backend: Literal["none", "virtuoso"] = "none"
    graph_template: str = "http://data.climatesense-project.eu/graph/{SOURCE}"


@dataclass
class CacheConfig:
    """Configuration for data cache."""

    cache_dir: str | Path = "cache"
    default_ttl_hours: float = 24.0


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""

    data_sources: list[DataSourceConfig] = field(default_factory=list[DataSourceConfig])
    batch_size: int = 500
    progress_interval_seconds: float = 10.0
    document_extraction: DocumentExtractionConfig = field(
        default_factory=DocumentExtractionConfig
    )
    duplicate_audit: DuplicateAuditConfig = field(default_factory=DuplicateAuditConfig)
    enrichment: EnrichmentConfig = field(default_factory=EnrichmentConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    deployment: DeploymentConfig = field(default_factory=DeploymentConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("Pipeline batch_size must be positive")
        if self.progress_interval_seconds < 0:
            raise ValueError("Pipeline progress interval must be non-negative")
