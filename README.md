# ClimateSense KG Pipeline

[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://python.org)
[![uv](https://img.shields.io/badge/dependency-uv-blue)](https://docs.astral.sh/uv/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> The ClimateSense KG is a continuously updated knowledge graph that integrates climate fact-checking data from multiple sources to combat climate misinformation. It links information from fact-checking organizations with enriched data, giving researchers a more comprehensive view of the problem.

## 🔍 Overview

![Pipeline Architecture](docs/pipeline.svg)

### Key Features

- Multi-source ingestion from major climate fact-checking organizations
  - [EuroClimateCheck](https://github.com/climatesense-project/euroclimatecheck-scraper)
  - [ClaimReviewData](https://github.com/MartinoMensio/claimreview-data)
  - [DeFacto](https://defacto-observatoire.fr/Fact-checks/)
  - [DBKF](https://dbkf.ontotext.com/)
  - [DeSmog](https://github.com/climatesense-project/climate-disinformation-database)
  - [Climafacts (Skeptical-Science)](https://github.com/climatesense-project/climafacts-kg)
  - [CLIMATE-FEVER](https://www.sustainablefinance.uzh.ch/en/research/climate-fever.html)
- Data enrichment with:
  - Text extraction from URLs using [trafilatura](https://trafilatura.readthedocs.io/)
  - Entity linking using [DBpedia Spotlight](https://www.dbpedia-spotlight.org/)
  - [Factors classification](https://github.com/climatesense-project/cimple-factors-server) using fine-tuned BERT models
- RDF output using [Schema.org](https://schema.org/) and [CIMPLE ontology](https://github.com/CIMPLE-project/knowledge-base)
- Triple store deployment through Virtuoso or QLever
- [YAML-based configuration](#configuration)

### Documentation & Resources

- [URI design patterns](docs/URI-patterns.md) and [RDF namespaces](docs/URI-patterns.md#rdf-namespace-declarations)
- Public SPARQL endpoint: https://data.climatesense-project.eu/sparql

## 📋 Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (for dependency management)
- [just](https://github.com/casey/just) (for task automation)
- [Docker & Docker Compose](https://docs.docker.com/get-docker/) (for Docker setup)

## Table of Contents

- [ClimateSense KG Pipeline](#climatesense-kg-pipeline)
  - [🔍 Overview](#-overview)
    - [Key Features](#key-features)
    - [Documentation \& Resources](#documentation--resources)
  - [📋 Prerequisites](#-prerequisites)
  - [Table of Contents](#table-of-contents)
  - [Quick Start](#quick-start)
  - [Configuration](#configuration)
  - [Querying the Knowledge Graph](#querying-the-knowledge-graph)
    - [Example SPARQL Queries](#example-sparql-queries)
  - [Development](#development)
    - [Setup](#setup)
    - [Common Tasks](#common-tasks)
    - [CLI Usage](#cli-usage)
      - [`run` - Run the pipeline](#run---run-the-pipeline)
      - [`purge-processing-results` - Delete recomputable results](#purge-processing-results---delete-recomputable-results)
  - [Acknowledgments](#acknowledgments)

## Quick Start

The quickest way to get started is by using Docker, which brings up every required service (including PostgreSQL).

**Install:**

```bash
git clone https://github.com/climatesense-project/climatesense-kg.git
cd climatesense-kg
just install
cp docker/.env.example docker/.env
```

At minimum, set the required secrets in `docker/.env` (`VIRTUOSO_PASSWORD`, `QLEVER_ACCESS_TOKEN`, `POSTGRES_PASSWORD`), and the RDF Workbench secrets (`BOOTSTRAP_ADMIN_PASSWORD` for QLever, `SESSION_SECRET` and `VIRTUOSO_ADAPTER_TOKEN` for Virtuoso). The triplestore is chosen by which compose overlay you load: `docker/docker-compose.virtuoso.yml` or `docker/docker-compose.qlever.yml`.

**Run the minimal pipeline:**

```bash
docker compose -f docker/docker-compose.yml run --rm pipeline \
  run --config config/minimal.yaml
```

This ingests the bundled sample documents and writes an RDF snapshot under `data/rdf/`.

**Deploy the snapshot to your chosen triplestore:**

- **Virtuoso**

  ```bash
  just virtuoso-deploy && just virtuoso-up
  ```

- **QLever**

  ```bash
  just qlever-deploy && just qlever-up
  ```

The stack also includes the [RDF Workbench](https://github.com/ehrhart/rdf-workbench), a UI for exploring and managing the triplestore, available at `http://localhost:3001`. It starts with either overlay. For Virtuoso, uploads and exports live in the `rdf-workbench-imports` volume, which is shared with the Virtuoso container for bulk loading.

## Configuration

The pipeline uses YAML-based configuration. Example config:

```yaml
data_sources:
  - name: "claimreview_sample"
    type: "claimreviewdata"
    provider:
      provider_type: "file"
      file_path: "samples/claimreviewdata-data/claim_reviews.json"
  - name: "euroclimatecheck_sample"
    type: "euroclimatecheck"
    provider:
      provider_type: "file"
      file_path: "samples/euroclimatecheck-data/all_articles.json"

batch_size: 500
progress_interval_seconds: 10

document_extraction:
  enabled: true
  max_workers: 32
  rate_limit_delay: 0.5
  timeout: 15
  max_retries: 2
  transient_retry_delay_hours: 1
  blocked_retry_delay_hours: 720
  dns_retry_delay_hours: 168
  content_retry_delay_hours: 720
  progress_interval_seconds: 10

enrichment:
  progress_interval_seconds: 10

  dbpedia_spotlight:
    enabled: true
    api_url: "https://dbpedia-spotlight.tools.eurecom.fr/rest/annotate"
    model_id: "dbpedia-spotlight-en"
    confidence: 0.6
    support: 30
    timeout: 20
    max_workers: 8

  dbpedia_entity_properties:
    enabled: true
    sparql_endpoint: "https://dbpedia.org/sparql"
    properties:
      - "http://www.w3.org/2003/01/geo/wgs84_pos#geometry"
      - "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
      - "http://www.w3.org/2003/01/geo/wgs84_pos#long"
      - "http://www.georss.org/georss/point"
      - "http://www.opengis.net/ont/geosparql#asWKT"
    timeout: 30
    rate_limit_delay: 0

  cimple:
    enabled: true
    model_versions:
      emotion: "1"
      sentiment: "1"
      political_leaning: "1"
      tropes: "1"
      persuasion_techniques: "1"
      conspiracies: "1"
      climate_related: "1"
    batch_size: 64
    max_length: 128
    timeout: 300
    rate_limit_delay: 0
    max_workers: 1

output:
  output_path: "data/rdf/{DATETIME}/{SOURCE}.nt.gz"
  base_uri: "http://data.climatesense-project.eu"
  retention:
    keep_latest: 3

cache:
  cache_dir: "cache"
  default_ttl_hours: 24.0

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file_path: "logs/daily_pipeline.log"
  max_file_size: "50MB"
  backup_count: 10
```

`data/organizations.ttl` is the fixed, manually maintained source of truth for fact-checker identity and metadata.

`data/graphs.ttl` is the curated catalog for the published named graphs.

## Querying the Knowledge Graph

Once loaded into the selected triplestore, query the knowledge graph using SPARQL:

- **Virtuoso SPARQL Endpoint**: http://localhost:8890/sparql
- **Virtuoso Faceted Browser**: http://localhost:8890/fct
- **QLever SPARQL Endpoint**: http://localhost:7019
- **RDF Workbench**: http://localhost:3001

### Example SPARQL Queries

**Find all climate claims:**

```sparql
PREFIX schema: <http://schema.org/>
SELECT ?claim ?text ?rating
WHERE {
  ?review a schema:ClaimReview ;
          schema:itemReviewed ?claim ;
          schema:reviewRating ?rating .
  ?claim schema:text ?text .
}
LIMIT 10
```

**Find claims by fact-checking organization:**

```sparql
PREFIX schema: <http://schema.org/>
SELECT ?claim ?author ?authorName
WHERE {
  ?claim a schema:ClaimReview ;
         schema:author ?author .
  ?author a schema:Organization ;
          schema:name ?authorName .
}
LIMIT 10
```

## Development

> Running the pipeline directly on the host (via `just run ...` or `uv run climatesense-kg run ...`) requires a running PostgreSQL instance. The Docker commands above are the easiest way to get one set-up.

### Setup

```bash
just setup-dev
```

### Common Tasks

```bash
just format          # Format code with ruff
just check           # Run linting and type checks
just pre-commit-all  # Run pre-commit on all files
```

### CLI Usage

The CLI is invoked as `uv run climatesense-kg`. Available commands:

#### `run` - Run the pipeline

| Argument                | Description                                                            |
| ----------------------- | ---------------------------------------------------------------------- |
| `-c, --config CONFIG`   | Configuration file path                                                |
| `--debug`               | Enable DEBUG level logging                                             |
| `--skip-download`       | Skip data downloads and use only cached/already downloaded data        |
| `--skip-extraction`     | Skip external document fetches; apply stored successful results        |
| `--skip-enrichment`     | Skip external enrichment calls; apply stored successful results        |
| `--no-cache-extraction` | Ignore stored successes and refetch/recompute all document extractions |
| `--no-cache-enrichment` | Ignore stored successes and re-run all enrichment                      |

#### `purge-processing-results` - Delete recomputable results

| Argument | Description                                          |
| -------- | ---------------------------------------------------- |
| `--yes`  | Confirm deletion of all persisted processing results |

**Examples:**

```bash
# Display help
uv run climatesense-kg --help

# Run minimal pipeline with debug logging
uv run climatesense-kg run --config config/minimal.yaml --debug

# Recompute extraction and enrichment results while using cached source data
uv run climatesense-kg run --config config/daily.yaml --skip-download --no-cache-extraction --no-cache-enrichment

# Restore stored extractions without fetching fact-check documents
uv run climatesense-kg run --config config/daily.yaml --skip-extraction

# Redeploy existing RDF snapshots
# just virtuoso-deploy  # Virtuoso
# just qlever-deploy    # QLever

# Or select a snapshot explicitly
# just virtuoso-deploy data/rdf/2026-08-15_143734  # Virtuoso
# just qlever-deploy data/rdf/2026-08-15_143734    # QLever

# Delete recomputable extraction and enrichment results without deleting identities
uv run climatesense-kg purge-processing-results --yes
```

## Acknowledgments

This project builds upon the work of the CIMPLE project and reuses components from:

- [CIMPLE Converter](https://github.com/CIMPLE-project/converter)
- [CIMPLE Knowledge Base](https://github.com/CIMPLE-project/knowledge-base)
