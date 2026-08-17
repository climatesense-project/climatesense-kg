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
- Triple store deployment through Virtuoso uploads or complete QLever indexes
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
  - [Docker Setup](#docker-setup)
  - [Configuration](#configuration)
  - [Querying pipeline state](#querying-pipeline-state)
    - [Example SQL Queries](#example-sql-queries)
  - [Production operations](#production-operations)
  - [Querying the Knowledge Graph](#querying-the-knowledge-graph)
    - [Example SPARQL Queries](#example-sparql-queries)
  - [Auditing near-duplicate claim reviews](#auditing-near-duplicate-claim-reviews)
  - [Development](#development)
    - [Setup](#setup)
    - [Common Tasks](#common-tasks)
    - [CLI Usage](#cli-usage)
    - [QLever UI](#qlever-ui)
  - [Acknowledgments](#acknowledgments)

## Quick Start

**Install:**

```bash
git clone https://github.com/climatesense-project/climatesense-kg.git
cd climatesense-kg
just install
```

**Run:**

```bash
just run config/minimal.yaml
```

## Docker Setup

**Requirements:**

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [just](https://github.com/casey/just)

**Initial Setup:**

1. Clone the repository and navigate to the project directory:

   ```bash
   git clone https://github.com/climatesense-project/climatesense-kg.git
   cd climatesense-kg
   ```

2. Copy and configure environment variables:

   ```bash
   cp docker/.env.example docker/.env
   ```

   Edit `.env` to configure:
   - `COMPOSE_PROFILES`: Select exactly one triplestore profile (`virtuoso` or `qlever`)
   - `GITHUB_TOKEN`: GitHub token used for private repositories
   - `VIRTUOSO_HOST`: Virtuoso host name (default `virtuoso`)
   - `VIRTUOSO_BIND_ADDRESS`: Host interface for the Virtuoso HTTP endpoint (default `127.0.0.1`)
   - `VIRTUOSO_PORT`: Virtuoso HTTP/SPARQL port (default `8890`)
   - `VIRTUOSO_USER`: Virtuoso database user (default `dba`)
   - `VIRTUOSO_PASSWORD`: Virtuoso database password (required; no default)
   - `QLEVER_ENDPOINT`: Internal QLever SPARQL endpoint used by analytics (default `http://qlever:7019`)
   - `QLEVER_PORT`: Published QLever port (default `7019`)
   - `QLEVER_ACCESS_TOKEN`: Token for QLever administrative operations
   - `QLEVER_INDEX_MEMORY`: Memory available to index builds (default `1G`)
   - `QLEVER_MEMORY_FOR_QUERIES`: Memory available to queries (default `5G`)
   - `QLEVER_CACHE_MAX_SIZE`: Query-result cache size (default `2G`)
   - `QLEVER_QUERY_TIMEOUT`: Default query timeout (default `30s`)
   - `QLEVER_UI_PORT`: Published QLever UI port (default `7018`)
   - `QLEVERUI_SECRET_KEY`: Required Django signing key for QLever UI
   - `QLEVERUI_ALLOWED_HOSTS`: Hosts accepted by QLever UI
   - `QLEVERUI_CSRF_TRUSTED_ORIGINS`: Trusted browser origins for QLever UI
   - `QLEVER_UI_BACKEND_URL`: Browser-visible QLever endpoint used by the UI
   - `CIMPLE_FACTORS_API_URL`: CIMPLE Factors API base URL (default `http://localhost:8000`)
   - `PIPELINE_UID`: Host UID used for pipeline-generated files (default `1000`)
   - `PIPELINE_GID`: Host GID used for pipeline-generated files (default `1000`)
   - `POSTGRES_HOST`: Pipeline state database host (default `postgres`)
   - `POSTGRES_BIND_ADDRESS`: Host interface for PostgreSQL (default `127.0.0.1`)
   - `POSTGRES_PORT`: Pipeline state database port (default `5432`)
   - `POSTGRES_DB`: Durable pipeline-state database name (default `climatesense`)
   - `POSTGRES_USER`: Pipeline state database user (default `postgres`)
   - `POSTGRES_PASSWORD`: Pipeline state database password (required)
   - `ANALYTICS_SPARQL_ENDPOINT`: Selected triplestore endpoint for analytics
   - `ANALYTICS_ALLOWED_ORIGINS`: Comma-separated origins permitted to call the analytics API (default `http://localhost:3000`)
   - `ANALYTICS_RESULT_CACHE_TTL`: Analytics query-result cache TTL in seconds (default `300`)
   - `ANALYTICS_SPARQL_TIMEOUT`: SPARQL timeout in seconds for analytics queries (default `20`)
   - `NEXT_PUBLIC_ANALYTICS_API_URL`: Base URL the dashboard uses for the analytics API (default `http://localhost:8000`)
   - `ANALYTICS_API_PORT`: Published port for the analytics API container (default `8000`)
   - `ANALYTICS_UI_PORT`: Published port for the analytics UI container (default `3000`)

3. Start the required services:

   - **Option 1. Virtuoso**

     Start the Virtuoso stack:

     ```bash
     just virtuoso-up
     ```

     Virtuoso only reads RDF files under `/database/data` when `DirsAllowed` in the
     generated `virtuoso.ini` includes it (set via `VIRT_PARAMETERS_DIRSALLOWED` in
     `.env`). This is applied when the `virtuoso_data` volume is first created. For a
     volume created before this setting existed, add `/database/data` to the
     `DirsAllowed` line in the container's `/database/virtuoso.ini` and restart
     `climatesense-virtuoso`.

   - **Option 2. QLever** — start it after the snapshot exists (see step 4).

4. Run the pipeline with minimal configuration to verify the setup:

   ```bash
   docker compose -f docker/docker-compose.yml run --rm pipeline \
     run --config config/minimal.yaml
   ```

   A complete daily snapshot can then be deployed to the selected triplestore:

   ```bash
   just virtuoso-deploy              # Virtuoso
   just qlever-deploy && just qlever-up   # QLever
   ```

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

duplicate_audit:
  similarity_threshold: 0.9
  minimum_similarity_words: 50
  group_batch_size: 100

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
    batch_size: 32
    max_length: 128
    timeout: 30
    rate_limit_delay: 0.1
    max_workers: 1

output:
  output_path: "data/rdf/{DATETIME}/{SOURCE}.nt.gz"
  base_uri: "http://data.climatesense-project.eu"

cache:
  cache_dir: "cache"
  default_ttl_hours: 24.0
```

Deployment is not part of the pipeline: complete RDF snapshots are pushed to the
selected triplestore with `just virtuoso-deploy` or `just qlever-deploy`.

`data/organizations.ttl` is the fixed, manually maintained source of truth for fact-checker identity and metadata. Each entry has an explicit stable ClimateSense IRI, one curated name, one or more website URLs, country-level location, and memberships or parent relationships where applicable. Every processor must provide an organization website, and extracted organizations resolve exclusively by normalized URL.

`data/graphs.ttl` is the curated catalog for the published named graphs.

## Querying pipeline state

PostgreSQL is the pipeline's authoritative working state and must be included in
normal backup and restore procedures. `source_observations` contains the active
normalized source snapshots. `documents`, `document_urls`, `document_text_hashes`,
and `claim_reviews` preserve canonical identity and the random UUIDs used by review
URIs. `document_extractions` and `enrichment_results` contain the current reusable
outcome for each external computation. The filesystem cache stores downloaded source
artifacts and may be recreated.

Processing failures have either a retry time or a permanent status. The next run
reuses current successes, defers failures whose retry time has not arrived, retries
due failures, and retains permanent failures until explicitly forced. The
`processing_results` view exposes extraction and enrichment state through one
read-only shape for analytics.

Semantic settings are part of result identity: Spotlight model, confidence and
support, CIMPLE model versions and maximum input length, and the selected DBpedia
properties. Endpoint URLs, timeouts, retry counts, rate limits, batch sizes, and
worker counts are operational settings and do not invalidate stored results.

Document extraction fetches up to `document_extraction.max_workers` documents in
parallel while allowing only one active request per hostname. Records sharing the
same normalized document URL reuse one fetch. Results are committed after each
bounded pipeline batch, and progress is logged at
`document_extraction.progress_interval_seconds`. Timeouts, temporary connection
failures, HTTP 408/429 and server errors are retried; access blocks and DNS failures
use configurable cooldowns; invalid URLs and HTTP 404/410 responses are retained as
permanent failures. Known bot-challenge pages returned with HTTP 200 are treated as
access blocks rather than document content. If a run is interrupted, already
committed results are reused on the next run.
Use `--skip-extraction` to apply stored successful extractions without fetching
failed or missing documents.

Enrichment work is divided into bounded, independently committed work units.
Spotlight annotates texts with up to `enrichment.dbpedia_spotlight.max_workers`
concurrent requests. CIMPLE sends batches of `enrichment.cimple.batch_size` texts
with up to `enrichment.cimple.max_workers` requests in flight for each model.
Progress identifies the active enricher and reports work-unit completion within the
current review batch. An interrupted run reuses every committed work unit.

Each source is installed in one transaction. A failed download or parse leaves that
source's previous active snapshot intact. Extraction, identity resolution,
enrichment, and RDF projection read fixed-size database batches controlled by
`batch_size`. Exact identity resolution preserves existing source assignments, then
matches documents by organization-scoped URL and text-hash aliases. Fuzzy text
similarity is reserved for the manual duplicate audit.

The exporter writes N-Triples incrementally to one temporary file per graph. Complete
files are externally sorted and deduplicated on disk before atomic replacement, so
the published snapshots contain no repeated statements. An incomplete run preserves
both the existing file and the deployed graph. Export finalization temporarily needs
space for the raw file, external-sort working files, the deduplicated file, and any
previous snapshot. Long-running stages log throughput, ETA, counters, and process
RSS.

### Example SQL Queries

```sql
-- Current reusable coverage and retry state by stage
SELECT stage_name, stage_version, status, COUNT(*) AS total,
       MIN(retry_at) AS next_retry_at
FROM processing_results GROUP BY stage_name, stage_version, status;

-- Current extraction failures by category and HTTP status
SELECT failure_category, http_status, COUNT(*) AS results,
       MIN(retry_at) AS next_retry_at
FROM document_extractions
WHERE status <> 'success'
GROUP BY failure_category, http_status;

-- Highest-confidence identity candidates for offline auditing
SELECT left_review_id, right_review_id, similarity, evidence
FROM duplicate_candidates ORDER BY similarity DESC;
```

Every run reports dependency availability, eligible subjects, stored successes,
deferred and permanent failures, computed outcomes, and missing results for each
external processor. A graph with missing required enrichment results is neither
published to its final output path nor deployed; its existing snapshots remain
untouched and the run is reported as degraded. Spotlight and DBpedia property
processing govern the DBpedia enrichment graph, while enabled CIMPLE models govern
the source graphs that contain their claim-analysis triples.

## Production operations

The backup, restore, outage, and fresh-deployment procedures are documented in the
[production operations runbook](docs/operations.md). In particular, restoring the
PostgreSQL identity tables is what preserves non-deterministic claim-review UUIDs.

## Querying the Knowledge Graph

Once loaded into the selected triplestore, query the knowledge graph using SPARQL:

- **Virtuoso SPARQL Endpoint**: http://localhost:8890/sparql
- **Virtuoso Faceted Browser**: http://localhost:8890/fct
- **QLever SPARQL Endpoint**: http://localhost:7019
- **QLever UI**: http://localhost:7018

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

## Auditing near-duplicate claim reviews

Run the exact audit after identity resolution to find nearly identical review bodies
that still have distinct claim-review resources. Comparisons are limited to reviews
from the same organization that are attached to the same exact claim, keeping each
working set small and semantically relevant. The command replaces the diagnostic
rows in `duplicate_candidates`; it never changes canonical identities or RDF.

```bash
uv run climatesense-kg audit-duplicates --config config/daily.yaml
```

The default score is the containment overlap of normalized five-word shingles. A
score of `0.9` means at least 90% of the smaller review's shingles also occur in the
larger review. Reviews shorter than `duplicate_audit.minimum_similarity_words` are
excluded. Query `duplicate_candidates` as a manual review queue: a match can represent
a duplicate source record, a URL alias, or legitimate syndication. Require human
confirmation before merging or deleting resources.

## Development

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

```bash
# Display help
uv run climatesense-kg --help

# Run minimal pipeline with debug logging
uv run climatesense-kg run --config config/minimal.yaml --debug

# Recompute extraction and enrichment results while using cached source data
uv run climatesense-kg run --config config/daily.yaml --skip-download --force-regenerate

# Restore stored extractions without fetching fact-check documents
uv run climatesense-kg run --config config/daily.yaml --skip-extraction

# Redeploy existing RDF snapshots to the selected triplestore
just virtuoso-deploy

# Build and activate QLever from the latest complete RDF snapshot
just qlever-deploy

# Or select a snapshot explicitly
just qlever-deploy data/rdf/2026-08-15_143734

# Delete recomputable extraction and enrichment results without deleting identities
uv run climatesense-kg flush-processing-results --yes
```

### QLever UI

`just qlever-up` starts the QLever engine and QLever UI.
The UI is available at http://localhost:7018 and persists its Django database in the `qlever_ui_data` Docker volume.

Create an administrator only if you want to customize the UI through http://localhost:7018/admin:

```bash
just qlever-ui-admin
```

## Acknowledgments

This project builds upon the work of the CIMPLE project and reuses components from:

- [CIMPLE Converter](https://github.com/CIMPLE-project/converter)
- [CIMPLE Knowledge Base](https://github.com/CIMPLE-project/knowledge-base)
