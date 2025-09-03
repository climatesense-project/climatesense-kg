# ClimateSense KG Pipeline

[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://python.org)
[![Poetry](https://img.shields.io/badge/dependency-poetry-blue)](https://python-poetry.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> The ClimateSense KG is a continuously updated knowledge graph that integrates climate fact-checking data from multiple sources to combat climate misinformation. It links information from fact-checking organizations with enriched data, giving researchers a more comprehensive view of the problem.

## 🔍 Overview

![Pipeline Architecture](docs/pipeline.svg)

### Key Features

- Multi-source ingestion from major climate fact-checking organizations
  - [EuroClimateCheck](https://github.com/climatesense-project/euroclimatecheck-scraper)
  - [MisinfoMe](https://github.com/MartinoMensio/claimreview-data)
  - [DeFacto](https://defacto-observatoire.fr/Fact-checks/)
  - [DBKF](https://dbkf.ontotext.com/)
- Data enrichment with:
  - Text extraction from URLs using [trafilatura](https://trafilatura.readthedocs.io/)
  - Entity linking using [DBpedia Spotlight](https://www.dbpedia-spotlight.org/)
  - Factors classification using fine-tuned BERT models
- RDF output using [Schema.org](https://schema.org/) and [CIMPLE ontology](https://github.com/CIMPLE-project/knowledge-base)
- Triple store deployment supporting Virtuoso
- [YAML-based configuration](#configuration)

### Documentation & Resources

- [URI design patterns](docs/URI-patterns.md) and [RDF namespaces](docs/URI-patterns.md#rdf-namespace-declarations)
- Public SPARQL endpoint: https://data.climatesense-project.eu/sparql

## 📋 Prerequisites

- Python 3.11+
- [Poetry](https://python-poetry.org/docs/#installation) (for dependency management)
- [just](https://github.com/casey/just) (for task automation)
- [Docker & Docker Compose](https://docs.docker.com/get-docker/) (for Docker setup)

## Table of Contents

- [Overview](#-overview)
  - [Key Features](#key-features)
  - [Documentation & Resources](#documentation--resources)
- [Prerequisites](#-prerequisites)
- [Quick Start](#quick-start)
- [Docker Setup](#docker-setup)
- [Configuration](#configuration)
- [Output](#-output)
- [Querying the Knowledge Graph](#querying-the-knowledge-graph)
  - [Example SPARQL Queries](#example-sparql-queries)
- [Development](#development)
  - [Setup](#setup)
  - [Common Tasks](#common-tasks)
  - [CLI Usage](#cli-usage)
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

**Initial Setup:**

1. Clone the repository and navigate to the docker directory:

   ```bash
   git clone https://github.com/climatesense-project/climatesense-kg.git
   cd climatesense-kg/docker
   ```

2. Copy and configure environment variables:

   ```bash
   cp .env.example .env
   ```

   Edit `.env` to configure:
   - `GITHUB_TOKEN`: Your GitHub token for private repositories
   - `VIRTUOSO_PORT`: Port for Virtuoso (default: 8890)
   - `ISQL_PORT`: Port for Virtuoso ISQL interface (default: 8080)

3. Start the services:

   ```bash
   docker compose up -d
   ```

4. Run the pipeline:
   ```bash
   docker compose run --rm pipeline run -c config/minimal.yaml
   ```

## Configuration

The pipeline uses YAML-based configuration. Example config:

```yaml
data_sources:
  - name: "claimreview_sample"
    type: "misinfome"
    input_path: "samples/misinfome-data"
  - name: "euroclimatecheck_sample"
    type: "euroclimatecheck"
    input_path: "samples/euroclimatecheck-data"

enrichment:
  url_text_extraction:
    enabled: true
    rate_limit_delay: 0.5
    timeout: 15
    max_retries: 2

  dbpedia_spotlight:
    enabled: true
    api_url: "https://api.dbpedia-spotlight.org/en/annotate"
    confidence: 0.6
    support: 30
    timeout: 20
    rate_limit_delay: 0.2

  bert_factors:
    enabled: true
    models_path: "data/cimple-factors-models"
    batch_size: 64
    device: "auto"
    max_length: 128

output:
  format: "turtle"
  output_path: "data/rdf/{DATE}/{SOURCE}.ttl"
  base_uri: "http://data.climatesense-project.eu"
  uri_cache_path: "cache/processed_uris.txt"
```

## Querying the Knowledge Graph

Once loaded into Virtuoso, query the knowledge graph using SPARQL:

- **SPARQL Endpoint**: http://localhost:8890/sparql
- **Faceted Browser**: http://localhost:8890/fct

### Example SPARQL Queries

**Find all climate claims:**

```sparql
PREFIX schema: <http://schema.org/>
SELECT ?claim ?text ?rating
WHERE {
  ?claim a schema:ClaimReview ;
         schema:claimReviewed ?text ;
         schema:reviewRating ?rating .
}
LIMIT 10
```

**Find claims by fact-checking organization:**

```sparql
PREFIX schema: <http://schema.org/>
SELECT ?claim ?author
WHERE {
  ?claim a schema:ClaimReview ;
         schema:author ?author .
}
LIMIT 10
```

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
poetry run climatesense-kg --help
poetry run climatesense-kg run --config config/minimal.yaml

# Force redeployment even when no RDF changes are detected
poetry run climatesense-kg run --config config/minimal.yaml --force-deployment
```

## Acknowledgments

This project builds upon the work of the CIMPLE project and reuses components from:

- [CIMPLE Converter](https://github.com/CIMPLE-project/converter)
- [CIMPLE Knowledge Base](https://github.com/CIMPLE-project/knowledge-base)
