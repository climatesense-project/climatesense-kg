# Analytics API

FastAPI-based analytics service for the ClimateSense Knowledge Graph project.

## Overview

The Analytics API provides metrics and statistics about the knowledge graph and pipeline processing. It exposes endpoints for querying both SQL (pipeline metrics) and SPARQL (knowledge graph metrics) data sources.

## Architecture

```
┌─────────────────┐
│  Analytics UI   │
│   (Next.js)     │
└────────┬────────┘
         │ HTTP
         ▼
┌─────────────────┐      ┌──────────────┐
│  Analytics API  │─────▶│  PostgreSQL  │
│    (FastAPI)    │      │  (pipeline)  │
└────────┬────────┘      └──────────────┘
         │
         │                ┌───────────────┐
         └───────────────▶│  Triplestore  │
                          │     (RDF)     │
                          └───────────────┘
```

## Metrics Endpoints

### Pipeline Metrics

- `GET /metrics/stages/success-rate` - Success rates by semantic stage version
- `GET /metrics/stages/error-types` - Stage error type breakdown
- `GET /metrics/stages/domain-failures` - Document extraction failures by domain
- `GET /metrics/stages/recent-activity` - Recent semantic-stage activity

### Knowledge Graph Metrics

- `GET /metrics/kg/triple-volume` - Triple count per graph
- `GET /metrics/kg/class-distribution` - RDF class distribution
- `GET /metrics/kg/core-counts` - Core entity counts
- `GET /metrics/kg/enrichment-coverage` - Enrichment coverage stats
- `GET /metrics/kg/entity-types` - Entity type counts
- `GET /metrics/kg/claim-factors` - Claim factor distributions

### Analytics result cache management

- `GET /cache/status` - Cache status overview
- `POST /cache/clear` - Clear cache data
- `POST /cache/refresh` - Refresh cache data

### Health Check

- `GET /health` - Service health status

## Configuration

Environment variables:

- `ANALYTICS_SPARQL_ENDPOINT` - Selected triplestore SPARQL endpoint URL
- `ANALYTICS_SPARQL_USER` - Optional SPARQL authentication user
- `ANALYTICS_SPARQL_PASSWORD` - Optional SPARQL authentication password
- `ANALYTICS_ALLOWED_ORIGINS` - CORS allowed origins (comma-separated)
- `ANALYTICS_SPARQL_TIMEOUT` - SPARQL query timeout in seconds (default: 20)
- `POSTGRES_*` - Durable pipeline-state PostgreSQL connection settings
- `ANALYTICS_RESULT_CACHE_TTL` - Analytics query-result cache TTL in seconds

Pipeline activity endpoints read immutable rows from `stage_result_attempts`, so a
failure remains visible after a later retry succeeds.

Use `http://virtuoso:8890/sparql` for Virtuoso or `http://qlever:7019` for QLever.

## Development

### Running Locally

```bash
cd services/analytics_api
uvicorn analytics_api.main:app --reload
```
