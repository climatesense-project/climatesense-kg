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
│    (FastAPI)    │      │   (cache)    │
└────────┬────────┘      └──────────────┘
         │
         │                ┌──────────────┐
         └───────────────▶│   Virtuoso   │
                          │    (RDF)     │
                          └──────────────┘
```

## Metrics Endpoints

### Pipeline Metrics

- `GET /metrics/enrichers/success-rate` - Enricher success rates
- `GET /metrics/enrichers/error-types` - Error type breakdown
- `GET /metrics/enrichers/domain-failures` - Domain-specific failures
- `GET /metrics/enrichers/recent-activity` - Recent enricher activity

### Knowledge Graph Metrics

- `GET /metrics/kg/triple-volume` - Triple count per graph
- `GET /metrics/kg/class-distribution` - RDF class distribution
- `GET /metrics/kg/core-counts` - Core entity counts
- `GET /metrics/kg/enrichment-coverage` - Enrichment coverage stats
- `GET /metrics/kg/entity-types` - Entity type counts
- `GET /metrics/kg/claim-factors` - Claim factor distributions

### Cache Management

- `GET /cache/status` - Cache status overview
- `POST /cache/clear` - Clear cache data
- `POST /cache/refresh` - Refresh cache data

### Health Check

- `GET /health` - Service health status

## Configuration

Environment variables:

- `ANALYTICS_SPARQL_ENDPOINT` - Virtuoso SPARQL endpoint URL
- `ANALYTICS_SPARQL_USER` - Optional SPARQL authentication user
- `ANALYTICS_SPARQL_PASSWORD` - Optional SPARQL authentication password
- `ANALYTICS_ALLOWED_ORIGINS` - CORS allowed origins (comma-separated)
- `ANALYTICS_SPARQL_TIMEOUT` - SPARQL query timeout in seconds (default: 20)
- `POSTGRES_*` - PostgreSQL connection settings

## Development

### Running Locally

```bash
cd services/analytics_api
uvicorn analytics_api.main:app --reload
```
