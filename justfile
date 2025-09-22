# Justfile for ClimateSense KG Pipeline
# Install just: https://github.com/casey/just

# ============================================================================
# Default and Help Commands
# ============================================================================

# Default recipe to display help
default:
    @just --list

# Run the CLI with help
help:
    uv run climatesense-kg --help

# ============================================================================
# Setup and Installation Commands
# ============================================================================

# Install dependencies
install:
    uv sync

# Setup development environment
setup-dev: install
    uv sync --group dev
    @just pre-commit-install

# Install pre-commit hooks
pre-commit-install:
    uv run pre-commit install
    uv run pre-commit install --hook-type commit-msg

# ============================================================================
# Development and Quality Commands
# ============================================================================

# Run code formatting
format:
    uv run ruff format src services
    uv run ruff check --fix src services

# Run all quality checks
check:
    uv run ruff check src services
    uv run ty check

# Run pre-commit on all files
pre-commit-all:
    uv run pre-commit run --all-files

# Run tests
test FILE="":
    #!/usr/bin/env bash
    if [ -n "{{FILE}}" ]; then
        uv run pytest "{{FILE}}" -v
    else
        uv run pytest tests/ -v
    fi

# ============================================================================
# Runtime Commands
# ============================================================================

# Run pipeline with configuration and optional extra arguments
run CONFIG *ARGS="":
    uv run climatesense-kg run --config {{CONFIG}} {{ARGS}}

# ============================================================================
# Docker Commands
# ============================================================================

# Build Docker images
docker-build:
    docker compose -f docker/docker-compose.yml build

# Run the daily pipeline in Docker
docker-daily-run:
    docker compose -f docker/docker-compose.yml run --build pipeline run --config config/daily.yaml

docker-minimal-run:
    docker compose -f docker/docker-compose.yml run --build -v ./samples:/app/samples pipeline run --config config/minimal.yaml

# ============================================================================
# Cache Commands
# ============================================================================

# Flush entire PostgreSQL cache
cache-flush:
    @echo "WARNING: This will delete ALL cache data in PostgreSQL!"
    @read -p "Are you sure? (y/N) " confirm; \
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then \
        cd docker && docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "TRUNCATE TABLE cache_entries;"' && \
        echo "✅ PostgreSQL cache cleared successfully"; \
    else \
        echo "❌ Cache flush cancelled"; \
    fi

# Delete cache entries for a specific step
cache-delete STEP:
    @echo "Deleting PostgreSQL cache for step: {{STEP}}"
    @cd docker && COUNT=$(docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM cache_entries WHERE step = '\''{{STEP}}'\'';"' | tr -d ' '); \
    if [ "$COUNT" -eq 0 ]; then \
        echo "No cache entries found for step {{STEP}}"; \
        exit 0; \
    fi; \
    echo "Found $COUNT cache entries for step {{STEP}}"; \
    read -p "Are you sure you want to delete them? (y/N) " confirm; \
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then \
        docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "DELETE FROM cache_entries WHERE step = '\''{{STEP}}'\'';"' && \
        echo "✅ PostgreSQL cache deleted for {{STEP}}"; \
    else \
        echo "❌ Cache deletion cancelled"; \
    fi

# List all cache steps
cache-list:
    @echo "=== Cached Steps ==="
    @cd docker && docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT step, COUNT(*) as count FROM cache_entries GROUP BY step ORDER BY count DESC;"' || true

# ============================================================================
# Database and Virtuoso Commands
# ============================================================================

# Connect to Virtuoso SQL interface
isql:
    cd docker && docker compose exec virtuoso isql localhost:1111 dba

# Run SPARQL query against Virtuoso
sparql QUERY:
    @cd docker && \
    docker compose exec -T virtuoso sh -c '\
        tmp=$(mktemp) || exit 1; \
        printf "%s" "$1" > "$tmp" && \
        wget -q -O - \
            --header="Content-Type: application/sparql-query" \
            --header="Accept: application/json" \
            --post-file="$tmp" \
            "http://localhost:8890/sparql"; \
        rc=$?; \
        rm -f "$tmp"; \
        exit $rc' -- "{{QUERY}}"

# Show Virtuoso graph statistics
virtuoso-stats:
    @cd docker && \
    docker compose exec -T virtuoso sh -c '\
        tmp=$(mktemp) || exit 1; \
        printf "%s" "$1" > "$tmp" && \
        wget -q -O - \
            --header="Content-Type: application/sparql-query" \
            --header="Accept: application/json" \
            --post-file="$tmp" \
            "http://localhost:8890/sparql"; \
        rc=$?; \
        rm -f "$tmp"; \
        exit $rc' -- "SELECT ?g (COUNT(?s) AS ?triples) WHERE { GRAPH ?g { ?s a [] } } GROUP BY ?g ORDER BY DESC(?triples)" | \
    jq -r '.results.bindings[] | [.g.value, .triples.value] | @tsv' | \
    awk 'BEGIN {printf "%-80s %10s\n", "Graph", "Triples"; printf "%-80s %10s\n", "-----", "-------"} {printf "%-80s %10s\n", $1, $2}' \
    || echo "No data found or Virtuoso not running."
