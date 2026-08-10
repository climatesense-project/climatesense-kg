# Justfile for ClimateSense KG Pipeline
# Install just: https://github.com/casey/just
set dotenv-filename := ".env"
set dotenv-load := true

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

# Start the stack with Virtuoso as the triplestore
virtuoso-up:
    COMPOSE_PROFILES=virtuoso docker compose -f docker/docker-compose.yml up -d

# Build the initial QLever index from historical RDF and vocabularies
qlever-init:
    COMPOSE_PROFILES=qlever-init docker compose -f docker/docker-compose.yml run --rm qlever-index

# Start the stack with QLever as the triplestore
qlever-up:
    COMPOSE_PROFILES=qlever docker compose -f docker/docker-compose.yml up -d

# Create an administrator for the QLever UI
qlever-ui-admin:
    COMPOSE_PROFILES=qlever docker compose -f docker/docker-compose.yml exec qlever-ui-app python manage.py createsuperuser

# Query the QLever SPARQL endpoint
qlever-sparql QUERY:
    @curl -fsS -G \
        --header "Accept: application/sparql-results+json" \
        --data-urlencode "query={{QUERY}}" \
        "http://localhost:${QLEVER_PORT:-7019}"

# Show QLever named-graph statistics
qlever-stats:
    @just qlever-sparql "SELECT ?g (COUNT(*) AS ?triples) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY DESC(?triples)" | \
    jq -r '.results.bindings[] | [.g.value, .triples.value] | @tsv' | \
    awk 'BEGIN {printf "%-80s %10s\n", "Graph", "Triples"; printf "%-80s %10s\n", "-----", "-------"} {printf "%-80s %10s\n", $1, $2}'

# Fold persisted QLever updates into a fresh optimized index, then restart
qlever-rebuild:
    COMPOSE_PROFILES=qlever docker compose -f docker/docker-compose.yml exec -T qlever bash -lc 'qlever rebuild-index --access-token "$$QLEVER_ACCESS_TOKEN" --keep-old-index-dirs newest'
    COMPOSE_PROFILES=qlever docker compose -f docker/docker-compose.yml restart qlever

# ============================================================================
# Semantic Stage State Commands
# ============================================================================

# Flush semantic stage results without changing canonical identities
stage-results-flush:
    @echo "WARNING: This will delete ALL semantic stage results!"
    @read -p "Are you sure? (y/N) " confirm; \
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then \
        cd docker && docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "TRUNCATE TABLE stage_results;"' && \
        echo "✅ Semantic stage results cleared successfully"; \
    else \
        echo "❌ Stage-result flush cancelled"; \
    fi

# Delete results for a specific semantic stage
stage-results-delete STAGE:
    @echo "Deleting PostgreSQL results for stage: {{STAGE}}"
    @cd docker && COUNT=$(docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM stage_results WHERE stage_name = '\''{{STAGE}}'\'';"' | tr -d ' '); \
    if [ "$COUNT" -eq 0 ]; then \
        echo "No results found for stage {{STAGE}}"; \
        exit 0; \
    fi; \
    echo "Found $COUNT results for stage {{STAGE}}"; \
    read -p "Are you sure you want to delete them? (y/N) " confirm; \
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then \
        docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "DELETE FROM stage_results WHERE stage_name = '\''{{STAGE}}'\'';"' && \
        echo "✅ PostgreSQL results deleted for {{STAGE}}"; \
    else \
        echo "❌ Stage-result deletion cancelled"; \
    fi

# List all semantic stages
stage-results-list:
    @echo "=== Semantic Stage Results ==="
    @cd docker && docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT stage_name, stage_version, success, COUNT(*) AS count FROM stage_results GROUP BY stage_name, stage_version, success ORDER BY stage_name, stage_version, success;"' || true

# ============================================================================
# Database Commands
# ============================================================================

db-shell:
    cd docker && docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'

db-backup FILE="${POSTGRES_DB:-db}_`date +%Y%m%d_%H%M%S`.dump.gz":
    @docker compose -f docker/docker-compose.yml exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -Fc "$POSTGRES_DB"' | gzip > "{{FILE}}" && \
    echo "✅ Database backup saved to {{FILE}}"

db-restore FILE:
    @echo "Restoring database from {{FILE}}"
    @read -p "This will overwrite the current database. Are you sure? (y/N) " confirm; \
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then \
        gunzip -c "{{FILE}}" | docker compose -f docker/docker-compose.yml exec -T postgres sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' && \
        echo "✅ Database restored from {{FILE}}"; \
    else \
        echo "❌ Database restore cancelled"; \
    fi

# ============================================================================
# Virtuoso Commands
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
