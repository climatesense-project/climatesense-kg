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
# Virtuoso Commands
# ============================================================================

# Start the stack with Virtuoso as the triplestore
virtuoso-up:
    docker compose -f docker/docker-compose.yml -f docker/docker-compose.virtuoso.yml up -d

# Redeploy all Virtuoso graphs from the latest (or given) RDF snapshot
virtuoso-deploy SNAPSHOT="":
    ./docker/virtuoso/deploy.sh "{{SNAPSHOT}}"

# Query the Virtuoso SPARQL endpoint
virtuoso-sparql QUERY:
    @curl -fsS -G \
        --header "Accept: application/sparql-results+json" \
        --data-urlencode "query={{QUERY}}" \
        "http://localhost:${VIRTUOSO_PORT:-8890}/sparql"

# Show Virtuoso named-graph statistics
virtuoso-stats:
    @just virtuoso-sparql "SELECT ?g (COUNT(?s) AS ?triples) WHERE { GRAPH ?g { ?s a [] } } GROUP BY ?g ORDER BY DESC(?triples)" | \
    jq -r '.results.bindings[] | [.g.value, .triples.value] | @tsv' | \
    awk 'BEGIN {printf "%-80s %10s\n", "Graph", "Triples"; printf "%-80s %10s\n", "-----", "-------"} {printf "%-80s %10s\n", $1, $2}'

# Open an interactive Virtuoso ISQL shell
virtuoso-isql:
    cd docker && docker compose -f docker-compose.yml -f docker-compose.virtuoso.yml exec -T virtuoso isql localhost:1111 dba "$VIRTUOSO_PASSWORD"

# ============================================================================
# QLever Commands
# ============================================================================

# Start the stack with QLever as the triplestore
qlever-up:
    docker compose -f docker/docker-compose.yml -f docker/docker-compose.qlever.yml up -d

# Build and activate a complete QLever index. Defaults to the latest RDF snapshot.
qlever-deploy SNAPSHOT="":
    ./docker/qlever/deploy-index.sh "{{SNAPSHOT}}"

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
        docker compose -f docker/docker-compose.yml exec -T postgres sh -c 'dropdb -U "$POSTGRES_USER" --if-exists "$POSTGRES_DB" && createdb -U "$POSTGRES_USER" "$POSTGRES_DB"' && \
        gunzip -c "{{FILE}}" | docker compose -f docker/docker-compose.yml exec -T postgres sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB"' && \
        echo "✅ Database restored from {{FILE}}"; \
    else \
        echo "❌ Database restore cancelled"; \
    fi
