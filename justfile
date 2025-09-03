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
    poetry run climatesense-kg --help

# ============================================================================
# Setup and Installation Commands
# ============================================================================

# Install dependencies
install:
    poetry install

# Setup development environment
setup-dev: install
    poetry install --with dev
    poetry run mypy --install-types --non-interactive src
    @just pre-commit-install

# Install pre-commit hooks
pre-commit-install:
    poetry run pre-commit install
    poetry run pre-commit install --hook-type commit-msg

# ============================================================================
# Development and Quality Commands
# ============================================================================

# Run code formatting
format:
    poetry run ruff format src
    poetry run ruff check --fix src

# Run all quality checks
check:
    poetry run ruff check src
    poetry run mypy src

# Run pre-commit on all files
pre-commit-all:
    poetry run pre-commit run --all-files

# ============================================================================
# Runtime Commands
# ============================================================================

# Run pipeline with configuration
run CONFIG:
    poetry run climatesense-kg run --config {{CONFIG}}

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
# Database and Virtuoso Commands
# ============================================================================

# Connect to Virtuoso SQL interface
isql:
    cd docker && docker compose exec virtuoso isql localhost:1111 dba

# Run SPARQL query against Virtuoso
sparql QUERY:
    curl -X POST \
        -H "Content-Type: application/sparql-query" \
        -H "Accept: application/json" \
        -d "{{QUERY}}" \
        "http://localhost:8890/sparql"

# Show Virtuoso graph statistics
virtuoso-stats:
    @curl -s -X POST \
        -H "Content-Type: application/sparql-query" \
        -H "Accept: application/json" \
        -d "SELECT ?g (COUNT(*) as ?triples) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY DESC(?triples)" \
        "http://localhost:8890/sparql" | \
    jq -r '["Graph", "Triples"], ["-----", "-------"], (.results.bindings[] | [ .g.value, .triples.value ]) | @tsv' | \
    column -t -s $'\t' || echo "No data found or Virtuoso not running."
