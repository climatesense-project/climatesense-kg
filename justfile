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
    uv run ruff format src
    uv run ruff check --fix src

# Run all quality checks
check:
    uv run ruff check src
    uv run ty check

# Run pre-commit on all files
pre-commit-all:
    uv run pre-commit run --all-files

# Run tests
test:
    uv run pytest tests/ -v

# ============================================================================
# Runtime Commands
# ============================================================================

# Run pipeline with configuration
run CONFIG:
    uv run climatesense-kg run --config {{CONFIG}}

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
