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
# Cache Commands
# ============================================================================

# Flush entire Redis cache
cache-flush:
    @echo "WARNING: This will delete ALL cache data in Redis!"
    @read -p "Are you sure? (y/N) " confirm; \
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then \
        @cd docker && docker compose exec redis redis-cli FLUSHDB && \
        echo "✅ Redis cache cleared successfully"; \
    else \
        echo "❌ Cache flush cancelled"; \
    fi

# Clear cache for specific enricher step (e.g., enricher.dbpedia_spotlight)
cache-clear STEP:
    @echo "Clearing cache for step: {{STEP}}"
    @cd docker && COUNT=$(docker compose exec redis redis-cli KEYS "*:climatesense:{{STEP}}:*" | wc -l | tr -d ' '); \
    if [ "$COUNT" -eq 0 ]; then \
        echo "No cache entries found for step {{STEP}}"; \
        exit 0; \
    fi; \
    echo "Found $COUNT cache entries for step {{STEP}}"; \
    read -p "Are you sure you want to delete them? (y/N) " confirm; \
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then \
        @docker compose exec redis redis-cli EVAL "local keys = redis.call('KEYS', ARGV[1]) for i=1,#keys,5000 do redis.call('DEL', unpack(keys, i, math.min(i+4999, #keys))) end return #keys" 0 "*:climatesense:{{STEP}}:*" && \
        echo "✅ Cache cleared for {{STEP}}"; \
    else \
        echo "❌ Cache clear cancelled"; \
    fi

# List all cache steps
cache-list:
    @echo "=== Cached Steps ==="
    @cd docker && docker compose exec redis redis-cli KEYS "*:climatesense:*" | sed 's/.*:climatesense:\([^:]*\):.*/\1/' | sort | uniq -c | sort -nr || true

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
