#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ClimateSense KG nightly deploy pipeline
# ============================================================
# Intended to be run nightly from cron. Environment-specific
# values (project directory, healthcheck URL, lock file, log
# directory) are read from ops/deploy.env, which is not tracked
# by git. Copy ops/deploy.env.example to ops/deploy.env and
# adjust the values.
# ============================================================

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
PROJECT_DIR=${PROJECT_DIR:-$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)}
DEPLOY_ENV_FILE=${DEPLOY_ENV_FILE:-"$SCRIPT_DIR/deploy.env"}

if [[ -f "$DEPLOY_ENV_FILE" ]]; then
	# shellcheck disable=SC1090
	source "$DEPLOY_ENV_FILE"
fi

TRIPLESTORE=${TRIPLESTORE:-qlever}
CONFIG_FILE=${CONFIG_FILE:-config/daily.yaml}
LOCK_FILE=${LOCK_FILE:-/tmp/climatesense_deploy.lock}
HEALTHCHECK_BASE_URL=${HEALTHCHECK_BASE_URL:?Set HEALTHCHECK_BASE_URL in $DEPLOY_ENV_FILE}
LOG_DIR=${LOG_DIR:-"$PROJECT_DIR/logs"}
RUN_TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
LOG_FILE="$LOG_DIR/daily_$RUN_TIMESTAMP.log"

mkdir -p "$LOG_DIR"

log() {
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

case "$TRIPLESTORE" in
	qlever)
		COMPOSE_FILES="docker/docker-compose.yml docker/docker-compose.qlever.yml"
		DEPLOY_SCRIPT="docker/qlever/deploy-index.sh"
		;;
	virtuoso)
		COMPOSE_FILES="docker/docker-compose.yml docker/docker-compose.virtuoso.yml"
		DEPLOY_SCRIPT="docker/virtuoso/deploy.sh"
		;;
	*)
		log "Error: TRIPLESTORE must be 'qlever' or 'virtuoso', got '$TRIPLESTORE'"
		exit 1
		;;
esac

compose() {
	local args=()
	for f in $COMPOSE_FILES; do
		args+=(-f "$f")
	done
	docker compose "${args[@]}" "$@"
}

notify_healthcheck() {
	local endpoint="${1:-}"
	local max_attempts=3
	local attempt=1
	local url="$HEALTHCHECK_BASE_URL"

	if [[ -n "$endpoint" ]]; then
		url="$url/$endpoint"
	fi

	while [[ $attempt -le $max_attempts ]]; do
		if curl -fsS -m 10 --retry 5 "$url" >/dev/null 2>&1; then
			log "Healthcheck notification sent: ${endpoint:-root}"
			return 0
		fi
		log "Healthcheck notification attempt $attempt failed for: ${endpoint:-root}"
		((attempt++))
		sleep 2
	done

	log "Failed to notify healthcheck system after $max_attempts attempts"
	return 1
}

cleanup() {
	local exit_code=$?

	if [[ $exit_code -ne 0 ]]; then
		log "Script failed with exit code $exit_code"
	fi

	notify_healthcheck "$exit_code" || true

	exec 200>&-
	rm -f "$LOCK_FILE"
}

trap cleanup EXIT

exec 200>"$LOCK_FILE"
if ! flock -n 200; then
	log "Another instance is already running"
	exit 1
fi

log "Starting ClimateSense deployment process"

log "Validating environment..."

if [[ ! -d "$PROJECT_DIR" ]]; then
	log "Error: Project directory $PROJECT_DIR does not exist"
	exit 1
fi

if [[ ! -w "$PROJECT_DIR" ]]; then
	log "Error: No write permission to $PROJECT_DIR"
	exit 1
fi

log "Notifying healthcheck system that deployment has started"
notify_healthcheck "start"

log "Navigating to project directory: $PROJECT_DIR"
cd "$PROJECT_DIR"

for f in $COMPOSE_FILES; do
	if [[ ! -f "$f" ]]; then
		log "Error: Docker compose file not found: $f"
		exit 1
	fi
done

if [[ ! -f "$CONFIG_FILE" ]]; then
	log "Error: Configuration file not found: $CONFIG_FILE"
	exit 1
fi

log "Updating repository..."

if ! git pull --ff-only; then
	log "Error: Git pull failed. This might be due to conflicts or network issues."
	log "You may need to manually resolve conflicts or check network connectivity."
	exit 1
fi

NEW_COMMIT=$(git rev-parse HEAD)
log "Updated to commit: $NEW_COMMIT"

log "Building Docker containers..."
if ! compose build; then
	log "Error: Failed to start Docker containers"
	exit 1
fi

log "Running daily pipeline..."
if ! compose run --build pipeline run -c "$CONFIG_FILE"; then
	log "Error: Pipeline execution failed"
	exit 1
fi

log "Pipeline execution completed successfully"

log "Deploying new RDF snapshot to $TRIPLESTORE..."
if ! ./"$DEPLOY_SCRIPT"; then
	log "Error: $TRIPLESTORE deployment failed"
	exit 1
fi
log "$TRIPLESTORE deployment updated successfully"

log "Starting $TRIPLESTORE stack..."
if ! compose up -d; then
	log "Error: $TRIPLESTORE stack start failed"
	exit 1
fi
log "$TRIPLESTORE stack started"

log "Waiting for analytics API to serve SPARQL-backed metrics..."
READINESS_ATTEMPTS=15
READINESS_POLL_TIMEOUT=120
readiness_attempt=1
readiness_ok=0
while [[ $readiness_attempt -le $READINESS_ATTEMPTS ]]; do
	if compose exec -T analytics-api python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/metrics/kg/triple-volume', timeout=$READINESS_POLL_TIMEOUT).read()" >/dev/null 2>&1; then
		readiness_ok=1
		break
	fi
	log "Analytics API not ready yet (attempt $readiness_attempt/$READINESS_ATTEMPTS)"
	readiness_attempt=$((readiness_attempt + 1))
	sleep 10
done

if [[ $readiness_ok -ne 1 ]]; then
	log "Warning: analytics API did not become ready before cache refresh"
fi

log "Refreshing analytics API cache..."
if refresh_output=$(compose exec -T analytics-api python -m analytics_api.scripts.refresh_cache 2>&1); then
	refresh_status=0
else
	refresh_status=$?
fi

while IFS= read -r line; do
	log "refresh-cache: $line"
done <<<"$refresh_output"

if [[ $refresh_status -eq 0 ]]; then
	log "Analytics API cache refreshed successfully"
else
	log "Warning: cache refresh failed with exit code $refresh_status; caches will warm on demand"
fi

log "Deployment completed successfully"
log "ClimateSense deployment process finished successfully"
log "Current commit: $NEW_COMMIT"
