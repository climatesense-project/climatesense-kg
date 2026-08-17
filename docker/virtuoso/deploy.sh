#!/bin/sh
#
# Redeploy every ClimateSense named graph into Virtuoso from one complete RDF
# snapshot.
#
# Usage: deploy.sh [SNAPSHOT]
#   SNAPSHOT   Explicit run directory (data/rdf/YYYY-MM-DD_HHMMSS); defaults to
#              the most recent run directory below data/rdf.

set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
data_dir="$repo_root/data"
compose_file="$repo_root/docker/docker-compose.yml"
snapshot_arg=${1:-}
virtuoso_password=${VIRTUOSO_PASSWORD:-}
container_data_dir="/database/data"
graph_template="http://data.climatesense-project.eu/graph/{SOURCE}"

graphs="claimreviewdata euroclimatecheck defacto dbkf desmog climafacts climate-fever dbpedia-enricher"

if [ -z "$virtuoso_password" ]; then
    echo "VIRTUOSO_PASSWORD must be set." >&2
    exit 1
fi

# --- Select and validate the snapshot ---------------------------------------

if [ -z "$snapshot_arg" ]; then
    snapshot_path=$(find "$data_dir/rdf" -mindepth 1 -maxdepth 1 -type d ! -name '.*' -print | sort | tail -n 1)
elif [ ! -d "$snapshot_arg" ] && [ -d "$repo_root/$snapshot_arg" ]; then
    snapshot_path="$repo_root/$snapshot_arg"
else
    snapshot_path=$snapshot_arg
fi

if [ -z "$snapshot_path" ] || [ ! -d "$snapshot_path" ]; then
    echo "No RDF snapshot run directory found. Pass its path explicitly." >&2
    exit 1
fi

snapshot_dir=$(CDPATH= cd -- "$snapshot_path" && pwd)
snapshot_name=$(basename "$snapshot_dir")

case "$snapshot_name" in
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *)
        echo "Snapshot must be a run directory named YYYY-MM-DD_HHMMSS: $snapshot_dir" >&2
        exit 1
        ;;
esac

for graph in $graphs; do
    artifact="$snapshot_dir/${graph}.nt.gz"
    if [ ! -s "$artifact" ]; then
        echo "Incomplete snapshot; missing or empty file: $artifact" >&2
        exit 1
    fi
done

for catalog in "$data_dir/organizations.ttl" "$data_dir/graphs.ttl"; do
    if [ ! -s "$catalog" ]; then
        echo "Missing or empty catalog: $catalog" >&2
        exit 1
    fi
done

# --- Helpers -----------------------------------------------------------------

# Double single quotes for use inside a Virtuoso SQL literal.
sqlescape() {
    printf '%s' "$1" | sed "s/'/''/g"
}

# Expand the graph name into its named-graph URI.
graph_uri() {
    printf '%s' "$graph_template" | sed "s|{SOURCE}|$1|"
}

# Map a host path below $data_dir to the matching path inside the container.
container_path() {
    case "$1/" in
        "$data_dir"/*) printf '%s/%s' "$container_data_dir" "${1#"$data_dir"/}" ;;
        *) echo "Path must be stored below $data_dir: $1" >&2; exit 1 ;;
    esac
}

# Run one SQL batch through isql, failing loudly if Virtuoso reports an error.
# isql itself exits 0 even when a statement fails, so the output is scanned for
# the "*** Error" marker that Virtuoso emits for failed statements.
isql() {
    if output=$(docker compose -f "$compose_file" exec -T virtuoso \
        isql localhost:1111 dba "$virtuoso_password" 2>&1); then
        rc=0
    else
        rc=$?
    fi
    printf '%s\n' "$output"
    if [ "$rc" -ne 0 ] || printf '%s\n' "$output" | grep -Fq '*** Error'; then
        return 1
    fi
}

# Replace one named graph with a full snapshot of one RDF file.
deploy_file() {
    host_path=$1
    graph_name=$2

    container_file=$(container_path "$host_path")
    container_dir=$(dirname "$container_file")
    file_name=$(basename "$container_file")
    uri=$(graph_uri "$graph_name")

    echo "Replacing <$uri> with $host_path"
    if ! isql <<SQL
log_enable(2);
SPARQL CLEAR SILENT GRAPH <$uri>;
delete from DB.DBA.LOAD_LIST where LL_FILE = '$(sqlescape "$container_file")';
ld_dir('$(sqlescape "$container_dir")', '$(sqlescape "$file_name")', '$(sqlescape "$uri")');
rdf_loader_run();
checkpoint;
log_enable(1);
SQL
    then
        echo "Deployment failed for $host_path" >&2
        return 1
    fi
}

wait_for_virtuoso() {
    attempts=0
    while [ "$attempts" -lt 30 ]; do
        if docker compose -f "$compose_file" exec -T virtuoso sh -c \
            'wget -q -O - http://localhost:8890/sparql >/dev/null 2>&1'; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 2
    done
    return 1
}

# --- Deploy -------------------------------------------------------------------

echo "Deploying Virtuoso graphs from snapshot $snapshot_name"
COMPOSE_PROFILES=virtuoso docker compose -f "$compose_file" up -d

for graph in $graphs; do
    deploy_file "$snapshot_dir/${graph}.nt.gz" "$graph"
done

deploy_file "$data_dir/graphs.ttl" catalog
deploy_file "$data_dir/organizations.ttl" organizations

if wait_for_virtuoso; then
    echo "Virtuoso is serving snapshot $snapshot_name"
    exit 0
fi

echo "Virtuoso did not become ready after deployment" >&2
exit 1
