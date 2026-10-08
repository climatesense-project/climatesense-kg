#!/bin/sh

set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
data_dir="$repo_root/data"
compose_file="$repo_root/docker/docker-compose.yml"
qlever_compose_file="$repo_root/docker/docker-compose.qlever.yml"
template="$repo_root/docker/qlever/Qleverfile"
snapshot_path=${1:-}

if [ -z "$snapshot_path" ]; then
    snapshot_path=$(find "$data_dir/rdf" -mindepth 2 -maxdepth 2 -type f -name .complete -print | sort | tail -n 1)
    snapshot_path=${snapshot_path%/*}
elif [ ! -d "$snapshot_path" ] && [ -d "$repo_root/$snapshot_path" ]; then
    snapshot_path="$repo_root/$snapshot_path"
fi

if [ -z "$snapshot_path" ] || [ ! -d "$snapshot_path" ]; then
    echo "No completed RDF snapshot run directory found. Pass its path explicitly." >&2
    exit 1
fi

snapshot_dir=$(CDPATH= cd -- "$snapshot_path" && pwd)
snapshot_name=$(basename "$snapshot_dir")
snapshot_id=$snapshot_name

case "$snapshot_name" in
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *)
        echo "Snapshot must be a run directory named YYYY-MM-DD_HHMMSS: $snapshot_dir" >&2
        exit 1
        ;;
esac

case "$snapshot_dir/" in
    "$data_dir"/*) snapshot_relative=${snapshot_dir#"$data_dir"/} ;;
    *)
        echo "Snapshot must be stored below $data_dir" >&2
        exit 1
        ;;
esac

if [ ! -f "$snapshot_dir/.complete" ]; then
    echo "Snapshot is not marked complete: $snapshot_dir/.complete" >&2
    exit 1
fi

for artifact in "$snapshot_dir"/*.nt.gz; do
    if [ ! -s "$artifact" ]; then
        echo "Incomplete snapshot; empty file: $artifact" >&2
        exit 1
    fi
done

for catalog in "$data_dir/organizations.ttl" "$data_dir/graphs.ttl"; do
    if [ ! -s "$catalog" ]; then
        echo "Missing or empty catalog: $catalog" >&2
        exit 1
    fi
done

if ! find "$data_dir/vocabularies" -type f -name '*.ttl' -size +0c -print -quit | grep -q .; then
    echo "No non-empty Turtle vocabularies found in $data_dir/vocabularies" >&2
    exit 1
fi

temporary_dir=$(mktemp -d "${TMPDIR:-/tmp}/climatesense-qlever.XXXXXX")
trap 'rm -rf "$temporary_dir"' EXIT HUP INT TERM

container_snapshot_dir="../source/$snapshot_relative"

graph_base_uri="http://data.climatesense-project.eu/graph/"
snapshot_input_json=
for artifact in "$snapshot_dir"/*.nt.gz; do
    graph=$(basename "$artifact" .nt.gz)
    snapshot_input_json="$snapshot_input_json{ \"cmd\": \"gunzip -c {}\", \"format\": \"nt\", \"graph\": \"${graph_base_uri}${graph}\", \"for-each\": \"\${data:SNAPSHOT_DIRECTORY}/${graph}.nt.gz\", \"parallel\": \"true\" }, "
done

sed \
    -e "s|__SNAPSHOT_DIRECTORY__|$container_snapshot_dir|g" \
    -e "s|__SNAPSHOT_INPUT_JSON__|$snapshot_input_json|g" \
    "$template" >"$temporary_dir/Qleverfile"

compose() {
    docker compose -f "$compose_file" -f "$qlever_compose_file" "$@"
}

volume_admin() {
    compose run --rm --no-deps qlever-volume-admin sh -eu -c "$1"
}

wait_for_qlever() {
    attempts=0
    while [ "$attempts" -lt 30 ]; do
        if compose exec -T qlever curl -fsS -G \
            -H 'Accept: application/sparql-results+json' \
            --data-urlencode 'query=ASK { ?s ?p ?o }' \
            http://localhost:7019 >/dev/null 2>&1; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 2
    done
    return 1
}

echo "Building QLever index from snapshot $snapshot_id"
QLEVER_FORCE_REBUILD=1 QLEVER_DEPLOY_QLEVERFILE="$temporary_dir/Qleverfile" \
    compose run --rm qlever-index
compose stop qlever >/dev/null 2>&1 || true

compose up -d --force-recreate qlever
if wait_for_qlever; then
    echo "QLever is serving snapshot $snapshot_id"
    exit 0
fi

echo "QLever did not become ready; restoring the previous index" >&2
compose logs --tail 100 qlever >&2 || true
compose stop qlever >/dev/null 2>&1 || true

if ! volume_admin '
    test -d /data/index-previous
    rm -rf /data/index-failed
    mv /data/index-current /data/index-failed
    mv /data/index-previous /data/index-current
    rm -rf /data/index-failed
'; then
    echo "No previous index is available for rollback." >&2
    exit 1
fi

compose up -d --force-recreate qlever
if wait_for_qlever; then
    echo "Previous QLever index restored." >&2
else
    echo "Rollback completed, but QLever still did not become ready." >&2
    compose logs --tail 100 qlever >&2 || true
fi
exit 1
