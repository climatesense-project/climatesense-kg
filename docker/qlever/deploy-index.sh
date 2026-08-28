#!/bin/sh

set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
data_dir="$repo_root/data"
compose_file="$repo_root/docker/docker-compose.yml"
qlever_compose_file="$repo_root/docker/docker-compose.qlever.yml"
template="$repo_root/docker/qlever/Qleverfile"
snapshot_path=${1:-}
qlever_uid=${QLEVER_UID:-999}
qlever_gid=${QLEVER_GID:-999}

case "$qlever_uid:$qlever_gid" in
    *[!0-9:]*)
        echo "QLEVER_UID and QLEVER_GID must be numeric." >&2
        exit 1
        ;;
esac

if [ -z "$snapshot_path" ]; then
    snapshot_path=$(find "$data_dir/rdf" -mindepth 1 -maxdepth 1 -type d ! -name '.*' -print | sort | tail -n 1)
elif [ ! -d "$snapshot_path" ] && [ -d "$repo_root/$snapshot_path" ]; then
    snapshot_path="$repo_root/$snapshot_path"
fi

if [ -z "$snapshot_path" ] || [ ! -d "$snapshot_path" ]; then
    echo "No RDF snapshot run directory found. Pass its path explicitly." >&2
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

graphs="claimreviewdata euroclimatecheck defacto dbkf desmog climafacts climate-fever dbpedia-enricher"
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

if ! find "$data_dir/vocabularies" -type f -name '*.ttl' -size +0c -print -quit | grep -q .; then
    echo "No non-empty Turtle vocabularies found in $data_dir/vocabularies" >&2
    exit 1
fi

temporary_dir=$(mktemp -d "${TMPDIR:-/tmp}/climatesense-qlever.XXXXXX")
trap 'rm -rf "$temporary_dir"' EXIT HUP INT TERM

container_snapshot_dir="../source/$snapshot_relative"
sed \
    -e "s|__SNAPSHOT_DIRECTORY__|$container_snapshot_dir|g" \
    "$template" >"$temporary_dir/Qleverfile"

compose() {
    docker compose -f "$compose_file" -f "$qlever_compose_file" "$@"
}

volume_admin() {
    compose --profile qlever-init run --rm --no-deps qlever-volume-admin sh -eu -c "$1"
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
volume_admin "
    rm -rf /data/index-next
    mkdir -p /data/index-next
    chown -R $qlever_uid:$qlever_gid /data/index-next
"
QLEVER_DEPLOY_QLEVERFILE="$temporary_dir/Qleverfile" \
    compose --profile qlever-init run --rm qlever-index
echo "Switching QLever to the completed candidate index"
compose stop qlever >/dev/null 2>&1 || true
volume_admin '
    test -f /data/index-next/Qleverfile
    rm -rf /data/index-previous
    if [ -d /data/index-current ]; then
        mv /data/index-current /data/index-previous
    fi
    mv /data/index-next /data/index-current
'

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
