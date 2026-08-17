# Production Operations

## Durable state

PostgreSQL is authoritative pipeline state and needs the same backup guarantees as
the published RDF. It contains:

- active normalized records in `source_observations`;
- persistent random document and claim-review UUIDs in `documents` and
  `claim_reviews`;
- document URL and exact-text aliases in `document_urls` and
  `document_text_hashes`;
- current extraction results and retry state in `document_extractions`;
- current enrichment results and retry state in `enrichment_results`;
- pipeline run status and optional manual duplicate candidates.

The filesystem `cache/` directory holds downloaded source artifacts. Analytics query
responses are also caches. Neither replaces a PostgreSQL backup.

## Backup and restore

Create a compressed backup:

```bash
docker compose -f docker/docker-compose.yml exec -T postgres sh -c \
  'pg_dump --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --format=custom' \
  > climatesense.dump
```

Restore it into an empty database:

```bash
docker compose -f docker/docker-compose.yml exec -T postgres sh -c \
  'pg_restore --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --exit-on-error' \
  < climatesense.dump
```

Archive deployed RDF snapshots with the matching database backup. Verify a restore by
checking a known source record's `claim_review_id`; preserving the database preserves
its non-deterministic claim-review URI.

## Running the pipeline

Start PostgreSQL and the selected triplestore, then run:

```bash
docker compose -f docker/docker-compose.yml run --build --rm pipeline \
  run --config config/daily.yaml
```

Operational modes are explicit:

- `--skip-download` reads only cached source files and ignores cache expiry;
- `--skip-extraction` makes no document requests and uses current stored successes;
- `--skip-enrichment` makes no enrichment requests and uses current stored successes;
- `--skip-deployment` generates complete artifacts without changing the triplestore;
- `--force-regenerate` recomputes extraction and enrichment results regardless of
  current result state.

RDF export always produces a complete snapshot of every successfully ingested source.

## Run lifecycle and recovery

One session-scoped PostgreSQL advisory lock permits one writer at a time. A concurrent
run exits before ingestion. PostgreSQL releases the lock when a process or connection
ends, including an OOM kill. The next run marks any abandoned `running` row failed and
continues from committed database state.

Inspect recent runs:

```sql
SELECT id, status, started_at, finished_at, error, summary
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 20;
```

Each source is installed in one transaction. Download, parse, validation, or database
failure rolls back that source and leaves its previous active observations unchanged.
The failed source's RDF graph is not exported or replaced during that run.

## Progress and memory

Ingestion, extraction, identity resolution, enrichment, and RDF export process fixed
database batches. Progress lines report processed and total items, rate, ETA, stage
counters, and process RSS. `batch_size` controls the common database batch size;
`progress_interval_seconds` and the extraction/enrichment-specific progress settings
control log frequency. Enrichment lines include the active enricher, the current
review range, and progress through its pending semantic subjects.

Follow a running container from another terminal:

```bash
docker ps --filter label=com.docker.compose.service=pipeline \
  --format 'table {{.Names}}\t{{.Status}}'
docker logs --tail 100 --follow <pipeline-container-name>
```

Committed external-processing results survive interruption. Enrichment results are
written after each completed work unit, so only in-flight units must be repeated.

## Document extraction

The extractor selects distinct normalized URLs from active observations. It allows
only one active request per hostname, applies host-specific request policy, and stops
scheduling a host for the run after an HTTP 429 response. Current successes are
reused. Retryable failures are retried after `retry_at`; permanent failures remain
stored unless processing is forced.

Default scheduling is:

- transient network and server failures: one hour;
- DNS resolution failures: seven days;
- access blocks and detected bot challenges: thirty days;
- pages with no extractable text: thirty days;
- invalid URLs, unsupported or oversized responses, and HTTP 404/410: permanent.

Inspect extraction coverage:

```sql
SELECT status, failure_category, http_status, COUNT(*) AS results,
       MIN(retry_at) AS next_retry_at
FROM document_extractions
GROUP BY status, failure_category, http_status
ORDER BY status, next_retry_at NULLS LAST;
```

`--skip-extraction` never retries failures or fetches missing documents. Stored source
text remains available to identity resolution and RDF projection when extraction is
missing.

## Identity resolution

Identity resolution applies exact rules in this order:

1. keep an existing source-record assignment;
2. use the stable source/native identifier embodied by the record key;
3. match a document from the same organization through a normalized URL alias;
4. match a document from the same organization through an exact normalized text hash;
5. otherwise create a document with a random UUID;
6. reuse or create the `(document, claim)` review with a random UUID;
7. persist the source assignment.

All observed, redirected, and canonical URLs become durable document aliases. All
exact body hashes become durable text aliases even when another body is selected as
the document's preferred export content. This keeps identity independent of batch
boundaries and content-selection order.

Monitor resolution coverage:

```sql
SELECT COUNT(*) FILTER (WHERE claim_review_id IS NOT NULL) AS resolved,
       COUNT(*) FILTER (WHERE claim_review_id IS NULL) AS unresolved
FROM source_observations
WHERE active;
```

Fuzzy text similarity never assigns identities. Rebuild the bounded manual queue with:

```bash
uv run climatesense-kg audit-duplicates --config config/daily.yaml
```

Candidates are stored in `duplicate_candidates` and do not alter RDF.

## Enrichment completeness and outages

Each enricher stores one current result per semantic subject. A matching success is
reused; a due retryable failure is recomputed; a future retry and a permanent failure
remain missing. Health checks run once per dependency per pipeline run and only when
work actually requires the dependency.

The enrichment service runs bounded work units and owns concurrency, checkpointing,
and progress reporting. Spotlight uses one text per work unit and defaults to eight
workers against the hosted ClimateSense endpoint. DBpedia properties retain
single-worker batched access to the public SPARQL endpoint. Each CIMPLE model uses
configured text batches and a separately bounded worker count. Worker counts and
timeouts are operational settings, so tuning them does not invalidate stored
semantic results.

An unavailable dependency does not discard stored successes. If required subjects
are missing, the run is degraded and the affected graph is incomplete. Incomplete
graphs do not replace their final output files and are not sent to the triplestore.
Their existing local and deployed snapshots remain intact.

DBpedia Spotlight claim/review annotations and enabled DBpedia property results govern
the `dbpedia-enricher` graph. Enabled CIMPLE model results govern source graphs because
their claim analysis is emitted there.

Inspect all current processing results through the analytics view:

```sql
SELECT stage_name, stage_version, status, COUNT(*) AS results,
       MIN(retry_at) AS next_retry_at
FROM processing_results
GROUP BY stage_name, stage_version, status
ORDER BY stage_name, status;
```

After restoring a dependency, run the pipeline normally. Stored successes are reused
and due failures are retried.

## RDF snapshots and deployment

The exporter writes N-Triples to a temporary file in the destination directory. A
complete graph is externally sorted and deduplicated on disk, then atomically renamed
to its final path. Failed or incomplete graphs discard their temporary files without
changing the previous snapshot. Finalization can temporarily require enough free disk
space for the raw export, external-sort working files, the deduplicated export, and
the previous snapshot.

Virtuoso deployment receives only complete artifacts and replaces each named graph as
a full snapshot. `redeploy` treats selected files as operator-approved complete
snapshots. Do not use it for files from an interrupted or manually assembled export:

```bash
uv run climatesense-kg redeploy \
  --config config/daily.yaml \
  --rdf-dir data/rdf
```

QLever is deployed as one complete native index:

```bash
just qlever-deploy
```

A scheduled run should chain export and deployment so indexing starts only after the
pipeline exits successfully:

```bash
docker compose -f docker/docker-compose.yml run --build --rm pipeline \
  run --config config/daily.yaml --skip-deployment && \
just qlever-deploy
```

The command selects the latest run directory (named `<run>` as `YYYY-MM-DD_HHMMSS`)
below `data/rdf` by default, and requires all eight graph files with fixed names
(`claimreviewdata.nt.gz`, `euroclimatecheck.nt.gz`, ...) inside it. An explicit cohort can be
selected by passing its run directory:

```bash
just qlever-deploy data/rdf/2026-08-15_143734
```

The candidate is built in `index-next` while `index-current` continues serving
queries. Once indexing succeeds, QLever stops, the directories are renamed, and the
server starts from the completed candidate. The former current index is retained as
`index-previous`. If the readiness query fails, the command restores that previous
index and restarts QLever. The switch never exposes a partially built index.

## Recomputing external results

Delete extraction and enrichment results without touching observations or identities:

```bash
uv run climatesense-kg flush-processing-results --yes
```

This deletes `document_extractions` and `enrichment_results`. The next pipeline run
recomputes them. Documents, URL/text aliases, source assignments, and random review
UUIDs remain unchanged.

## Initial database validation

The packaged schema is one initial migration intended for an empty PostgreSQL
database. Before the first deployment:

1. Run the pipeline with `--skip-deployment` and all configured dependencies ready.
2. Inspect source, extraction, identity, enrichment, and RDF counts.
3. Run it again and confirm that document and claim-review UUIDs are unchanged and
   current successful results are reused.
4. Exercise an unavailable enrichment dependency and confirm that its graph file and
   deployed graph remain untouched.
5. Back up PostgreSQL and the complete RDF snapshots.
6. Deploy the verified snapshots with `just qlever-deploy` or the configured
   Virtuoso backend.
