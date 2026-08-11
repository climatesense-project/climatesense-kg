# Production Operations

## Durable State

PostgreSQL is durable application state. Back it up with the same retention and
recovery guarantees as the published RDF.

The database contains two independently managed categories:

- Identity tables assign and preserve document and claim-review UUIDs. They are not
  recomputable because claim-review UUIDs are intentionally random.
- Stage-result tables store recomputable extraction and enrichment outcomes.
  `stage_results` holds current reusable results and `stage_result_attempts` retains
  immutable attempt diagnostics.

The filesystem `cache/` directory contains downloaded source artifacts. Analytics
SQL and SPARQL responses are also caches. Neither cache replaces a PostgreSQL backup.

## Backup and Restore

Create a compressed PostgreSQL backup before deployments that can affect pipeline
state and after initializing a fresh identity registry:

```bash
docker compose -f docker/docker-compose.yml exec -T postgres sh -c \
  'pg_dump --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --format=custom' \
  > climatesense.dump
```

Restore into the empty `climatesense` database created by PostgreSQL before running
the pipeline:

```bash
docker compose -f docker/docker-compose.yml exec -T postgres sh -c \
  'pg_restore --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --exit-on-error' \
  < climatesense.dump
```

Also archive the currently deployed RDF snapshots so the PostgreSQL and RDF states
can be rolled back together. Verify a restored database by resolving a known source
record and confirming that its claim-review UUID matches the backup.

## Stage-Result Flush

Use the dedicated command only when extraction and enrichment results must be
recomputed:

```bash
uv run climatesense-kg flush-stage-results --yes
```

This deletes `stage_results` and `stage_result_attempts`. It does not delete source
records, review documents, claim-review identities, or identity candidates. Deleting
the PostgreSQL database or its identity tables creates a different identity universe
and therefore different claim-review UUIDs.

## Attempt-History Retention

`stage_result_attempts` is append-only and should be monitored like any audit table.
Choose a retention period that covers the analytics and incident-review window. To
prune only diagnostics older than 180 days after taking a backup:

```bash
docker compose -f docker/docker-compose.yml exec -T postgres sh -c \
  'psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' <<'SQL'
DELETE FROM stage_result_attempts
WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '180 days';
SQL
```

This does not affect current reusable results in `stage_results` or any identity
table. Analytics over attempt history reflects the retained time window.

## Enrichment Completeness

Each enabled enrichment stage reports:

- dependency availability;
- eligible semantic subjects;
- stored successes and failures;
- computed successes and failures;
- results still missing after the run.

A successful empty result is complete. A stored failure is diagnostic state, not a
reusable result, and is retried whenever the dependency is available. Dependency
availability is reported as not checked when stored-only execution is requested or
when every eligible subject was restored successfully and no external call is needed.

DBpedia Spotlight claim annotations, review annotations, and enabled DBpedia property
lookups are required for a complete `dbpedia-enricher` graph. The pipeline replaces
that graph only when all required results and the RDF projection are complete.
Enabled CIMPLE stages are required for complete source graphs because their claim
analysis is emitted there. Complete source graphs and catalogs may still deploy when
only the DBpedia enrichment graph is incomplete. Any graph with missing required
results retains its deployed snapshot. Disabling an enrichment stage does not clear
its deployed graph.

## Enrichment Outage Runbook

1. Run the pipeline normally and inspect the per-stage completeness summary.
2. If a dependency is unavailable but every eligible subject has a stored success,
   allow the run to continue; the graph is complete from restored results.
3. If any required result is missing or failed, confirm that the run is marked
   degraded and lists the affected stage.
4. Confirm that `dbpedia-enricher` appears under preserved or skipped graphs and is
   absent from replacement requests to the triplestore.
5. Confirm that complete source graphs and catalogs deployed successfully.
6. Restore the dependency and rerun. Stored failures are retried automatically;
   stored successes are reused.
7. Deploy the enrichment graph only after its missing-result count reaches zero.

Do not use `redeploy` with an incomplete enrichment artifact. That command treats
the selected files as operator-approved full snapshots and cannot reconstruct the
completeness evidence from the originating run.

## Fresh Production Cutover

1. Archive the current PostgreSQL database and deployed RDF for rollback only.
2. Create an empty PostgreSQL database named `climatesense`.
3. Run the complete pipeline with deployment disabled and all enrichment services
   available.
4. Verify eligible-subject, success, failure, and missing-result counts for every
   stage.
5. Inspect the RDF and confirm that claim reviews, claims, and enrichment links are
   internally consistent.
6. Run the pipeline again and confirm that unchanged successful results are restored
   from PostgreSQL.
7. Make Spotlight unavailable and confirm that complete stored annotations are
   reused without annotation requests.
8. Create an intentional stored-result miss while Spotlight remains unavailable;
   confirm a degraded run and verify that the deployed DBpedia graph is untouched.
9. Back up the initialized PostgreSQL database, including the identity registry.
10. Deploy the graphs produced by a complete run.

The archived database is not imported into the fresh database, and no RDF URI
compatibility layer is required.
