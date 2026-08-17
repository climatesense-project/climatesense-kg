# Pipeline Refactor Plan

## Goal

Make pipeline identities and enrichment results durable, reusable, and explicit while keeping the RDF model and deployment process predictable.

The implementation should preserve the current RDF relationships. The principal identity rule is that claim-review URIs use UUIDs assigned by the identity registry. Those UUIDs are intentionally non-deterministic: PostgreSQL is durable application state and must be backed up and restored to preserve the same claim-review identities.

## Decisions

- Keep non-deterministic UUIDs for claim reviews.
- Use deterministic SHA-256-based identities where already defined for entities such as claims and people.
- Treat PostgreSQL as durable pipeline state, not as a disposable cache.
- Use one PostgreSQL database named `climatesense`; separate identity data from recomputable stage results through schema and migration boundaries.
- Do not build an importer for the existing production annotation database. The production database and RDF may be archived for rollback, but the fresh production run will not read or migrate them.
- Do not add an RDF compatibility layer or migrate existing RDF URI identifiers.
- Keep downloaded artifacts and analytics query results as caches; do not describe the identity registry or persisted enrichment results as caches.

## Implementation

### 1. Rename and clarify persistence

- Use `climatesense` as the PostgreSQL database name in Compose, environment examples, pipeline configuration, analytics configuration, and documentation.
- Describe PostgreSQL as durable state that requires normal backup, restore, and retention procedures.
- Separate identity-registry migrations from stage-result migrations.
- Ensure the stage-results flush operation deletes only recomputable stage results and never identity mappings.
- Keep a single physical PostgreSQL service unless operational requirements later justify separate services.

### 2. Store enrichment results by semantic subject

Persist each result under the subject whose content determines it:

| Result                                     | Storage identity                                    |
| ------------------------------------------ | --------------------------------------------------- |
| CIMPLE result for one model                | Claim URI plus model identity                       |
| DBpedia Spotlight annotations for a claim  | Claim URI plus claim-text input digest              |
| DBpedia Spotlight annotations for a review | Exact review-text digest                            |
| Selected DBpedia properties                | DBpedia entity URI plus property-selection identity |

- Split Spotlight processing into claim-text and review-text stages so each can be restored independently.
- Store CIMPLE output separately for each model so adding or changing one model does not invalidate unrelated results.
- Avoid keys based on run order, source document position, or an entire assembled RDF document.

### 3. Define stored-result semantics

Each persisted stage result must record enough information to decide whether it can be reused:

- semantic subject identity;
- normalized input digest;
- stage and model identifiers or versions;
- semantic configuration digest;
- status and diagnostic details;
- result payload, including a valid empty result;
- creation and update timestamps.

The pipeline behavior must be:

- reuse a stored successful result when its semantic inputs still match;
- treat a successful empty result as complete;
- retry a stored failure when its dependency is available;
- retain failure diagnostics without treating the failure as a reusable success;
- retry stored document-extraction failures under the same rule;
- invalidate a result when its input, stage behavior, model, or semantic configuration changes.

### 4. Separate semantic and operational configuration

Include settings that can change result meaning in the stored-result identity. Examples include:

- Spotlight model identity, confidence, and support thresholds;
- CIMPLE model identity and version;
- maximum accepted input length when it changes processed content;
- selected DBpedia properties.

Exclude settings that only control execution. Examples include:

- endpoint URL when it serves the same declared model;
- timeouts and retry counts;
- rate limits and batch sizes.

Changing operational settings must not invalidate otherwise reusable results.

### 5. Report completeness explicitly

Produce an enrichment-completeness report for every relevant stage with at least:

- dependency availability;
- eligible subject count;
- stored successes;
- stored failures;
- computed successes;
- computed failures;
- missing results.

The run summary must distinguish results computed during the run, results restored from PostgreSQL, and results that remain incomplete.

### 6. Make deployment completeness-aware

- Publish an RDF graph only when all results required by that graph are complete.
- If Spotlight is unavailable and stored results are incomplete, preserve the existing RDF snapshot and mark the run as degraded.
- Build QLever only from a complete cohort of graph snapshots produced by one run.
- Activate the completed QLever index with a short directory switch and retain the previous index for rollback.
- Do not interpret a disabled enrichment stage as an instruction to clear its deployed graph.
- Keep Virtuoso's per-graph deployment decisions tied to the completeness report.

### 7. Align code and documentation

- Use a version-neutral application identifier consistently across HTTP clients.
- Document PostgreSQL backup and restore as part of production operations.
- Document stage-result flushing separately from identity preservation and database restoration.
- Add an outage runbook covering dependency unavailability, stored-result coverage, degraded runs, and safe graph deployment.
- Use only the canonical architecture terminology throughout code and documentation.

## Implementation Order

1. Rename the database and separate persistence responsibilities.
2. Refactor enrichment storage around semantic subjects.
3. Implement stored-result status, invalidation, and retry semantics.
4. Separate semantic configuration from operational configuration.
5. Add completeness reporting and completeness-aware deployment.
6. Update code terminology, configuration examples, and operational documentation.
7. Validate the behavior locally before the production cutover.

## Fresh Production Cutover

1. Archive the existing production PostgreSQL database and deployed RDF for rollback only.
2. Create a fresh PostgreSQL database named `climatesense`.
3. Run the complete pipeline with deployment disabled and all enrichment services available.
4. Verify subject counts, stored successes, stored failures, and missing results for every stage.
5. Inspect the generated RDF and confirm claim-review identities and enrichment links are internally consistent.
6. Run the pipeline again and confirm that successful stage results are restored from PostgreSQL.
7. Make Spotlight unavailable and confirm the pipeline reuses complete stored annotations without external calls.
8. Create an intentional stored-result miss while Spotlight is unavailable and confirm the run is degraded and the deployed DBpedia graph remains untouched.
9. Back up the initialized PostgreSQL database, including the identity registry.
10. Deploy the graphs produced by a complete run.

## Acceptance Criteria

- A fresh run with all dependencies available produces complete source and enrichment graphs.
- A second unchanged run restores successful results from PostgreSQL instead of recomputing them.
- An unavailable enrichment service with complete stored coverage does not prevent reuse.
- An unavailable enrichment service with missing stored coverage marks the run as degraded and does not replace the affected deployed graph.
- Successful empty results are reused as complete results.
- Stored failures are retried when their dependency is available.
- Operational configuration changes do not invalidate results.
- Semantic input, model, or configuration changes invalidate only the affected results.
- Flushing stage results never removes identity mappings.
- Backing up and restoring PostgreSQL preserves claim-review UUIDs.
- Default configuration and documentation use `climatesense` and canonical architecture terminology consistently.
- The repository's formatting, static checks, and test suite pass.

## Out of Scope

- Deterministic claim-review UUIDs.
- Importing results or identities from the existing production database.
- Migrating existing RDF URI identifiers.
- Maintaining a permanent RDF compatibility layer.
