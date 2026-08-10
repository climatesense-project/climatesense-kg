# ClimateSense KG pipeline v2

## Status

This document defines the breaking v2 architecture. The v2 pipeline is rebuilt from
source data; compatibility with v1 entity IRIs and processing-cache entries is not a
goal.

## Decisions

1. Source observations, fetched documents, and canonical claim reviews are distinct
   domain concepts.
2. A canonical claim review receives an opaque UUID from the identity registry. Its
   IRI is `http://data.climatesense-project.eu/claim-review/{uuid}`.
3. URLs, dates, ratings, claims, and content hashes are evidence about identity. They
   are not entity identifiers.
4. PostgreSQL is authoritative for identity and processing state. Pipeline execution
   fails if either store is unavailable.
5. Downloaded source artifacts remain in the filesystem artifact store. They are not
   processing-state entries.
6. Only deterministic identity matches are merged automatically. Similarity-only
   matches are recorded for offline duplicate audits.
7. The RDF layer serializes resolved entities. It never discovers or merges identity.
8. Normalized ratings are global concepts. Source rating scales are scoped to their
   fact-checking organization.

## Domain model

### Source review record

A `SourceReviewRecord` is one claim/rating pair observed in an upstream source. It
contains a stable source record key, source provenance, an organization reference,
an observed document URL, and source-supplied metadata. It has no RDF identity.

The source record key is used for source extraction and processing checkpoints. A
native upstream identifier is preferred. When no native identifier exists, the
processor derives a deterministic key from the source name and the unmodified fields
that identify the source observation.

### Review document

A `ReviewDocument` represents the fetched fact-checking document. It records the
observed URL, redirect target, HTML canonical URL, preferred URL, extracted content,
and exact and near-duplicate fingerprints. Several source records may resolve to the
same document.

### Canonical claim review

A `CanonicalClaimReview` is the resolved assertion that an organization reviewed a
claim and assigned a rating. It has an explicit UUID and may retain several document
URL aliases. One document may contain several canonical claim reviews when it assesses
several claims. It also retains the current run's source observations so each named
source graph can project its own rating, date, language, authors, and descriptive
metadata without attributing one feed's values to another feed.

## Pipeline

```text
SourceIngestor
  -> OrganizationResolver
  -> DocumentExtractor
  -> IdentityResolver
  -> EnrichmentRunner
  -> RdfBuilder
  -> ArtifactDeployer
```

Each stage accepts and returns typed values. A stage does not construct unrelated
infrastructure and does not catch failures that make its output unreliable.

## Persistence boundaries

### SourceArtifactStore

Stores downloaded releases and source payloads. The initial implementation remains a
locked, compressed filesystem store.

### StageResultStore

Stores a stage result under:

```text
subject key + stage name + stage version + input hash + configuration hash
```

Changing code, relevant configuration, or input invalidates a result without manual
cache deletion.

### IdentityRegistry

Stores canonical document and review UUIDs, source-record mappings, fingerprints, and
duplicate candidates. Identity lookup and assignment occur in one transaction.

## Identity policy

The resolver considers candidates from the same canonical organization. It applies
the following rules in order:

| Evidence                                          | Decision                                     |
| ------------------------------------------------- | -------------------------------------------- |
| Existing source record key                        | Reuse its document and review identities     |
| Same native source identifier                     | Reuse the mapped identities                  |
| Same canonical or final URL and same exact claim  | Reuse the review identity                    |
| Same normalized content hash and same exact claim | Reuse the review identity                    |
| Body similarity at least 0.90                     | Record for audit; do not merge automatically |
| No candidate                                      | Assign new document and review UUIDs         |

An exact-content match from another organization does not merge automatically.
Rating differences do not split an otherwise deterministic match. Ratings are
source-owned metadata and can differ between source graph projections.

## RDF contract

The core RDF continues to publish `schema:ClaimReview`. A resolved review has one IRI
and can have multiple `schema:url` aliases:

```turtle
<http://data.climatesense-project.eu/claim-review/550e8400-e29b-41d4-a716-446655440000>
    a schema:ClaimReview ;
    schema:itemReviewed <http://data.climatesense-project.eu/claim/example> ;
    schema:author <http://data.climatesense-project.eu/organization/factual> ;
    schema:url <https://example.test/old-path>,
               <https://example.test/current-path> .
```

The extracted content variants and identity evidence remain in PostgreSQL. The RDF
contains the selected document text, canonical identity links, and source-owned
metadata in each source graph.

## Completion invariants

- Reprocessing a source record returns the same UUID.
- URL aliases for the same organization, document, and claim resolve to one review.
- Editing document content does not change an established UUID.
- One document with two claims produces two claim-review UUIDs.
- Cross-organization similarity never causes an automatic merge.
- Rating, date, URL, and content changes do not recalculate an entity IRI.
- Every fuzzy candidate stores its score and evidence for offline auditing.
- A failed or incomplete source never replaces its published graph.
- A successful empty source produces an empty full snapshot and clears stale members.
- RDF generation and deployment contain no identity-resolution decisions.
