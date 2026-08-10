# URI design patterns

This document describes the breaking v2 RDF identifiers. The configured base URI is
`http://data.climatesense-project.eu` unless `output.base_uri` overrides it.

## Identity rules

An RDF identifier has one of three origins:

1. Claim-review and document identities are opaque UUIDs assigned once by the
   PostgreSQL identity registry.
2. Immutable value concepts use namespaced SHA-256 digests.
3. Curated concepts use identifiers maintained in repository vocabularies.

URLs, publication dates, ratings, and document fingerprints are identity evidence.
They never become part of a claim-review IRI. URL changes therefore add aliases to
an existing entity instead of minting another entity.

## Entity patterns

| Entity            | Pattern                      | Assignment                                               |
| ----------------- | ---------------------------- | -------------------------------------------------------- |
| Claim review      | `{base}/claim-review/{uuid}` | Random UUID persisted by the identity registry           |
| Claim             | `{base}/claim/{sha256}`      | Digest of the exact validated claim text                 |
| Person            | `{base}/person/{sha256}`     | Digest of name and website                               |
| Source rating     | `{base}/rating/{sha256}`     | Digest of organization IRI and source-rating fingerprint |
| Normalized rating | `{base}/rating/{label}`      | Curated normalized label                                 |
| Organization      | `{base}/organization/{slug}` | Curated in `data/organizations.ttl`                      |

For example:

```text
http://data.climatesense-project.eu/claim-review/550e8400-e29b-41d4-a716-446655440000
```

The same claim-review resource may have several `schema:url` values. A source rating
is organization-scoped because identical labels from different fact-checkers do not
necessarily represent the same point on the same scale. Normalized ratings remain
global concepts and are linked with `cimple:normalizedReviewRating`.

Digest inputs are encoded as compact JSON arrays containing a namespace and each
value before SHA-256 hashing. This avoids ambiguous string concatenation.

## Enrichment patterns

Model-owned classification concepts use one percent-encoded path segment:

| Concept              | Pattern                               |
| -------------------- | ------------------------------------- |
| Emotion              | `{base}/emotion/{value}`              |
| Sentiment            | `{base}/sentiment/{value}`            |
| Political leaning    | `{base}/political-leaning/{value}`    |
| Conspiracy           | `{base}/conspiracy/{value}`           |
| Trope                | `{base}/trope/{value}`                |
| Persuasion technique | `{base}/persuasion-technique/{value}` |

DBpedia entities and properties retain their external absolute IRIs.

## Named graphs

Published graph IRIs follow `{base}/graph/{SOURCE}`. Source graphs contain complete
claim-review snapshots for that source. Provider-owned entity assertions use stable
enrichment graph names; currently DBpedia output is published in
`{base}/graph/dbpedia-enricher`. The curated graph catalog in `data/graphs.ttl`
describes every managed graph.

Every deployment replaces a managed graph from one complete snapshot. A redeployment
therefore rejects directories containing multiple candidate snapshots for one graph.

## Namespace bindings

| Prefix         | URI                                           |
| -------------- | --------------------------------------------- |
| `rdf`          | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` |
| `rdfs`         | `http://www.w3.org/2000/01/rdf-schema#`       |
| `owl`          | `http://www.w3.org/2002/07/owl#`              |
| `xsd`          | `http://www.w3.org/2001/XMLSchema#`           |
| `dc`           | `http://purl.org/dc/elements/1.1/`            |
| `schema`       | `http://schema.org/`                          |
| `skos`         | `http://www.w3.org/2004/02/skos/core#`        |
| `cimple`       | `http://data.cimple.eu/ontology#`             |
| `climatesense` | `{base}/ontology#`                            |
| `base`         | `{base}/`                                     |

The implementation is split between the
[`domain` model](../src/climatesense_kg/domain/models.py), the
[`identity resolver`](../src/climatesense_kg/identity/resolver.py), and the
[`RDF generator`](../src/climatesense_kg/rdf_generation/generator.py). The complete
identity policy and persistence boundaries are in
[`architecture-v2.md`](architecture-v2.md).
