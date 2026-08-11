# URI Design Patterns

This document describes the URI design patterns used in the ClimateSense Knowledge
Graph pipeline. Claim reviews receive persistent identifiers, immutable value
resources use deterministic hashes, and curated resources use explicitly assigned
identifiers.

## Base URI Configuration

The base URI is configurable via the `output.base_uri` setting in YAML configuration
files:

```yaml
output:
  base_uri: "http://data.climatesense-project.eu"
```

All relative URIs are resolved against this base URI. An absolute URI, such as a
curated organization or DBpedia entity URI, is preserved unchanged.

## URI Generation Strategy

The pipeline uses three identifier strategies:

1. Claim reviews receive random UUIDs that are assigned once and persisted by the
   identity registry.
2. Claims, people, and source ratings receive deterministic SHA-256 hashes derived
   from their immutable identifying values.
3. Organizations and normalized ratings use curated, human-readable identifiers.

Hash inputs are encoded as compact JSON arrays containing a namespace followed by the
identifying values. The UTF-8 JSON representation is hashed with SHA-256 and rendered
as a lowercase hexadecimal string. Namespacing and structured encoding prevent
collisions caused by ambiguous string concatenation.

## Entity URI Patterns

### Claim Reviews

**Pattern**: `{base_uri}/claim-review/{uuid}`

**Assignment**: A UUID is generated when the identity registry cannot resolve a
source observation to an existing claim review. The UUID is then persisted and reused.

**Example**:
`http://data.climatesense-project.eu/claim-review/550e8400-e29b-41d4-a716-446655440000`

The URI is not derived from the claim text, rating, review URL, publication date, or
document content. Changes to those values therefore do not recalculate the URI. When
several URLs identify the same resolved review, they are emitted as multiple
`schema:url` values on the same resource.

### Claims

**Pattern**: `{base_uri}/claim/{sha256_hash}`

**Hash input**: `["claim", canonical_claim_text]`

**Example**: `http://data.climatesense-project.eu/claim/f1e2d3c4b5a6...`

Claim text is canonicalized before hashing by decoding HTML entities and normalizing
whitespace while preserving URLs and other identity-bearing content. Empty,
URL-only, and otherwise non-meaningful claims are rejected.

### Organizations

**Pattern**: `{base_uri}/organization/{human-readable_slug}`

**Example**:
`http://data.climatesense-project.eu/organization/les-surligneurs`

Organization IRIs are stable, human-readable identifiers assigned explicitly in
[`data/organizations.ttl`](../data/organizations.ttl); the pipeline does not derive
them at runtime. Every processor must provide an organization website, and extracted
organizations are resolved against a unique normalized catalog URL. An unresolved
organization stops the run so maintainers can add or correct its catalog entry instead
of silently creating another identity.

The catalog is the sole source of organization metadata. Claim-review source graphs
link to organization IRIs with `schema:author`; names, websites, country-level
locations, network memberships, and parent relationships live in
`{base_uri}/graph/organizations`.

### People

**Pattern**: `{base_uri}/person/{sha256_hash}`

**Hash input**: `["person", name, website_or_empty_string]`

**Example**: `http://data.climatesense-project.eu/person/a1b2c3d4e5f6...`

A person's role and external source URI are descriptive metadata and do not affect
the generated URI.

### Source Ratings

**Pattern**: `{base_uri}/rating/{sha256_hash}`

Source-rating identity is scoped to the organization that defined the scale. The
outer hash input is:

```text
["organization-rating", organization_uri, rating_fingerprint]
```

The `rating_fingerprint` is itself a SHA-256 hash of:

```text
["rating", label, original_label, rating_value, best_rating, worst_rating]
```

Missing values are represented by empty strings. The explanation is descriptive
metadata and is not part of the identifier.

**Example**: `http://data.climatesense-project.eu/rating/3c2b1a9f8e7d...`

Scoping prevents identical labels from different fact-checking organizations from
being treated as the same source-scale concept.

### Normalized Ratings

**Pattern**: `{base_uri}/rating/{normalized_label}`

**Example**: `http://data.climatesense-project.eu/rating/not_credible`

Normalized ratings are curated global concepts. A claim review links to its source
rating with `schema:reviewRating` and, when normalization is available, to the global
concept with `cimple:normalizedReviewRating`.

## Enrichment URI Patterns

The RDF generator creates model-owned classification URIs using the following
patterns:

### Emotions

**Pattern**: `{base_uri}/emotion/{value}`

**Example**: `http://data.climatesense-project.eu/emotion/anger`

### Sentiments

**Pattern**: `{base_uri}/sentiment/{value}`

**Example**: `http://data.climatesense-project.eu/sentiment/negative`

### Political Leanings

**Pattern**: `{base_uri}/political-leaning/{value}`

**Example**: `http://data.climatesense-project.eu/political-leaning/left`

### Conspiracies

**Pattern**: `{base_uri}/conspiracy/{value}`

**Example**: `http://data.climatesense-project.eu/conspiracy/climate_change_hoax`

### Tropes

**Pattern**: `{base_uri}/trope/{value}`

**Example**: `http://data.climatesense-project.eu/trope/time_will_tell`

### Persuasion Techniques

**Pattern**: `{base_uri}/persuasion-technique/{value}`

**Example**:
`http://data.climatesense-project.eu/persuasion-technique/appeal_to_authority`

For all model-owned classifications, the value is trimmed, lowercased, has spaces
replaced with underscores, and is percent-encoded as one URI path segment. DBpedia
entities and properties retain their external absolute IRIs.

## RDF Namespace Declarations

The generated RDF uses the following namespace bindings:

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
| `climatesense` | `{base_uri}/ontology#`                        |
| `base`         | `{base_uri}/`                                 |

## Graph URI Templates

For triple-store deployment, graph URIs follow a template pattern:

**Pattern**: `{base_uri}/graph/{SOURCE}`

**Example**: `http://data.climatesense-project.eu/graph/euroclimatecheck`

The `{SOURCE}` placeholder is replaced with a managed logical graph name. Most
generated graphs use the configured data-source name. Provider-owned entity linking
uses a stable enrichment graph name instead:

- `{base_uri}/graph/dbpedia-enricher` contains DBpedia Spotlight `schema:mentions`
  assertions and DBpedia entity properties.
- Source graphs contain the claims and reviews referenced by those assertions, but no
  DBpedia entity-linking triples.
- `{base_uri}/graph/organizations` and `{base_uri}/graph/vocabularies` contain curated
  repository data.

Each published graph IRI is described in the curated
[`data/graphs.ttl`](../data/graphs.ttl) catalog.

## Implementation Details

Deterministic URI generation is implemented in the canonical
[`domain models`](../src/climatesense_kg/domain/models.py). Claim-review UUIDs are
assigned by the [`identity resolver`](../src/climatesense_kg/identity/resolver.py) and
persisted by the identity registry. Organization resolution is implemented by the
[`organization catalog`](../src/climatesense_kg/config/organizations.py).

The [`RDFGenerator`](../src/climatesense_kg/rdf_generation/generator.py) resolves
relative URIs against the configured base URI and manages namespace bindings and RDF
serialization.
