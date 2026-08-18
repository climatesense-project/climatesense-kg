# URI Design Patterns

The ClimateSense Knowledge Graph pipeline uses three identifier strategies. Claim reviews receive persistent identifiers, claims, people, and source ratings use deterministic hashes, and organizations and normalized ratings use explicitly assigned identifiers.

## Base URI Configuration

The base URI is configurable through the `output.base_uri` setting in YAML configuration files:

```yaml
output:
  base_uri: "http://data.climatesense-project.eu"
```

Relative URIs are resolved against this base URI. Curated organizations and DBpedia entities supply absolute URIs, which the RDF generator uses directly.

## URI Generation Strategy

1. Claim reviews receive random UUIDs that are assigned once and persisted in a PostgreSQL database.
2. Claims, people, and source ratings receive deterministic SHA-256 hashes derived from their immutable identifying values.
3. Organizations and normalized ratings use curated, human-readable identifiers.

Claim reviews don't use deterministic hashing because a review's identity cannot be derived from its content. Two reviews can quote the same claim text, and a review's text, rating, or URLs can change when a document is re-extracted or corrected. A content hash would rename the review every time that happened and would merge reviews that only share the same wording. The review is instead a stable node that gathers every observation of one claim made by one organization on one document.

The identity service assigns the UUIDs. It resolves the document first, from URL aliases and the normalized text hash, and gives it a UUID. Each review is keyed by its document and claim pair. The service checks whether a review already exists for that pair; if not, it generates a fresh UUID and inserts the row. Every observation of that document and claim is linked to the review, and on later runs the observations already carry the review ID, so the same UUID is reused without recomputing it from content.

Hash inputs are encoded as compact JSON arrays with a namespace followed by the identifying values. The UTF-8 JSON representation is hashed with SHA-256 and rendered as a lowercase hexadecimal string. Namespacing and structured encoding prevent collisions caused by ambiguous string concatenation.

## Entity URI Patterns

### Claim Reviews

**Pattern**: `{base_uri}/claim-review/{uuid}`

**Assignment**: Identity resolution assigns and persists one UUID for each canonical claim review.

**Example**: `http://data.climatesense-project.eu/claim-review/550e8400-e29b-41d4-a716-446655440000`

### Claims

**Pattern**: `{base_uri}/claim/{sha256_hash}`

**Hash input**: `["claim", canonical_claim_text]`

**Example**: `http://data.climatesense-project.eu/claim/f1e2d3c4b5a6...`

Claim text is canonicalized before hashing by decoding HTML entities and normalizing whitespace while preserving URLs and other identity-bearing content. Empty, URL-only, and otherwise non-meaningful claims are rejected.

### Organizations

**Pattern**: `{base_uri}/organization/{human-readable_slug}`

**Example**: `http://data.climatesense-project.eu/organization/les-surligneurs`

[`data/organizations.ttl`](../data/organizations.ttl) assigns stable, human-readable organization IRIs. Every processor provides an organization website, and runtime resolution maps its normalized URL to one catalog entry. If an organization is missing from the catalog, the pipeline fails with an error.

### People

**Pattern**: `{base_uri}/person/{sha256_hash}`

**Hash input**: `["person", name, website]`

**Example**: `http://data.climatesense-project.eu/person/a1b2c3d4e5f6...`

### Source Ratings

**Pattern**: `{base_uri}/rating/{sha256_hash}`

**Hash input**: `["source-rating", organization_uri, rating_fingerprint]`

The `rating_fingerprint` is itself a SHA-256 hash of: `["rating", label, original_label, rating_value, best_rating, worst_rating]`

Missing values are represented by empty strings.

**Example**: `http://data.climatesense-project.eu/rating/3c2b1a9f8e7d...`

### Normalized Ratings

**Pattern**: `{base_uri}/rating/{normalized_label}`

**Example**: `http://data.climatesense-project.eu/rating/not_credible`

Normalized ratings are curated global concepts. A claim review links to its source rating with `schema:reviewRating` and, when normalization is available, to the global concept with `cimple:normalizedReviewRating`.

## Enrichment URI Patterns

For all model-owned classifications, the value is trimmed, lowercased, has spaces replaced with underscores, and is percent-encoded as one URI path segment.

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

**Example**: `http://data.climatesense-project.eu/persuasion-technique/appeal_to_authority`

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

The `{SOURCE}` placeholder represents a managed logical graph name. Source graphs use the configured data-source name, while provider-owned entity linking uses a stable enrichment graph name:

- `{base_uri}/graph/dbpedia-enricher` contains DBpedia Spotlight `schema:mentions` assertions and DBpedia entity properties.
- Source graphs contain the claims and reviews referenced by those assertions.
- The DBpedia enrichment graph owns all DBpedia entity-linking triples.
- `{base_uri}/graph/organizations` and `{base_uri}/graph/vocabularies` contain curated repository data.

Each published graph IRI is described in the curated [`data/graphs.ttl`](../data/graphs.ttl) catalog.
