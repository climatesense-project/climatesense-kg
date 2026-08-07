# URI Design Patterns

This document describes the URI design patterns used in the ClimateSense Knowledge Graph pipeline. The system generates deterministic, hash-based URIs for claims, ratings, and claim reviews. Organization identities are assigned explicitly in the curated organization catalog.

## Base URI Configuration

The base URI is configurable via the `output.base_uri` setting in YAML configuration files:

```yaml
output:
  base_uri: "http://data.climatesense-project.eu"
```

All relative URIs are resolved against this base URI to create full URIs for RDF resources.

## URI Generation Strategy

The pipeline uses SHA-224 hashing to generate deterministic URIs based on content.

## Entity URI Patterns

### Claim Reviews

**Pattern**: `{base_uri}/claim-review/{sha224_hash}`

**Hash Input**: `claim-review` + normalized_claim_text + rating_label + normalized_review_url + date_published

**Example**: `http://data.climatesense-project.eu/claim-review/a1b2c3d4e5f6...`

The hash incorporates:

- Normalized claim text (processed by `normalize_text()`)
- Rating label (if present)
- Normalized review URL (hostname + path with trailing slash)
- Date published (if present)

### Claims

**Pattern**: `{base_uri}/claim/{sha224_hash}`

**Hash Input**: `claim` + normalized_text

**Example**: `http://data.climatesense-project.eu/claim/f1e2d3c4b5a6...`

### Organizations

**Pattern**: `{base_uri}/organization/{human-readable-slug}`

**Example**: `http://data.climatesense-project.eu/organization/les-surligneurs`

Organization IRIs are stable, human-readable identifiers assigned in `data/organizations.ttl`; the pipeline does not derive them at runtime. Every processor must provide an organization website, and extracted organizations are resolved exclusively against a unique normalized catalog URL. An unresolved organization stops the run so that maintainers can add or correct its catalog entry instead of silently creating a second identity.

The catalog is the sole source of organization metadata. Claim-review source graphs only link to organization IRIs with `schema:author`; names, websites, country-level locations, network memberships, and parent relationships live in `http://data.climatesense-project.eu/graph/organizations`.

### Ratings

**Pattern**: `{base_uri}/rating/{sha224_hash}`

**Hash Input**: `rating` + (original_label or label)

**Example**: `http://data.climatesense-project.eu/rating/3c2b1a9f8e7d...`

Ratings use the original label if available, falling back to the normalized label.

## Enrichment URI Patterns

The RDF generator creates additional URIs for enrichment data using the following patterns:

### Emotions

**Pattern**: `{base_uri}/emotion/{emotion_name}`
**Example**: `http://data.climatesense-project.eu/emotion/anger`

### Sentiments

**Pattern**: `{base_uri}/sentiment/{sentiment_name}`
**Example**: `http://data.climatesense-project.eu/sentiment/negative`

### Political Leanings

**Pattern**: `{base_uri}/political-leaning/{leaning_name}`
**Example**: `http://data.climatesense-project.eu/political-leaning/left`

### Conspiracies

**Pattern**: `{base_uri}/conspiracy/{conspiracy_name}`
**Example**: `http://data.climatesense-project.eu/conspiracy/climate_change_hoax`

### Tropes

**Pattern**: `{base_uri}/trope/{trope_name}`
**Example**: `http://data.climatesense-project.eu/trope/time_will_tell`

### Persuasion Techniques

**Pattern**: `{base_uri}/persuasion-technique/{technique_name}`
**Example**: `http://data.climatesense-project.eu/persuasion-technique/appeal_to_authority`

### Normalized Ratings

**Pattern**: `{base_uri}/rating/{normalized_label}`
**Example**: `http://data.climatesense-project.eu/rating/false`

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

For triple store deployment, graph URIs follow a template pattern:

**Pattern**: `{base_uri}/graph/{SOURCE}`
**Example**: `http://data.climatesense-project.eu/graph/euroclimatecheck`

The `{SOURCE}` placeholder is replaced with a managed logical graph name. Most generated graphs use the configured data source name. Provider-owned entity linking uses a stable enrichment graph name instead:

- `http://data.climatesense-project.eu/graph/dbpedia-enricher` contains DBpedia Spotlight `schema:mentions` assertions and DBpedia entity properties.
- Source graphs contain the claims and reviews referenced by those assertions, but no DBpedia entity-linking triples.

Each published graph IRI is described in the curated [`data/graphs.ttl`](../data/graphs.ttl) catalog.

## Implementation Details

URI generation is implemented in the canonical data models ([`src/climatesense_kg/config/models.py`](../src/climatesense_kg/config/models.py)). Organization resolution is implemented by the curated catalog loader ([`src/climatesense_kg/config/organizations.py`](../src/climatesense_kg/config/organizations.py)).

The `RDFGenerator` class ([`src/climatesense_kg/rdf_generation/generator.py`](../src/climatesense_kg/rdf_generation/generator.py)) handles the conversion from relative URIs to full URIs and manages the RDF serialization with proper namespace bindings.
