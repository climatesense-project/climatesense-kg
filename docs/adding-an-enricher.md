# Adding an enricher

An enricher wraps one external service that adds data to canonical claim reviews. The base class is in `src/climatesense_kg/enrichers/base.py`. The service that schedules and caches enrichers is in `src/climatesense_kg/enrichment.py`.

The existing enrichers follow two patterns:

- **Entity-linking enrichers**: They attach `EntityMention` objects to claims or reviews (DBpedia Spotlight, OpenTapioca, ReFinED), and fetch extra properties for those linked entities. Their triples end up in their own dedicated RDF graph, so they need the graph wiring in [step 5](#5-wire-the-rdf-graph-entity-linking-enrichers-only).
- **Claim-analysis enrichers** write values into `claim.analysis` and don't require any graph wiring.

## 1. Write the enricher class

Create `src/climatesense_kg/enrichers/<name>_enricher.py`. Subclass `Enricher` and call the base constructor:

```python
class ExampleEnricher(Enricher):
    def __init__(
        self, *, api_url: str, confidence: float, max_workers: int = 4
    ) -> None:
        super().__init__(
            "example",
            version="1",
            semantic_config={"confidence": confidence},
            batch_size=25,
            max_workers=max_workers,
        )
        self.api_url = api_url
        self.confidence = confidence
```

The base constructor arguments control caching and concurrency:

- `name` is the persistence key in the `enrichment_results` table and the stage name in logs. Renaming an enricher orphans its stored results, and the pipeline recomputes everything. When one class serves two targets, encode the target in the name, as `DBpediaSpotlightEnricher` does with `dbpedia_spotlight.claim` and `dbpedia_spotlight.review`.
- `version` participates in cache validation. Bump it whenever a code change alters results for the same input.
- The base class hashes `semantic_config` into the cache key alongside the version. Include every setting that changes results, such as a model id or a confidence threshold. Leave out parameters that change only performance, such as timeouts and worker counts.
- `availability_key` lets enrichers share a health check. It defaults to `name`. The service calls `is_available` once per run and, while the check fails, leaves the enricher's pending subjects uncomputed.
- `batch_size` and `max_workers` bound the service's work units and thread pool. Both must be positive numbers.

Implement the five required methods:

```python
    def is_available(self) -> bool:
        ...

    def subject_key(self, item: CanonicalClaimReview) -> str:
        ...

    def input_value(self, item: CanonicalClaimReview) -> Any:
        ...

    def compute_item(self, item: CanonicalClaimReview) -> ProcessingResult:
        return ProcessingResult.success({"value": ...})

    def apply_item(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        ...
```

The service relies on these rules:

- `compute_item` must not mutate the review. The service stores payloads in Postgres and calls `apply_item` later, sometimes on a later run or during export through `EnrichmentService.apply_stored`.
- The payload must be JSON-serializable. The service stores it in a JSONB column and reads it back as plain dicts, so `apply_item` must work from deserialized values.
- `apply_item` must be idempotent. Remove the enricher's own earlier contribution before extending (e.g., as `DBpediaSpotlightEnricher.apply_item` does by filtering out its `source` first).
- One subject can back several reviews in the same batch. The default `apply` passes the payload to every target, so `apply_item` runs once per review.
- The base class hashes `input_value` into the cache key next to `version` and `semantic_config`. Return only the fields whose change should force recomputation.
- Two items with the same `subject_key` but different `input_value` raise a `RuntimeError` in `subjects()`. Pick a key that captures the semantic input, or hash the input into the key, as `DBpediaSpotlightEnricher` does for review text.
- An unhandled exception in `compute_item` becomes a retryable failure with the error recorded in the payload. Return `ProcessingResult.permanent_failure({...})` when a retry cannot fix the problem.
- Override `compute_batch(subjects)` when the API accepts a batch.
- Override `eligible_items(items)` to skip items the enricher cannot serve (e.g., `DBpediaSpotlightEnricher` drops reviews with empty text for the `review` target).

## 2. Export the class

Add the import and the `__all__` entry in `src/climatesense_kg/enrichers/__init__.py`.

## 3. Add configuration

Add a dataclass in `src/climatesense_kg/config/schemas.py`:

```python
@dataclass
class ExampleConfig:
    """Configuration for the example enricher."""

    enabled: bool = False
    api_url: str = "https://example.test/annotate"
    confidence: float = 0.5
    timeout: int = 20
    max_workers: int = 4

    def __post_init__(self) -> None:
        if self.max_workers <= 0:
            raise ValueError("Example max_workers must be positive")
```

Register it on `EnrichmentConfig` in the same file:

```python
    example: ExampleConfig = field(default_factory=ExampleConfig)
```

`load_config` parses the YAML through dacite in strict mode, so every YAML key must match a field exactly. The files in `config/` show the YAML format. Put credentials in environment variables rather than the YAML, and validate them in the constructor, as `CimpleModelEnricher` does with `CIMPLE_FACTORS_API_URL` and `CIMPLE_FACTORS_API_KEY`.

If your enricher depends on another enricher's output (e.g., DBpedia Spotlight must be enabled for DBpedia entity properties fetching to work), enforce that in `EnrichmentConfig.__post_init__`.

## 4. Construct it in bootstrap

`_build_enrichers` in `src/climatesense_kg/bootstrap.py` builds the list the service runs. Append instances behind the enabled flag:

```python
    if enrichment.example.enabled:
        example = enrichment.example
        enrichers.append(
            ExampleEnricher(
                api_url=example.api_url,
                confidence=example.confidence,
                max_workers=example.max_workers,
            )
        )
```

Annotators that can target either the claim or the review text run as two instances, one per target (e.g., DBpedia Spotlight, OpenTapioca, ReFinED).

## 5. Wire the RDF graph (entity-linking enrichers only)

Skip this step for claim-analysis enrichers.

Each entity mention carries a `source` string. The exporter splits mentions across enrichment graphs by that source. Three touchpoints:

1. `src/climatesense_kg/config/graphs.py`: add a graph-name constant and the frozenset of entity sources that write into it.
2. `_enrichment_graphs` in `bootstrap.py`: map the graph name when a member enricher is enabled.
3. `data/graphs.ttl`: describe the graph in the curated catalog. Update `docs/URI-patterns.md` to match.

The graph URI becomes `{base_uri}/graph/<name>`. Providers that link to the same knowledge base share one graph, which is why OpenTapioca and ReFinED both write into `wikidata-enricher`.

If you also add a SPARQL property enricher for the new graph, follow the two existing `SparqlEntityPropertyEnricher` blocks in `_build_enrichers`. They pass `entity_sources` to select the mentions to follow, plus a probe entity and property for the health check.

## 6. Test it

Add `tests/enrichers/test_<name>_enricher.py`. The `make_review` fixture in `tests/enrichers/conftest.py` builds `CanonicalClaimReview` objects. Test the pure parts against literal expected values: `subjects()` grouping, `compute_item` with a mocked request, `apply_item` with a stored payload, and idempotent re-application. If the config dataclass adds validation, cover it in `tests/config/test_enrichment_config.py`.

## 7. Run the checks

```
just format
just check
just test tests/enrichers/test_example_enricher.py
just test
```

`just check` runs ruff and ty.
