"""PostgreSQL contracts for the authoritative pipeline data path.

Set ``TEST_POSTGRES_DB`` (and optional ``TEST_POSTGRES_*`` connection variables)
to run these tests against a disposable database. The database contents are erased.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import gzip
from importlib.resources import files
import os
from pathlib import Path
from typing import LiteralString, cast
from unittest.mock import Mock
from urllib.parse import urlparse
from uuid import UUID, uuid4

from psycopg import sql
import pytest
from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF

from climatesense_kg.config.organizations import (
    ORGANIZATION_CATALOG_PATH,
    OrganizationCatalog,
)
from climatesense_kg.database import Database
from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalClaimReview,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
)
from climatesense_kg.enrichers import Enricher
from climatesense_kg.enrichment import EnrichmentService
from climatesense_kg.export import RdfExporter
from climatesense_kg.extraction import DocumentExtractionService, DocumentTarget
from climatesense_kg.identity import IdentityService, IdentitySummary
from climatesense_kg.identity.repository import IdentityRepository
from climatesense_kg.ingestion import IngestionService
from climatesense_kg.processing import ProcessingResult, StageSummary
from climatesense_kg.projection import ReviewProjectionReader
from climatesense_kg.rdf_generation import RDFGenerator
from climatesense_kg.utils.text_processing import normalize_document_url

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_POSTGRES_DB"),
    reason="TEST_POSTGRES_DB does not name a disposable PostgreSQL database",
)


def _database() -> Database:
    return Database(
        host=os.getenv("TEST_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("TEST_POSTGRES_PORT", "5432")),
        database=os.environ["TEST_POSTGRES_DB"],
        user=os.getenv("TEST_POSTGRES_USER", "postgres"),
        password=os.getenv("TEST_POSTGRES_PASSWORD"),
    )


@pytest.fixture
def database() -> Iterator[Database]:
    database = _database()
    with database.pool.connection() as connection:
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                """
                TRUNCATE TABLE
                    enrichment_results,
                    document_extractions,
                    source_observations,
                    claim_reviews,
                    document_text_hashes,
                    document_urls,
                    documents,
                    pipeline_runs
                CASCADE
                """
            )
    try:
        yield database
    finally:
        database.close()


def _record(
    native_id: str,
    url: str,
    claim_text: str,
    source_text: str,
    *,
    organization_name: str = "Factual.ro",
    organization_url: str = "https://factual.ro",
) -> SourceReviewRecord:
    source = SourceReference.from_observation(
        source_name="fixture",
        source_type="claimreviewdata",
        native_id=native_id,
        observed_url=url,
        claim_text=claim_text,
    )
    return SourceReviewRecord(
        source=source,
        claim=CanonicalClaim(claim_text),
        organization=OrganizationReference(
            name=organization_name,
            website=organization_url,
        ),
        document=ReviewDocument(observed_url=url, source_text=source_text),
    )


def _records() -> list[SourceReviewRecord]:
    shared_body = "A sufficiently descriptive fact-check body shared verbatim."
    return [
        _record("a", "https://factual.ro/review-a", "Claim alpha", shared_body),
        _record("b", "https://factual.ro/review-b", "Claim alpha", shared_body),
        _record(
            "c",
            "https://factual.ro/review-a#source-fragment",
            "Claim beta",
            "A different body attached to the same normalized document URL.",
        ),
        _record(
            "d",
            "https://africacheck.org/review-d",
            "Claim alpha",
            shared_body,
            organization_name="Africa Check",
            organization_url="https://africacheck.org",
        ),
    ]


class _FixtureEnricher(Enricher):
    def __init__(
        self,
        results: list[ProcessingResult],
        *,
        available: bool = True,
    ) -> None:
        super().__init__("fixture.enrichment", version="1")
        self.available = available
        self.results = results
        self.compute_calls = 0

    def is_available(self) -> bool:
        return self.available

    def subject_key(self, item: CanonicalClaimReview) -> str:
        return item.claim.uri

    def input_value(self, item: CanonicalClaimReview) -> str:
        return item.claim.text

    def compute_item(self, item: CanonicalClaimReview) -> ProcessingResult:
        del item
        result = self.results[self.compute_calls]
        self.compute_calls += 1
        return result

    def apply_item(
        self,
        item: CanonicalClaimReview,
        payload: dict[str, object],
    ) -> None:
        del item, payload


def _partition(database: Database, column: str) -> set[frozenset[str]]:
    if column not in {"claim_review_id", "document_id"}:
        raise ValueError(column)
    with database.pool.connection() as connection, connection.cursor() as cursor:
        if column == "claim_review_id":
            cursor.execute(
                """
                SELECT record_key, claim_review_id
                FROM source_observations
                WHERE active
                ORDER BY record_key
                """
            )
        else:
            cursor.execute(
                """
                SELECT observation.record_key, review.document_id
                FROM source_observations AS observation
                JOIN claim_reviews AS review
                  ON review.id = observation.claim_review_id
                WHERE observation.active
                ORDER BY observation.record_key
                """
            )
        rows = cursor.fetchall()
    groups: dict[object, set[str]] = {}
    for record_key, identity in rows:
        groups.setdefault(identity, set()).add(record_key)
    return {frozenset(group) for group in groups.values()}


def _install(
    database: Database,
    records: list[SourceReviewRecord],
    *,
    batch_size: int,
) -> tuple[dict[str, object], set[frozenset[str]], set[frozenset[str]]]:
    run = database.start_run(f"batch-{batch_size}")
    catalog = OrganizationCatalog(ORGANIZATION_CATALOG_PATH)
    service = IngestionService(
        database.pool,
        Mock(),
        catalog,
        batch_size=batch_size,
        progress_interval_seconds=0,
    )
    try:
        service.install_source(run.id, "fixture", records)
        IdentityService(
            database.pool,
            batch_size=batch_size,
            progress_interval_seconds=0,
        ).run(run.id)
        with database.pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT record_key, claim_review_id
                FROM source_observations
                ORDER BY record_key
                """
            )
            identities = dict(cursor.fetchall())
        return (
            identities,
            _partition(database, "document_id"),
            _partition(database, "claim_review_id"),
        )
    finally:
        database.finish_run(run, status="complete")


def _reset_domain_state(database: Database) -> None:
    with database.pool.connection() as connection:
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                """
                TRUNCATE TABLE
                    enrichment_results,
                    document_extractions,
                    source_observations,
                    claim_reviews,
                    document_text_hashes,
                    document_urls,
                    documents,
                    pipeline_runs
                CASCADE
                """
            )


def test_identity_partition_is_independent_of_order_and_batch_size(
    database: Database,
) -> None:
    records = _records()
    first_ids, first_documents, first_reviews = _install(
        database, records, batch_size=1
    )
    repeated_ids, repeated_documents, repeated_reviews = _install(
        database, list(reversed(records)), batch_size=3
    )

    assert repeated_ids == first_ids
    assert repeated_documents == first_documents
    assert repeated_reviews == first_reviews

    _reset_domain_state(database)
    _new_ids, reordered_documents, reordered_reviews = _install(
        database, list(reversed(records)), batch_size=2
    )
    assert reordered_documents == first_documents
    assert reordered_reviews == first_reviews
    assert len(first_documents) == 2
    assert len(first_reviews) == 3


def _reconcile(database: Database, batch_size: int = 2) -> tuple[IdentitySummary, UUID]:
    run = database.start_run("reconcile")
    try:
        summary = IdentityService(
            database.pool, batch_size=batch_size, progress_interval_seconds=0
        ).run(run.id)
    except Exception:
        database.finish_run(run, status="failed")
        raise
    database.finish_run(run, status="complete")
    return summary, run.id


def _extract(
    database: Database,
    record: SourceReviewRecord,
    result: ProcessingResult,
) -> None:
    url = record.document.observed_url
    key = normalize_document_url(url)
    assert key is not None
    DocumentExtractionService(database.pool)._store(
        [(DocumentTarget(key, url), result)]
    )


def _project(database: Database) -> list[CanonicalClaimReview]:
    reader = ReviewProjectionReader(
        database.pool, OrganizationCatalog(ORGANIZATION_CATALOG_PATH).resolve
    )
    return [item for batch in reader.iter_batches(batch_size=2) for item in batch]


@pytest.mark.parametrize("batch_size", [1, 3])
@pytest.mark.parametrize("evidence", ["url", "text", "both"])
def test_late_extraction_reconciles_inactive_identity_and_preserves_rdf(
    database: Database, batch_size: int, evidence: str
) -> None:
    original = _record(
        "original", "https://factual.ro/article", "Same claim", "Original body."
    )
    variant = _record("variant", "https://factual.ro/article?page=2", "Same claim", "")
    first_ids, _, _ = _install(database, [original], batch_size=1)
    original_document = _project(database)[0].document.id
    _extract(database, variant, ProcessingResult.retryable({"error": "timeout"}))
    second_ids, _, _ = _install(database, [variant], batch_size=1)
    retired_review = second_ids[variant.source.record_key]
    assert retired_review != first_ids[original.source.record_key]
    retired_document = _project(database)[0].document.id

    # An enrichment's semantic claim input is unaffected by document reconciliation.
    enricher = _FixtureEnricher([ProcessingResult.success({"value": "cached"})])
    reader = ReviewProjectionReader(
        database.pool, OrganizationCatalog(ORGANIZATION_CATALOG_PATH).resolve
    )
    enrichment = EnrichmentService(database.pool, reader, [enricher])
    assert enrichment.run()[0].succeeded == 1
    content = (
        "Different and more complete article body."
        if evidence == "url"
        else "Original body."
    )
    canonical = original.document.observed_url if evidence != "text" else None
    _extract(
        database,
        variant,
        ProcessingResult.success(
            {
                "content": content,
                "final_url": variant.document.observed_url,
                "canonical_url": canonical,
            }
        ),
    )
    summary, run_id = _reconcile(database, batch_size)
    assert (summary.documents_merged, summary.reviews_merged) == (1, 1)
    assert (summary.documents_created, summary.reviews_created) == (0, 0)
    (review,) = _project(database)
    assert review.id == first_ids[original.source.record_key]
    assert review.document.id == original_document
    assert review.retired_ids == {retired_review}
    assert review.document.urls == {
        original.document.observed_url,
        variant.document.observed_url,
    }
    assert review.review_text == content
    assert review.source_record_keys == {variant.source.record_key}
    assert enrichment.run()[0].cached == 1
    assert enricher.compute_calls == 1
    with database.pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT claim_review_id, active FROM source_observations ORDER BY active"
        )
        assert cursor.fetchall() == [(review.id, False), (review.id, True)]
        cursor.execute(
            "SELECT retired_id, survivor_id, merged_into_id, run_id, evidence FROM document_merges"
        )
        merge_row = cursor.fetchone()
        assert merge_row is not None
        retired, survivor, merged_into, recorded_run, proof = merge_row
        assert (retired, survivor, merged_into, recorded_run) == (
            retired_document,
            original_document,
            original_document,
            run_id,
        )
        assert any(link["kind"] in {"url", "text_hash"} for link in proof["links"])
    base = "http://data.climatesense-project.eu"
    generator = RDFGenerator(base)
    graph = Graph().parse(data=generator.generate([review], "nt"), format="nt")
    assert (
        URIRef(f"{base}/claim-review/{retired_review}"),
        OWL.sameAs,
        URIRef(f"{base}/{review.uri}"),
    ) in graph
    assert len(list(graph.subjects(RDF.type, generator.SCHEMA.ClaimReview))) == 1
    repeat, _ = _reconcile(database, 1 if batch_size == 3 else 3)
    assert (
        repeat.documents_created,
        repeat.reviews_created,
        repeat.documents_merged,
        repeat.reviews_merged,
    ) == (0, 0, 0, 0)
    assert _project(database)[0].id == review.id


def test_transitive_merges_preserve_distinct_claims_and_all_sources(
    database: Database,
) -> None:
    a = _record("a", "https://factual.ro/a", "Shared claim", "Body alpha")
    b = _record("b", "https://factual.ro/b", "Shared claim", "Body beta")
    c = _record("c", "https://factual.ro/c", "Distinct claim", "Body gamma")
    _install(database, [a], batch_size=1)
    oldest = _project(database)[0]
    _install(database, [a, b, c], batch_size=1)
    distinct = next(
        item.id for item in _project(database) if item.claim.text == "Distinct claim"
    )
    # A new observation bridges A by URL to B by text; C then joins B by URL.
    bridge = _record("bridge", a.document.observed_url, "Shared claim", "Body beta")
    bridge = replace(bridge, source=replace(bridge.source, source_name="second-source"))
    _extract(
        database,
        c,
        ProcessingResult.success(
            {"content": "Body gamma", "canonical_url": b.document.observed_url}
        ),
    )
    run = database.start_run("bridge")
    try:
        ingestion = IngestionService(
            database.pool, Mock(), OrganizationCatalog(ORGANIZATION_CATALOG_PATH)
        )
        ingestion.install_source(run.id, "second-source", [bridge])
        summary = IdentityService(database.pool, batch_size=1).run(run.id)
    finally:
        database.finish_run(run, status="complete")
    assert (summary.documents_merged, summary.reviews_merged) == (2, 1)
    projected = _project(database)
    assert {item.document.id for item in projected} == {oldest.document.id}
    assert {item.id for item in projected} == {oldest.id, distinct}
    shared = next(item for item in projected if item.id == oldest.id)
    assert shared.source_graphs() == ["fixture", "second-source"]
    assert shared.source_record_keys == {
        a.source.record_key,
        b.source.record_key,
        bridge.source.record_key,
    }


def test_later_merge_updates_history_to_current_survivor(database: Database) -> None:
    a = _record("a", "https://factual.ro/a", "Claim", "Body alpha")
    b = _record("b", "https://factual.ro/b", "Claim", "Body beta")
    c = _record("c", "https://factual.ro/c", "Claim", "Body gamma")
    _install(database, [a], batch_size=1)
    first = _project(database)[0]
    _install(database, [a, b], batch_size=1)
    middle = next(
        item
        for item in _project(database)
        if item.review_url == b.document.observed_url
    )
    _install(database, [a, b, c], batch_size=1)
    last = next(
        item
        for item in _project(database)
        if item.review_url == c.document.observed_url
    )
    _extract(
        database,
        c,
        ProcessingResult.success(
            {"canonical_url": b.document.observed_url, "content": "Body gamma"}
        ),
    )
    _reconcile(database)
    _extract(
        database,
        b,
        ProcessingResult.success(
            {"canonical_url": a.document.observed_url, "content": "Body beta"}
        ),
    )
    _reconcile(database)
    (review,) = _project(database)
    assert review.id == first.id
    assert review.retired_ids == {middle.id, last.id}
    with database.pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT survivor_id, merged_into_id FROM claim_review_merges WHERE retired_id = %s",
            (last.id,),
        )
        assert cursor.fetchone() == (first.id, middle.id)
        cursor.execute(
            "SELECT survivor_id, merged_into_id FROM document_merges WHERE retired_id = %s",
            (last.document.id,),
        )
        assert cursor.fetchone() == (first.document.id, middle.document.id)


def test_reconciliation_failure_rolls_back_all_identity_writes(
    database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    a = _record("a", "https://factual.ro/a", "Claim", "Body alpha")
    b = _record("b", "https://factual.ro/b", "Claim", "Body beta")
    _install(database, [a, b], batch_size=1)
    before = {
        (item.id, item.document.id, item.review_text) for item in _project(database)
    }
    _extract(
        database,
        b,
        ProcessingResult.success(
            {
                "canonical_url": a.document.observed_url,
                "content": "New complete document content",
            }
        ),
    )
    install_evidence = IdentityRepository._install_evidence

    def fail_after_writes(repository: IdentityRepository) -> None:
        install_evidence(repository)
        raise RuntimeError("injected transaction failure")

    with monkeypatch.context() as context:
        context.setattr(IdentityRepository, "_install_evidence", fail_after_writes)
        with pytest.raises(RuntimeError, match="injected transaction failure"):
            _reconcile(database)
    assert {
        (item.id, item.document.id, item.review_text) for item in _project(database)
    } == before
    with database.pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT (SELECT COUNT(*) FROM document_merges), (SELECT COUNT(*) FROM claim_review_merges)"
        )
        assert cursor.fetchone() == (0, 0)
    summary, _ = _reconcile(database)
    assert (summary.documents_merged, summary.reviews_merged) == (1, 1)


def test_migrations_upgrade_existing_data_and_are_repeatable(
    database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = f"migration_{uuid4().hex}"
    document_id = uuid4()
    with database.pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        cursor.execute(
            sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema))
        )
        cursor.execute(
            cast(
                LiteralString,
                files("climatesense_kg.persistence.migrations")
                .joinpath("0001_schema.sql")
                .read_text(),
            )
        )
        cursor.execute(
            "CREATE TABLE schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"
        )
        cursor.execute(
            "INSERT INTO schema_migrations (name) VALUES ('0001_schema.sql')"
        )
        cursor.execute(
            "INSERT INTO documents (id, organization_uri, preferred_url) VALUES (%s, 'organization', 'https://example.org/article')",
            (document_id,),
        )
    try:
        with monkeypatch.context() as context:
            context.setenv("PGOPTIONS", f"-c search_path={schema}")
            with _database() as upgraded:
                upgraded.migrate()
                with (
                    upgraded.pool.connection() as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute("SELECT id, preferred_url FROM documents")
                    assert cursor.fetchall() == [
                        (document_id, "https://example.org/article")
                    ]
                    cursor.execute("SELECT COUNT(*) FROM document_merges")
                    assert cursor.fetchone() == (0,)
                    cursor.execute(
                        "SELECT COUNT(*) FROM schema_migrations WHERE name = '0002_identity_merges.sql'"
                    )
                    assert cursor.fetchone() == (1,)
    finally:
        with database.pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


def test_identity_rejects_observation_assigned_to_another_organization(
    database: Database,
) -> None:
    records = _records()
    _install(database, records, batch_size=2)
    foreign = next(
        item for item in _project(database) if "africacheck.org" in item.review_url
    )
    with database.pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "UPDATE source_observations SET claim_review_id = %s WHERE record_key = %s",
            (foreign.id, records[0].source.record_key),
        )
    with pytest.raises(RuntimeError, match="foreign review"):
        _reconcile(database)
    with database.pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM document_merges")
        assert cursor.fetchone() == (0,)


def test_failed_source_install_rolls_back_the_whole_snapshot(
    database: Database,
) -> None:
    records = _records()
    _install(database, records, batch_size=2)
    original = records[0]

    def broken_snapshot() -> Iterator[SourceReviewRecord]:
        yield replace(
            original,
            document=replace(original.document, source_text="uncommitted edit"),
        )
        raise RuntimeError("truncated source")

    run = database.start_run("broken")
    service = IngestionService(
        database.pool,
        Mock(),
        OrganizationCatalog(ORGANIZATION_CATALOG_PATH),
        batch_size=1,
    )
    try:
        with pytest.raises(RuntimeError, match="truncated source"):
            service.install_source(run.id, "fixture", broken_snapshot())
    finally:
        database.finish_run(run, status="failed")

    with database.pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT payload_hash FROM source_observations WHERE record_key = %s",
            (original.source.record_key,),
        )
        assert cursor.fetchone() == (original.payload_hash,)
        cursor.execute("SELECT COUNT(*) FROM source_observations WHERE active")
        assert cursor.fetchone() == (4,)


def test_changed_claim_preserves_uuid(
    database: Database,
) -> None:
    shared = " ".join(f"word{index}" for index in range(70))
    records = [
        _record("a", "https://factual.ro/review-a", "Old claim A", f"{shared} one"),
        _record("b", "https://factual.ro/review-b", "Old claim B", f"{shared} two"),
    ]
    first_ids, _documents, _reviews = _install(database, records, batch_size=1)
    revised = [
        replace(record, claim=CanonicalClaim("Revised claim")) for record in records
    ]

    second_ids, _documents, _reviews = _install(database, revised, batch_size=2)

    assert second_ids == first_ids
    projected = next(
        iter(
            ReviewProjectionReader(
                database.pool,
                OrganizationCatalog(ORGANIZATION_CATALOG_PATH).resolve,
            ).iter_batches(batch_size=2)
        )
    )
    assert {review.claim.text for review in projected} == {"Revised claim"}


def test_enrichment_outage_retry_and_success_reuse(database: Database) -> None:
    _install(database, [_records()[0]], batch_size=1)
    reader = ReviewProjectionReader(
        database.pool,
        OrganizationCatalog(ORGANIZATION_CATALOG_PATH).resolve,
    )
    enricher = _FixtureEnricher(
        [
            ProcessingResult.retryable({"error": "temporary"}),
            ProcessingResult.success({"value": "ready"}),
        ],
        available=False,
    )
    service = EnrichmentService(database.pool, reader, [enricher], batch_size=1)

    outage = service.run()[0]
    assert not outage.available
    assert outage.missing == 1
    assert enricher.compute_calls == 0

    enricher.available = True
    retry = service.run()[0]
    assert retry.retryable_failures == 1
    assert retry.missing == 1
    assert enricher.compute_calls == 1

    success = service.run()[0]
    assert success.succeeded == 1
    assert enricher.compute_calls == 2

    cached = service.run()[0]
    assert cached.cached == 1
    assert cached.complete
    assert enricher.compute_calls == 2


def test_permanent_enrichment_failure_is_retained_until_forced(
    database: Database,
) -> None:
    _install(database, [_records()[0]], batch_size=1)
    reader = ReviewProjectionReader(
        database.pool,
        OrganizationCatalog(ORGANIZATION_CATALOG_PATH).resolve,
    )
    enricher = _FixtureEnricher(
        [
            ProcessingResult.permanent_failure({"error": "unsupported"}),
            ProcessingResult.success({"value": "forced"}),
        ]
    )
    service = EnrichmentService(database.pool, reader, [enricher], batch_size=1)

    failed = service.run()[0]
    retained = service.run()[0]

    assert failed.permanent_failures == 1
    assert retained.permanent_failures == 1
    assert enricher.compute_calls == 1

    forced = service.run(ignore_cache=True)[0]
    assert forced.succeeded == 1
    assert enricher.compute_calls == 2


def test_extraction_reuse_retry_retention_and_ignore_cache(
    database: Database,
) -> None:
    record = _records()[0]
    _install(database, [record], batch_size=1)
    key = normalize_document_url(record.document.observed_url)
    assert key is not None
    target = DocumentTarget(key, record.document.observed_url)
    service = DocumentExtractionService(
        database.pool,
        batch_size=1,
        max_workers=1,
        rate_limit_delay=0,
    )
    success = ProcessingResult.success(
        {
            "content": "Extracted document body",
            "final_url": record.document.observed_url,
            "canonical_url": None,
        }
    )
    service._compute_many = Mock(  # type: ignore[method-assign]
        side_effect=lambda targets, _executor: [success] * len(targets)
    )

    computed = service._process_batch([target], offline=False, ignore_cache=False)
    cached = service._process_batch([target], offline=False, ignore_cache=False)

    assert computed.succeeded == 1
    assert cached.cached == 1
    assert service._compute_many.call_count == 1

    service._store(
        [
            (
                target,
                ProcessingResult.retryable(
                    {"error": "later"},
                    retry_at=datetime.now(UTC) + timedelta(hours=1),
                ),
            )
        ]
    )
    deferred = service._process_batch([target], offline=False, ignore_cache=False)
    assert deferred.retryable_failures == 1
    assert service._compute_many.call_count == 1

    service._store(
        [
            (
                target,
                ProcessingResult.permanent_failure({"error": "gone"}),
            )
        ]
    )
    retained = service._process_batch([target], offline=False, ignore_cache=False)
    forced = service._process_batch([target], offline=False, ignore_cache=True)
    assert retained.permanent_failures == 1
    assert forced.succeeded == 1
    assert service._compute_many.call_count == 2


def test_extraction_batches_distribute_hosts(database: Database) -> None:
    records = [
        _record(
            f"same-{index}",
            f"https://aaa.example/review-{index:03}",
            f"Same-host claim {index}",
            f"Same-host body {index}",
        )
        for index in range(128)
    ]
    records.extend(
        _record(
            f"mixed-{index}",
            f"https://host-{index:03}.example/review",
            f"Mixed-host claim {index}",
            f"Mixed-host body {index}",
        )
        for index in range(128)
    )
    run = database.start_run("host-distribution")
    ingestion = IngestionService(
        database.pool,
        Mock(),
        OrganizationCatalog(ORGANIZATION_CATALOG_PATH),
        batch_size=32,
        progress_interval_seconds=0,
    )
    try:
        ingestion.install_source(run.id, "fixture", records)
    finally:
        database.finish_run(run, status="complete")

    batches: list[list[DocumentTarget]] = []
    service = DocumentExtractionService(
        database.pool,
        batch_size=32,
        max_workers=32,
        rate_limit_delay=0,
        progress_interval_seconds=0,
    )
    service._process_batch = Mock(  # type: ignore[method-assign]
        side_effect=lambda targets, **_options: (
            batches.append(targets) or StageSummary(service.name)
        )
    )

    service.run()

    first_hosts = {urlparse(target.url).hostname for target in batches[0]}
    assert len(first_hosts) >= 8


def test_abandoned_run_is_recovered_and_concurrent_writer_is_rejected(
    database: Database,
) -> None:
    abandoned = database.start_run("abandoned")
    contender = _database()
    try:
        with pytest.raises(RuntimeError, match="already active"):
            contender.start_run("contender")
    finally:
        contender.close()

    database.close()
    recovered_database = _database()
    recovered = recovered_database.start_run("recovered")
    try:
        with recovered_database.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT status FROM pipeline_runs WHERE id = %s", (abandoned.id,)
                )
                assert cursor.fetchone() == ("failed",)
    finally:
        recovered_database.finish_run(recovered, status="complete")
        recovered_database.close()


def test_database_projection_streams_valid_source_rdf(
    database: Database,
    tmp_path: Path,
) -> None:
    _install(database, _records(), batch_size=2)
    catalog = OrganizationCatalog(ORGANIZATION_CATALOG_PATH)
    reader = ReviewProjectionReader(database.pool, catalog.resolve)
    enrichment = EnrichmentService(database.pool, reader, [], batch_size=2)
    report = RdfExporter(
        reader,
        enrichment,
        RDFGenerator("http://data.climatesense-project.eu"),
        output_path_template=str(tmp_path / "{SOURCE}.nt.gz"),
        enrichment_graphs={},
        batch_size=2,
        progress_interval_seconds=0,
    ).run(("fixture",), datetime(2026, 8, 13))

    graph = Graph().parse(
        data=gzip.decompress(report.artifacts[0].path.read_bytes()).decode("utf-8"),
        format="nt",
    )
    assert report.reviews == 3
    assert report.artifacts[0].complete
    assert len(graph) > 0
