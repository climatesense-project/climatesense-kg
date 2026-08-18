"""PostgreSQL contracts for the authoritative pipeline data path.

Set ``TEST_POSTGRES_DB`` (and optional ``TEST_POSTGRES_*`` connection variables)
to run these tests against a disposable database. The database contents are erased.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import gzip
import os
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import urlparse

import pytest
from rdflib import Graph

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
from climatesense_kg.identity import IdentityService
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
        ).run()
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
