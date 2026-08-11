"""Tests for the set-based PostgreSQL identity repository."""

from unittest.mock import MagicMock

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalOrganization,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
)
from climatesense_kg.identity.models import IdentityBatchEvidence
from climatesense_kg.identity.planner import IdentityPlanner
from climatesense_kg.persistence.postgres_identity import PostgresIdentityBatch

ORGANIZATION = CanonicalOrganization(
    uri="https://data.example.test/organization/factual",
    name="Factual",
    website="https://factual.ro",
)


def _record(record_name: str) -> SourceReviewRecord:
    url = f"https://factual.ro/{record_name}"
    claim = CanonicalClaim(text=f"Claim {record_name}")
    return SourceReviewRecord(
        source=SourceReference.from_observation(
            source_name="source",
            source_type="dataset",
            observed_url=url,
            claim_text=claim.text,
            native_id=f"native-{record_name}",
        ),
        claim=claim,
        organization=OrganizationReference(
            name=ORGANIZATION.name,
            website=ORGANIZATION.website,
        ),
        document=ReviewDocument(observed_url=url),
    )


def _empty_evidence() -> IdentityBatchEvidence:
    return IdentityBatchEvidence(
        assignments_by_source_key={},
        assignments_by_native_key={},
        documents={},
        reviews={},
        assignments={},
        review_claims={},
    )


def test_evidence_loading_uses_a_fixed_number_of_set_queries() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[], [], []]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    repository = PostgresIdentityBatch(connection)
    records = [(_record(str(index)), ORGANIZATION) for index in range(50)]

    evidence = repository.load_evidence(records)

    assert evidence == _empty_evidence()
    assert cursor.execute.call_count == 3
    direct_query, direct_parameters = cursor.execute.call_args_list[0].args
    assert "unnest(%s::text[], %s::text[])" in direct_query
    assert len(direct_parameters[2]) == 50
    exact_query, exact_parameters = cursor.execute.call_args_list[1].args
    assert "normalized_text_hash = ANY(%s::text[])" in exact_query
    assert "%s IS NOT NULL" not in exact_query
    assert exact_parameters[1] == []
    claim_query, claim_parameters = cursor.execute.call_args_list[2].args
    assert "unnest(%s::text[], %s::text[])" in claim_query
    assert len(claim_parameters[0]) == 50


def test_commit_bulk_writes_each_plan_collection() -> None:
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    repository = PostgresIdentityBatch(connection)
    record = _record("one")
    plan = IdentityPlanner().plan([(record, ORGANIZATION)], _empty_evidence())

    repository.commit(plan)

    assert cursor.executemany.call_count == 3
    argument_batches = [call.args[1] for call in cursor.executemany.call_args_list]
    assert [len(arguments) for arguments in argument_batches] == [1, 1, 1]
    assert cursor.execute.call_count == 1
