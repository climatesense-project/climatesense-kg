"""Tests for PostgreSQL identity query construction."""

from unittest.mock import MagicMock

from climatesense_kg.persistence.postgres_identity import (
    PostgresIdentityTransaction,
)


def test_document_evidence_accepts_missing_text_hash() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    transaction = PostgresIdentityTransaction(connection)

    matches = transaction.documents_by_evidence(
        "https://example.test/organization",
        {"https://example.test/review"},
        None,
    )

    assert matches == []
    query, parameters = cursor.execute.call_args.args
    assert "d.normalized_text_hash = %s" in query
    assert "%s IS NOT NULL" not in query
    assert parameters[1] is None


def test_prepared_batch_avoids_per_record_assignment_lookup() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    transaction = PostgresIdentityTransaction(connection)
    record = MagicMock()
    record.source.record_key = "source-record"
    record.source.source_name = "source"
    record.source.native_id = "native-id"
    record.document.observed_url = "https://example.test/review"
    record.document.final_url = None
    record.document.canonical_url = None
    record.document.normalized_text_hash = None
    organization = MagicMock()
    organization.uri = "https://example.test/organization"

    transaction.prepare([(record, organization)])

    query, parameters = cursor.execute.call_args_list[0].args
    assert "record_key = ANY(%s::text[])" in query
    assert parameters == (["source-record"], ["source"], ["native-id"])
    assert cursor.execute.call_count == 2
    cursor.reset_mock()

    assert transaction.assignment_for_source("source-record") is None
    assert transaction.assignment_for_native_id("source", "native-id") is None
    assert (
        transaction.documents_by_evidence(
            organization.uri,
            {record.document.observed_url},
            None,
        )
        == []
    )
    cursor.execute.assert_not_called()
