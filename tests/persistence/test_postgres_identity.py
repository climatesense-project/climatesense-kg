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
