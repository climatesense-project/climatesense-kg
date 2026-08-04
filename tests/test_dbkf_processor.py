"""Tests for DBKF normalization."""

import json

from climatesense_kg.processors.dbkf import DbkfProcessor


def test_malformed_item_does_not_abort_later_records() -> None:
    payload = [
        {
            "id": "bad",
            "externalUrl": "https://bad.test/review",
            "headline": "Bad nested value",
            "itemReviewed": None,
        },
        {
            "id": "valid",
            "externalUrl": "https://example.test/review",
            "headline": "Valid review",
            "itemReviewed": {"text": "valid claim"},
            "publisher": {"name": "Example"},
        },
    ]

    results = list(DbkfProcessor("dbkf").process(json.dumps(payload).encode()))

    assert len(results) == 1
    assert results[0].claim.text == "valid claim"
