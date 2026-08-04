"""Tests for EuroClimateCheck normalization."""

import json

from climatesense_kg.processors.euroclimatecheck import EuroClimateCheckProcessor


def test_content_only_record_produces_non_empty_claim() -> None:
    payload = [
        {
            "url": "https://example.test/review",
            "content": "Claim from the content field",
            "source": "Example",
            "category": "False",
        }
    ]

    results = list(
        EuroClimateCheckProcessor("euroclimatecheck").process(
            json.dumps(payload).encode()
        )
    )

    assert len(results) == 1
    assert results[0].claim.text == "Claim from the content field"
