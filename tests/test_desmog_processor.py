"""Tests for DeSmog data normalization."""

import pytest

from climatesense_kg.processors.desmog import DesmogProcessor


@pytest.mark.parametrize("claim_text", ["b0", "The"])
def test_skips_extraction_artifact_claim_text(claim_text: str) -> None:
    payload = f"""
        @prefix schema: <https://schema.org/> .

        <https://source.example/claim/1>
            a schema:Claim ;
            schema:abstract "{claim_text}" ;
            schema:publisher <https://www.desmog.com/> ;
            schema:url "https://www.desmog.com/example/" .
    """.encode()
    processor = DesmogProcessor("desmog")

    assert list(processor.process(payload)) == []
