"""Tests for DE FACTO normalization."""

import json

from climatesense_kg.processors.defacto import DefactoProcessor


def test_rejects_null_titles_and_missing_review_urls() -> None:
    payload = [
        {"title": None, "rawTitle": None, "absoluteUrl": "https://example.test/a"},
        {"title": "Claim without URL", "absoluteUrl": None},
        {"title": "Claim with relative URL", "absoluteUrl": "/relative"},
    ]

    results = list(DefactoProcessor("defacto").process(json.dumps(payload).encode()))

    assert results == []


def test_emits_valid_title_and_absolute_review_url() -> None:
    payload = [
        {
            "title": "  A valid claim  ",
            "absoluteUrl": "https://example.test/review",
            "id": "xwiki:Medias.Example.Fact-check.WebHome",
            "organization_url": "https://www.example.org/about",
        }
    ]

    results = list(DefactoProcessor("defacto").process(json.dumps(payload).encode()))

    assert len(results) == 1
    assert results[0].claim.text == "A valid claim"
    assert results[0].document.observed_url == "https://example.test/review"
    assert results[0].organization.website == "https://example.org"


def test_rejects_fact_check_without_organization_url() -> None:
    payload = [
        {
            "title": "A valid claim",
            "absoluteUrl": "https://example.test/review",
            "id": "xwiki:Medias.Example.Fact-check.WebHome",
        }
    ]

    results = list(DefactoProcessor("defacto").process(json.dumps(payload).encode()))

    assert results == []
