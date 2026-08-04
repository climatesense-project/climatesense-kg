"""Tests for ClaimReviewData normalization."""

import json

from climatesense_kg.processors.claimreviewdata import ClaimReviewDataProcessor


def test_malformed_item_does_not_abort_later_records() -> None:
    payload = [
        {"claim_text": ["bad"], "review_url": "https://bad.test", "reviews": [None]},
        {
            "claim_text": ["valid claim"],
            "review_url": "https://example.test/review",
            "reviews": [{"original_label": "True", "label": "credible"}],
            "fact_checker": {"name": "Example"},
        },
    ]
    processor = ClaimReviewDataProcessor("claimreviewdata")

    results = list(processor.process(json.dumps(payload).encode()))

    assert len(results) == 1
    assert results[0].claim.text == "valid claim"


def test_emits_every_positionally_paired_claim_and_review() -> None:
    payload = [
        {
            "claim_text": ["first claim", "second claim"],
            "review_url": "https://example.test/review",
            "reviews": [
                {"original_label": "True", "label": "credible"},
                {"original_label": "False", "label": "not_credible"},
            ],
            "fact_checker": {"name": "Example"},
        }
    ]
    processor = ClaimReviewDataProcessor("claimreviewdata")

    results = list(processor.process(json.dumps(payload).encode()))

    assert [(result.claim.text, result.rating.label) for result in results] == [
        ("first claim", "credible"),
        ("second claim", "not_credible"),
    ]


def test_broadcasts_single_review_across_multiple_claims() -> None:
    payload = [
        {
            "claim_text": ["first claim", "second claim"],
            "review_url": "https://example.test/review",
            "reviews": [{"original_label": "False", "label": "not_credible"}],
            "fact_checker": {"name": "Example"},
        }
    ]
    processor = ClaimReviewDataProcessor("claimreviewdata")

    results = list(processor.process(json.dumps(payload).encode()))

    assert [result.claim.text for result in results] == [
        "first claim",
        "second claim",
    ]
