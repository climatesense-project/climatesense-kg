"""Tests for ClaimReviewData normalization."""

from io import BytesIO
import json

import pytest

from climatesense_kg.processors.claimreviewdata import ClaimReviewDataProcessor


def test_malformed_item_does_not_abort_later_records() -> None:
    payload = [
        {"claim_text": ["bad"], "review_url": "https://bad.test", "reviews": [None]},
        {
            "claim_text": ["valid claim"],
            "review_url": "https://example.test/review",
            "reviews": [{"original_label": "True", "label": "credible"}],
            "fact_checker": {"name": "Example", "website": "https://example.test"},
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
            "fact_checker": {"name": "Example", "website": "https://example.test"},
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
            "fact_checker": {"name": "Example", "website": "https://example.test"},
        }
    ]
    processor = ClaimReviewDataProcessor("claimreviewdata")

    results = list(processor.process(json.dumps(payload).encode()))

    assert [result.claim.text for result in results] == [
        "first claim",
        "second claim",
    ]


def test_skips_url_only_claim_text() -> None:
    payload = [
        {
            "claim_text": ["https://social.example/post/1"],
            "review_url": "https://example.test/review",
            "reviews": [{"original_label": "False", "label": "not_credible"}],
            "fact_checker": {
                "name": "Example",
                "website": "https://example.test",
            },
        }
    ]
    processor = ClaimReviewDataProcessor("claimreviewdata")

    assert list(processor.process(json.dumps(payload).encode())) == []


def test_skips_only_the_invalid_pair_in_a_multi_claim_record() -> None:
    payload = [
        {
            "claim_text": ["https://social.example/post/1", "A meaningful claim"],
            "review_url": "https://example.test/review",
            "reviews": [
                {"original_label": "False", "label": "not_credible"},
                {"original_label": "True", "label": "credible"},
            ],
            "fact_checker": {
                "name": "Example",
                "website": "https://example.test",
            },
        }
    ]
    processor = ClaimReviewDataProcessor("claimreviewdata")

    results = list(processor.process(json.dumps(payload).encode()))

    assert [(result.claim.text, result.rating.label) for result in results] == [
        ("A meaningful claim", "credible")
    ]


def test_ignores_null_values_in_optional_appearances() -> None:
    payload = [
        {
            "claim_text": ["A meaningful claim"],
            "appearances": [None, "https://social.example/post/1"],
            "review_url": "https://example.test/review",
            "reviews": [{"original_label": "True", "label": "credible"}],
            "fact_checker": {
                "name": "Example",
                "website": "https://example.test",
            },
        }
    ]

    results = list(
        ClaimReviewDataProcessor("claimreviewdata").process(
            json.dumps(payload).encode()
        )
    )

    assert results[0].claim.appearances == ["https://social.example/post/1"]


def test_streaming_parser_handles_values_across_read_boundaries() -> None:
    claim = "a" * (64 * 1024)
    payload = json.dumps(
        [
            {
                "claim_text": [claim],
                "review_url": "https://example.test/review",
                "reviews": [{"original_label": "True", "label": "credible"}],
                "fact_checker": {
                    "name": "Example",
                    "website": "https://example.test",
                },
            }
        ]
    ).encode()

    results = list(
        ClaimReviewDataProcessor("claimreviewdata").process_stream(BytesIO(payload))
    )

    assert [result.claim.text for result in results] == [claim]


@pytest.mark.parametrize(
    "payload",
    [
        b'[{"claim_text": []}',
        b"[] trailing-data",
        b'{"not": "an array"}',
    ],
)
def test_invalid_top_level_payload_aborts_the_source(payload: bytes) -> None:
    processor = ClaimReviewDataProcessor("claimreviewdata")

    with pytest.raises((json.JSONDecodeError, ValueError)):
        list(processor.process_stream(BytesIO(payload)))
