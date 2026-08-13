"""Tests for exact, on-demand duplicate comparison."""

import json
from unittest.mock import Mock
from uuid import UUID

from climatesense_kg.identity import DuplicateAuditor


def test_duplicate_comparison_is_exact_and_excludes_short_reviews() -> None:
    shared = [f"word{index}" for index in range(70)]
    reviews = [
        {
            "id": UUID("00000000-0000-0000-0000-000000000001"),
            "record_key": "first",
            "extracted_text": " ".join([*shared, "first ending"]),
            "word_count": 72,
        },
        {
            "id": UUID("00000000-0000-0000-0000-000000000002"),
            "record_key": "second",
            "extracted_text": " ".join([*shared, "second ending"]),
            "word_count": 72,
        },
        {
            "id": UUID("00000000-0000-0000-0000-000000000003"),
            "record_key": "short",
            "extracted_text": "too short for comparison",
            "word_count": 4,
        },
    ]
    auditor = DuplicateAuditor(
        Mock(),
        similarity_threshold=0.9,
        minimum_similarity_words=50,
    )

    candidate_pairs, eligible_pairs, matches = auditor._compare_group(reviews)

    assert candidate_pairs == 3
    assert eligible_pairs == 1
    assert len(matches) == 1
    source_record_key, candidate_review_id, similarity, evidence_json = matches[0]
    assert source_record_key == "second"
    assert candidate_review_id == reviews[0]["id"]
    assert similarity >= 0.9
    assert json.loads(evidence_json) == {
        "kind": "body_similarity",
        "same_organization": True,
        "same_claim": True,
        "left_word_count": 72,
        "right_word_count": 72,
    }
