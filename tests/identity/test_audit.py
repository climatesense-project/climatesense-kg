"""Tests for exact, on-demand duplicate comparison."""

from unittest.mock import Mock
from uuid import UUID

from climatesense_kg.identity import DuplicateAuditor


def test_duplicate_comparison_is_exact_and_excludes_short_reviews() -> None:
    shared = [f"word{index}" for index in range(70)]
    reviews = [
        {
            "id": UUID("00000000-0000-0000-0000-000000000001"),
            "content": " ".join([*shared, "first ending"]),
            "word_count": 72,
        },
        {
            "id": UUID("00000000-0000-0000-0000-000000000002"),
            "content": " ".join([*shared, "second ending"]),
            "word_count": 72,
        },
        {
            "id": UUID("00000000-0000-0000-0000-000000000003"),
            "content": "too short for comparison",
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
    left_review_id, right_review_id, similarity, evidence = matches[0]
    assert left_review_id == reviews[0]["id"]
    assert right_review_id == reviews[1]["id"]
    assert similarity >= 0.9
    assert evidence.obj == {
        "kind": "body_similarity",
        "same_organization": True,
        "same_claim": True,
        "left_word_count": 72,
        "right_word_count": 72,
    }
