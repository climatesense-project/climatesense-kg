"""Tests for versioned stage result identity."""

from climatesense_kg.persistence import (
    InMemoryStageResultStore,
    StageResult,
    StageResultKey,
)


def test_stage_key_invalidates_on_input_config_or_version_change() -> None:
    def key(
        *,
        version: str = "2",
        text: str = "claim",
        confidence: float = 0.5,
    ) -> StageResultKey:
        return StageResultKey.build(
            subject_key="review-id",
            stage_name="dbpedia",
            stage_version=version,
            input_value={"text": text},
            config_value={"confidence": confidence},
        )

    baseline = key()

    assert key() == baseline
    assert key(version="3") != baseline
    assert key(text="edited") != baseline
    assert key(confidence=0.8) != baseline


def test_stage_store_preserves_explicit_failure_results() -> None:
    store = InMemoryStageResultStore()
    key = StageResultKey.build(
        subject_key="review-id",
        stage_name="extract-document",
        stage_version="1",
        input_value="https://example.test/review",
        config_value={"timeout": 10},
    )
    failure = StageResult(success=False, payload={"error": "timeout"})

    store.put(key, failure)

    assert store.get(key) == failure


def test_stage_result_flush_preserves_a_separate_identity_boundary() -> None:
    store = InMemoryStageResultStore()
    key = StageResultKey.build(
        subject_key="claim/example",
        stage_name="enrichment.example",
        stage_version="1",
        input_value="claim",
        config_value={},
    )
    store.put(key, StageResult(success=True, payload={}))

    assert store.clear() == 1
    assert store.get(key) is None
