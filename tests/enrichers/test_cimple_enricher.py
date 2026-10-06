"""Behavioral tests for the CIMPLE model enricher."""

from unittest.mock import Mock, patch

import pytest

from climatesense_kg.enrichers import CimpleModelEnricher
from climatesense_kg.processing import ProcessingResult


@patch.dict(
    "os.environ",
    {
        "CIMPLE_FACTORS_API_URL": "https://factors.example.test",
        "CIMPLE_FACTORS_API_KEY": "test-key",
    },
)
def test_cimple_semantic_configuration_controls_cache_identity() -> None:
    first = CimpleModelEnricher(
        model="emotion", model_version="1", max_length=128, rate_limit_delay=0
    )
    operational_change = CimpleModelEnricher(
        model="emotion",
        model_version="1",
        max_length=128,
        timeout=999,
        rate_limit_delay=0,
    )
    semantic_change = CimpleModelEnricher(
        model="emotion", model_version="2", max_length=128, rate_limit_delay=0
    )

    assert first.config_hash == operational_change.config_hash
    assert first.config_hash != semantic_change.config_hash


@patch.dict(
    "os.environ",
    {
        "CIMPLE_FACTORS_API_URL": "https://factors.example.test",
        "CIMPLE_FACTORS_API_KEY": "test-key",
    },
)
def test_cimple_batch_result_is_applied_to_claim_analysis(make_review) -> None:
    enricher = CimpleModelEnricher(model="emotion", rate_limit_delay=0)
    review = make_review()
    subject = enricher.subjects([review])[0]
    with patch.object(
        enricher,
        "_call_model",
        return_value=[{"value": "concern"}],
    ):
        result = enricher.compute_batch([subject])[0]
    enricher.apply(subject, result.payload)

    assert result == ProcessingResult.success({"value": "concern"})
    assert review.claim.analysis.emotion == "concern"


def test_cimple_requires_absolute_api_url() -> None:
    with (
        patch.dict("os.environ", {"CIMPLE_FACTORS_API_URL": ""}),
        pytest.raises(
            ValueError,
            match="CIMPLE_FACTORS_API_URL must be an absolute HTTP\\(S\\) URL",
        ),
    ):
        CimpleModelEnricher(model="emotion")


def test_cimple_requires_non_empty_api_key() -> None:
    with (
        patch.dict(
            "os.environ",
            {
                "CIMPLE_FACTORS_API_URL": "https://factors.example.test",
                "CIMPLE_FACTORS_API_KEY": "",
            },
        ),
        pytest.raises(
            ValueError,
            match="CIMPLE_FACTORS_API_KEY must be set to a non-empty API key",
        ),
    ):
        CimpleModelEnricher(model="emotion")


def test_cimple_sends_api_key_header_on_model_calls() -> None:
    with patch.dict(
        "os.environ",
        {
            "CIMPLE_FACTORS_API_URL": "https://factors.example.test",
            "CIMPLE_FACTORS_API_KEY": "secret-key",
        },
    ):
        enricher = CimpleModelEnricher(model="emotion", rate_limit_delay=0)
    response = Mock(status_code=200)
    response.json.return_value = {"results": [{"value": "concern"}]}
    with patch("requests.post", return_value=response) as post:
        responses = enricher._call_model(["A climate claim"])

    assert responses == [{"value": "concern"}]
    assert post.call_args.kwargs["headers"]["X-API-Key"] == "secret-key"
