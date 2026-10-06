"""Behavioral tests for the OpenTapioca enricher."""

from unittest.mock import Mock, patch

from climatesense_kg.enrichers import OpenTapiocaEnricher

_OPENTAPIOCA_RESPONSE = {
    "text": "Emmanuel Macron visited Germany.",
    "annotations": [
        {
            "start": 0,
            "end": 15,
            "tags": [
                {
                    "id": "Q3052772",
                    "label": "Emmanuel Macron",
                    "types": {"Q5": True, "Q82955": True, "Q43229": False},
                    "score": 0.999,
                    "nb_sitelinks": 120,
                },
                {"id": "Q42", "types": {}, "score": 0.001, "nb_sitelinks": 0},
            ],
            "best_qid": "Q3052772",
            "best_tag_label": "Emmanuel Macron",
        },
        {"start": 24, "end": 31, "tags": [], "best_qid": None},
    ],
}


def test_opentapioca_computes_and_applies_entity_payload(make_review) -> None:
    enricher = OpenTapiocaEnricher(target="claim")
    review = make_review(claim="Emmanuel Macron visited Germany.")
    response = Mock(status_code=200)
    response.json.return_value = _OPENTAPIOCA_RESPONSE
    with patch("requests.post", return_value=response):
        subject = enricher.subjects([review])[0]
        result = enricher.compute_batch([subject])[0]
        enricher.apply(subject, result.payload)

    assert result.succeeded
    [entity] = review.claim.analysis.entities
    assert entity.uri == "http://www.wikidata.org/entity/Q3052772"
    assert entity.source == "opentapioca"
    assert entity.surface_form == "Emmanuel Macron"
    assert entity.types == [
        "http://www.wikidata.org/entity/Q5",
        "http://www.wikidata.org/entity/Q82955",
    ]
    assert entity.confidence is None
    assert entity.support == 120
    assert entity.offset == 0


def test_opentapioca_confidence_threshold_filters_low_score_mentions(
    make_review,
) -> None:
    enricher = OpenTapiocaEnricher(target="claim", confidence=0.5)
    review = make_review(claim="Emmanuel Macron visited Germany.")
    response = Mock(status_code=200)
    response.json.return_value = {
        "text": "Emmanuel Macron visited Germany.",
        "annotations": [
            {
                "start": 24,
                "end": 31,
                "tags": [{"id": "Q183", "types": [], "score": 0.3, "nb_sitelinks": 5}],
                "best_qid": "Q183",
                "best_tag_label": "Germany",
            }
        ],
    }
    with patch("requests.post", return_value=response):
        result = enricher.compute_batch([enricher.subjects([review])[0]])[0]

    assert result.succeeded
    assert review.claim.analysis.entities == []


def test_opentapioca_claim_subject_is_shared_by_claim_uri(make_review) -> None:
    enricher = OpenTapiocaEnricher(target="claim")
    reviews = [make_review(), make_review()]

    subjects = enricher.subjects(reviews)

    assert len(subjects) == 1
    assert len(subjects[0].targets) == 2
    assert subjects[0].key == reviews[0].claim.uri


def test_opentapioca_review_subject_is_shared_by_exact_body(make_review) -> None:
    enricher = OpenTapiocaEnricher(target="review")
    reviews = [
        make_review(body="The same exact body"),
        make_review(body="The same exact body"),
    ]

    subjects = enricher.subjects(reviews)

    assert len(subjects) == 1
    assert subjects[0].key.startswith("review-text/")


def test_opentapioca_healthcheck_probes_the_annotate_endpoint() -> None:
    enricher = OpenTapiocaEnricher(target="claim")
    response = Mock(status_code=200)
    with patch("requests.post", return_value=response) as post:
        assert enricher.is_available()

    assert post.call_args.kwargs["data"]["query"] == "test"


def test_opentapioca_confidence_is_semantic_configuration() -> None:
    baseline = OpenTapiocaEnricher(target="claim", confidence=0.5)
    operational_change = OpenTapiocaEnricher(
        target="claim", confidence=0.5, timeout=999
    )
    semantic_change = OpenTapiocaEnricher(target="claim", confidence=0.9)

    assert baseline.config_hash == operational_change.config_hash
    assert baseline.config_hash != semantic_change.config_hash


def test_opentapioca_worker_count_is_operational_configuration() -> None:
    serial = OpenTapiocaEnricher(target="claim", max_workers=1)
    concurrent = OpenTapiocaEnricher(target="claim", max_workers=8)

    assert serial.config_hash == concurrent.config_hash
