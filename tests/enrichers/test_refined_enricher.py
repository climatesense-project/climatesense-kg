"""Behavioral tests for the Refined enricher."""

from unittest.mock import Mock, patch

from climatesense_kg.enrichers import RefinedEnricher

_REFINED_RESPONSE = {
    "text": "Emmanuel Macron visited Germany in 2017.",
    "spans": [
        {
            "text": "Emmanuel Macron",
            "start": 0,
            "end": 15,
            "coarse_type": "MENTION",
            "mention_type": "PERSON",
            "date": None,
            "confidence": 0.9998,
            "entity": {
                "qid": "Q3052772",
                "label": None,
                "wikipedia_title": "Emmanuel Macron",
            },
            "types": [{"id": "Q5", "label": "human", "confidence": 1.0}],
        },
        {
            "text": "2017",
            "start": 35,
            "end": 39,
            "coarse_type": "DATE",
            "mention_type": None,
            "date": None,
            "confidence": 0.9,
            "entity": {"qid": "Q25290", "label": None, "wikipedia_title": "2017"},
            "types": [],
        },
        {
            "text": "Germany",
            "start": 24,
            "end": 31,
            "coarse_type": "MENTION",
            "mention_type": "ORG",
            "date": None,
            "confidence": 0.97,
            "entity": {"qid": None, "label": None, "wikipedia_title": None},
            "types": [],
        },
    ],
}


def test_refined_computes_and_applies_entity_payload(make_review) -> None:
    enricher = RefinedEnricher(target="claim")
    review = make_review(claim="Emmanuel Macron visited Germany in 2017.")
    response = Mock(status_code=200)
    response.json.return_value = _REFINED_RESPONSE
    with patch("requests.post", return_value=response):
        subject = enricher.subjects([review])[0]
        result = enricher.compute_batch([subject])[0]
        enricher.apply(subject, result.payload)

    assert result.succeeded
    [entity] = review.claim.analysis.entities
    assert entity.uri == "http://www.wikidata.org/entity/Q3052772"
    assert entity.source == "refined"
    assert entity.surface_form == "Emmanuel Macron"
    assert entity.types == ["http://www.wikidata.org/entity/Q5"]
    assert entity.confidence == 0.9998
    assert entity.support is None
    assert entity.offset == 0


def test_refined_confidence_threshold_filters_low_score_mentions(make_review) -> None:
    enricher = RefinedEnricher(target="claim", confidence=0.99)
    review = make_review(claim="Emmanuel Macron visited Germany in 2017.")
    response = Mock(status_code=200)
    response.json.return_value = {
        "text": "Emmanuel Macron visited Germany in 2017.",
        "spans": [
            {
                "text": "Germany",
                "start": 24,
                "end": 31,
                "coarse_type": "MENTION",
                "confidence": 0.97,
                "entity": {"qid": "Q183", "label": None, "wikipedia_title": "Germany"},
                "types": [],
            }
        ],
    }
    with patch("requests.post", return_value=response):
        result = enricher.compute_batch([enricher.subjects([review])[0]])[0]

    assert result.succeeded
    assert review.claim.analysis.entities == []


def test_refined_claim_subject_is_shared_by_claim_uri(make_review) -> None:
    enricher = RefinedEnricher(target="claim")
    reviews = [make_review(), make_review()]

    subjects = enricher.subjects(reviews)

    assert len(subjects) == 1
    assert len(subjects[0].targets) == 2
    assert subjects[0].key == reviews[0].claim.uri


def test_refined_review_subject_is_shared_by_exact_body(make_review) -> None:
    enricher = RefinedEnricher(target="review")
    reviews = [
        make_review(body="The same exact body"),
        make_review(body="The same exact body"),
    ]

    subjects = enricher.subjects(reviews)

    assert len(subjects) == 1
    assert subjects[0].key.startswith("review-text/")


def test_refined_healthcheck_probes_the_annotate_endpoint() -> None:
    enricher = RefinedEnricher(target="claim")
    response = Mock(status_code=200)
    with patch("requests.post", return_value=response) as post:
        assert enricher.is_available()

    assert post.call_args.kwargs["json"] == {"text": "test"}


def test_refined_confidence_is_semantic_configuration() -> None:
    baseline = RefinedEnricher(target="claim", confidence=0.5)
    operational_change = RefinedEnricher(target="claim", confidence=0.5, timeout=999)
    semantic_change = RefinedEnricher(target="claim", confidence=0.9)

    assert baseline.config_hash == operational_change.config_hash
    assert baseline.config_hash != semantic_change.config_hash


def test_refined_worker_count_is_operational_configuration() -> None:
    serial = RefinedEnricher(target="claim", max_workers=1)
    concurrent = RefinedEnricher(target="claim", max_workers=8)

    assert serial.config_hash == concurrent.config_hash
