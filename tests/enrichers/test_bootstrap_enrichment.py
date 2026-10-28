"""Wiring tests for enrichment providers built from pipeline configuration."""

from pathlib import Path
from unittest.mock import patch

import pytest

from climatesense_kg.bootstrap import _build_enrichers, _enrichment_graphs
from climatesense_kg.config import PipelineConfig, load_config
from climatesense_kg.enrichers import (
    CARDSEnricher,
    OpenTapiocaEnricher,
    RefinedEnricher,
    SparqlEntityPropertyEnricher,
)
from climatesense_kg.enrichment import order_enrichers


def _config(tmp_path: Path, enrichment_yaml: str) -> PipelineConfig:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"enrichment:\n{enrichment_yaml}output:\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )
    return load_config(config_path)


def _enricher_names(config: PipelineConfig) -> list[str]:
    return [enricher.name for enricher in _build_enrichers(config)]


def test_disabled_providers_build_nothing(tmp_path: Path) -> None:
    config = _config(tmp_path, "  progress_interval_seconds: 10\n")

    assert _build_enrichers(config) == []
    assert _enrichment_graphs(config) == {}


def test_spotlight_builds_both_targets_and_the_dbpedia_graph(tmp_path: Path) -> None:
    config = _config(tmp_path, "  dbpedia_spotlight:\n    enabled: true\n")

    assert _enricher_names(config) == [
        "dbpedia_spotlight.claim",
        "dbpedia_spotlight.review",
    ]
    assert _enrichment_graphs(config) == {
        "dbpedia-enricher": {"dbpedia_spotlight"},
    }


def test_wikidata_linkers_build_both_targets_and_the_wikidata_graph(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path,
        "  opentapioca:\n"
        "    enabled: true\n"
        "  refined:\n"
        "    enabled: true\n"
        "  wikidata_entity_properties:\n"
        "    enabled: true\n"
        "    properties:\n"
        "      - http://www.wikidata.org/prop/direct/P31\n",
    )

    enrichers = _build_enrichers(config)
    assert _enricher_names(config) == [
        "opentapioca.claim",
        "opentapioca.review",
        "refined.claim",
        "refined.review",
        "wikidata_entity_properties",
    ]
    assert all(
        isinstance(enricher, OpenTapiocaEnricher | RefinedEnricher)
        for enricher in enrichers[:4]
    )
    assert isinstance(enrichers[4], SparqlEntityPropertyEnricher)
    assert enrichers[4].entity_sources == {"opentapioca", "refined"}
    assert _enrichment_graphs(config) == {
        "wikidata-enricher": {"opentapioca", "refined"},
    }


def test_wikidata_graph_requires_only_one_linker(tmp_path: Path) -> None:
    config = _config(tmp_path, "  refined:\n    enabled: true\n")

    assert [type(enricher) for enricher in _build_enrichers(config)] == [
        RefinedEnricher,
        RefinedEnricher,
    ]
    assert _enrichment_graphs(config) == {
        "wikidata-enricher": {"opentapioca", "refined"},
    }


def test_entity_property_enrichers_run_after_every_linker(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        "  dbpedia_spotlight:\n"
        "    enabled: true\n"
        "  dbpedia_entity_properties:\n"
        "    enabled: true\n"
        "  opentapioca:\n"
        "    enabled: true\n"
        "  refined:\n"
        "    enabled: true\n"
        "  wikidata_entity_properties:\n"
        "    enabled: true\n"
        "    properties:\n"
        "      - http://www.wikidata.org/prop/direct/P31\n",
    )

    enrichers = _build_enrichers(config)
    assert enrichers[2].requires == {"entities"}
    assert all(
        enricher.provides == {"entities"}
        for enricher in enrichers
        if enricher.name.startswith(("dbpedia_spotlight", "opentapioca", "refined"))
    )

    ordered = order_enrichers(enrichers)
    assert [enricher.name for enricher in ordered] == [
        "dbpedia_spotlight.claim",
        "dbpedia_spotlight.review",
        "opentapioca.claim",
        "opentapioca.review",
        "refined.claim",
        "refined.review",
        "dbpedia_entity_properties",
        "wikidata_entity_properties",
    ]


def test_cards_builds_one_claim_enricher_with_its_classifier_settings(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path,
        "  cards:\n"
        "    enabled: true\n"
        "    classifier: matcher\n"
        "    min_threshold: 0.4\n"
        "    max_workers: 2\n",
    )

    enrichers = _build_enrichers(config)

    assert [enricher.name for enricher in enrichers] == ["cards"]
    assert isinstance(enrichers[0], CARDSEnricher)
    assert enrichers[0].semantic_config["classifier"] == "matcher"
    assert enrichers[0].semantic_config["min_threshold"] == 0.4
    assert enrichers[0].max_workers == 2
    assert _enrichment_graphs(config) == {}


@patch.dict(
    "os.environ",
    {
        "CIMPLE_FACTORS_API_URL": "https://cimple.example",
        "CIMPLE_FACTORS_API_KEY": "test-key",
    },
)
def test_cards_climate_gate_declares_the_cimple_provider(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        "  cimple:\n"
        "    enabled: true\n"
        "  cards:\n"
        "    enabled: true\n"
        "    climate_related_gate: true\n",
    )

    enrichers = {enricher.name: enricher for enricher in _build_enrichers(config)}

    assert enrichers["cards"].requires == {"climate_related"}
    assert enrichers["cimple.climate_related"].provides == {"climate_related"}


def test_cards_climate_gate_requires_the_cimple_provider(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        "  cards:\n    enabled: true\n    climate_related_gate: true\n",
    )

    with pytest.raises(ValueError, match="climate_related"):
        order_enrichers(_build_enrichers(config))


def test_cards_llm_concurrency_builds_into_the_enricher(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        "  cards:\n    enabled: true\n    classifier: llm\n    concurrency: 4\n",
    )

    enrichers = _build_enrichers(config)

    assert isinstance(enrichers[0], CARDSEnricher)
    assert enrichers[0].concurrency == 4
