"""Wiring tests for enrichment providers built from pipeline configuration."""

from pathlib import Path

from climatesense_kg.bootstrap import _build_enrichers, _enrichment_graphs
from climatesense_kg.config import PipelineConfig, load_config
from climatesense_kg.enrichers import (
    OpenTapiocaEnricher,
    RefinedEnricher,
    SparqlEntityPropertyEnricher,
)


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
