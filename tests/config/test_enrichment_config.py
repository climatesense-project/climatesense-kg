"""Tests for enrichment configuration validation."""

from pathlib import Path

import pytest

from climatesense_kg.config import load_config


def test_dbpedia_properties_require_spotlight(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "enrichment:\n"
        "  dbpedia_entity_properties:\n"
        "    enabled: true\n"
        "output:\n"
        "  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="require DBpedia Spotlight"):
        load_config(config_path)


def test_wikidata_properties_require_a_wikidata_linker(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "enrichment:\n"
        "  wikidata_entity_properties:\n"
        "    enabled: true\n"
        "output:\n"
        "  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="require OpenTapioca or ReFinED"):
        load_config(config_path)


@pytest.mark.parametrize(
    "section",
    [
        "  dbpedia_spotlight:\n    max_workers: 0\n",
        "  opentapioca:\n    max_workers: 0\n",
        "  refined:\n    max_workers: 0\n",
        "  cimple:\n    max_workers: 0\n",
        "  cards:\n    max_workers: 0\n",
    ],
)
def test_enrichment_worker_counts_must_be_positive(
    tmp_path: Path, section: str
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"enrichment:\n{section}output:\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="max_workers must be positive"):
        load_config(config_path)


@pytest.mark.parametrize(
    ("section", "message"),
    [
        ("  cards:\n    batch_size: 0\n", "batch_size must be positive"),
        ("  cards:\n    min_threshold: 1.5\n", "min_threshold must be between"),
        ("  cards:\n    languages: []\n", "languages must not be empty"),
        (
            "  cards:\n    classifier: matcher\n    use_preclassifier: false\n",
            "only apply to the llm classifier",
        ),
        (
            "  cards:\n    classifier: matcher\n    preset: climatesense-nslp\n",
            "only apply to the llm classifier",
        ),
    ],
)
def test_cards_configuration_is_validated(
    tmp_path: Path, section: str, message: str
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"enrichment:\n{section}output:\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=message):
        load_config(config_path)


def test_cards_preset_can_override_its_model(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "enrichment:\n  cards:\n    classifier: llm\n"
        "    preset: climatesense-nslp\n    model: openai/gpt-4o\n"
        "output:\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    cards = load_config(config_path).enrichment.cards

    assert (cards.preset, cards.model) == ("climatesense-nslp", "openai/gpt-4o")
