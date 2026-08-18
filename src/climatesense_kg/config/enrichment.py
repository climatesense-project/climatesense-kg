"""Semantic identifiers shared by enrichment configuration and stages."""

from dataclasses import dataclass
from typing import Literal

CimpleModelName = Literal[
    "emotion",
    "sentiment",
    "political_leaning",
    "tropes",
    "persuasion_techniques",
    "conspiracies",
    "climate_related",
]


@dataclass(frozen=True)
class CimpleModelSpec:
    """Stable identity and API route for one CIMPLE model."""

    endpoint: str
    default_version: str = "1"


CIMPLE_MODELS: dict[CimpleModelName, CimpleModelSpec] = {
    "emotion": CimpleModelSpec("emotion"),
    "sentiment": CimpleModelSpec("sentiment"),
    "political_leaning": CimpleModelSpec("political-leaning"),
    "tropes": CimpleModelSpec("tropes"),
    "persuasion_techniques": CimpleModelSpec("persuasion-techniques"),
    "conspiracies": CimpleModelSpec("conspiracy"),
    "climate_related": CimpleModelSpec("climate-related"),
}


def default_cimple_model_versions() -> dict[str, str]:
    """Return an independent mutable mapping for configuration instances."""

    return {name: spec.default_version for name, spec in CIMPLE_MODELS.items()}
