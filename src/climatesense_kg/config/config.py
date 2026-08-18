"""Configuration management utilities."""

import json
import logging
from pathlib import Path
from typing import Any

from dacite import Config, from_dict
import yaml

from ..provider_registry import PROVIDER_REGISTRATIONS
from .schemas import PipelineConfig

logger = logging.getLogger(__name__)


def _build_provider_configs(config_data: dict[str, Any]) -> None:
    """Resolve provider discriminators before constructing the full config."""

    sources = config_data.get("data_sources")
    if not isinstance(sources, list):
        return
    for source in sources:
        if not isinstance(source, dict):
            continue
        provider = source.get("provider")
        if not isinstance(provider, dict):
            continue
        provider_type = provider.get("provider_type")
        registration = PROVIDER_REGISTRATIONS.get(provider_type)
        if registration is None:
            raise ValueError(f"Unknown provider_type: {provider_type!r}")
        source["provider"] = from_dict(
            data_class=registration.config_type,
            data=provider,
            config=Config(strict=True),
        )


def load_config(config_path: str | Path) -> PipelineConfig:
    """Load configuration from a file."""
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, encoding="utf-8") as f:
        if config_path.suffix.lower() in [".yaml", ".yml"]:
            config_data = yaml.safe_load(f)
        elif config_path.suffix.lower() == ".json":
            config_data = json.load(f)
        else:
            raise ValueError(f"Unsupported configuration format: {config_path.suffix}")

    if not isinstance(config_data, dict):
        raise ValueError("Configuration root must be a mapping")

    try:
        _build_provider_configs(config_data)
        dataclass: PipelineConfig = from_dict(
            data_class=PipelineConfig, data=config_data, config=Config(strict=True)
        )
    except Exception as e:
        raise ValueError(f"Failed to parse configuration: {e}") from e

    if not str(Path(dataclass.output.output_path)).lower().endswith(".nt.gz"):
        raise ValueError("Pipeline RDF output_path must use the .nt.gz extension")

    return dataclass
