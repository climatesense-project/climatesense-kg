"""Configuration management utilities."""

import json
import logging
from pathlib import Path

from dacite import Config, from_dict
import yaml

from .rdf_formats import RDF_FORMAT_EXTENSIONS
from .schemas import PipelineConfig

logger = logging.getLogger(__name__)


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
        dataclass: PipelineConfig = from_dict(
            data_class=PipelineConfig, data=config_data, config=Config(strict=True)
        )
    except Exception as e:
        raise ValueError(f"Failed to parse configuration: {e}") from e

    output_suffix = Path(dataclass.output.output_path).suffix.lower()
    valid_suffixes = RDF_FORMAT_EXTENSIONS[dataclass.output.format]
    if output_suffix not in valid_suffixes:
        expected = ", ".join(sorted(valid_suffixes))
        raise ValueError(
            f"Output format {dataclass.output.format!r} requires a file extension "
            f"of {expected}; got {output_suffix or '<none>'}"
        )

    return dataclass
