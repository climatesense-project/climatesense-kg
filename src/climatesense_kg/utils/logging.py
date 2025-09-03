"""Logging utilities for the ClimateSense KG Pipeline."""

import logging
import logging.handlers
from pathlib import Path
import sys

from ..config.schemas import LoggingConfig


def setup_logging(config: LoggingConfig | None = None) -> logging.Logger:
    """
    Setup logging configuration for the pipeline.

    Args:
        config: Logging configuration

    Returns:
        logging.Logger: Configured root logger
    """
    if config is None:
        config = LoggingConfig()

    log_level = getattr(logging, config.level.upper(), logging.INFO)

    formatter = logging.Formatter(config.format)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if config.file_path:
        file_path = Path(config.file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        max_bytes = parse_file_size(config.max_file_size)

        file_handler = logging.handlers.RotatingFileHandler(
            filename=file_path,
            maxBytes=max_bytes,
            backupCount=config.backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    package_logger = logging.getLogger("climatesense_kg")
    package_logger.setLevel(log_level)

    return root_logger


def parse_file_size(size_str: str) -> int:
    """
    Parse file size string to bytes.

    Args:
        size_str: Size string like "10MB", "1GB", etc.

    Returns:
        int: Size in bytes
    """
    if not size_str:
        return 10 * 1024 * 1024  # Default 10MB

    size_str = size_str.upper().strip()

    multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}

    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            try:
                number = float(size_str[: -len(suffix)])
                return int(number * multiplier)
            except ValueError:
                break

    try:
        return int(size_str)
    except ValueError:
        return 10 * 1024 * 1024  # Default 10MB


def configure_external_loggers(level: str | int = logging.WARNING) -> None:
    """
    Configure logging level for external libraries to reduce noise.

    Args:
        level: Log level for external libraries
    """
    external_loggers = ["urllib3", "requests", "transformers", "torch", "rdflib"]

    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.WARNING)

    for logger_name in external_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
