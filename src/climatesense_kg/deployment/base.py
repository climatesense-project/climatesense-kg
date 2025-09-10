"""Base classes for deployment handlers."""

from abc import ABC, abstractmethod
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class DeploymentHandler(ABC):
    """Abstract base class for deployment handlers."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def deploy(self, rdf_file_path: Path, source_name: str) -> bool:
        """Deploy RDF data to the target system.

        Args:
            rdf_file_path: Path to the RDF file to deploy
            source_name: Name of the data source

        Returns:
            True if deployment was successful, False otherwise
        """
        pass
