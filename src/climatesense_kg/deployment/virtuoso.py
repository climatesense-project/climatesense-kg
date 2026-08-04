"""Virtuoso triplestore deployment utilities."""

from pathlib import Path
import re
from urllib.parse import urlsplit

import requests

from .base import DeploymentHandler


class VirtuosoDeploymentHandler(DeploymentHandler):
    """Handles RDF data deployment to Virtuoso triplestore."""

    _INVALID_SQL_INPUT = re.compile(r"[\x00-\x1f\x7f]")

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        graph_template: str,
        isql_service_url: str,
        isql_service_token: str,
    ):
        """Initialize Virtuoso deployment handler.

        Args:
            host: Virtuoso server host
            port: Virtuoso SQL port
            user: Database username
            password: Database password
            graph_template: Graph name template
            isql_service_url: URL of ISQL HTTP service
            isql_service_token: Bearer token for the ISQL HTTP service
        """
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        if not password:
            raise ValueError("VIRTUOSO_PASSWORD is required for Virtuoso deployment")
        self.password = password
        self.graph_template = graph_template
        self.isql_service_url = isql_service_url
        if not isql_service_token:
            raise ValueError("ISQL_SERVICE_TOKEN is required for Virtuoso deployment")
        self.isql_service_token = isql_service_token

    def deploy(self, rdf_file_path: Path, source_name: str) -> bool:
        """Deploy RDF data to Virtuoso.

        Args:
            rdf_file_path: Path to the RDF file to deploy
            source_name: Name of the data source

        Returns:
            True if deployment was successful, False otherwise
        """
        if not rdf_file_path.exists():
            self.logger.error(f"RDF file not found: {rdf_file_path}")
            return False

        graph_uri = self.graph_template.replace("{SOURCE}", source_name)

        self.logger.info(f"Deploying {rdf_file_path} to graph {graph_uri}")

        try:
            if not self._load_rdf_file(rdf_file_path, graph_uri):
                self.logger.error(f"Failed to load RDF file {rdf_file_path}")
                return False

            self.logger.info(
                f"Successfully deployed {rdf_file_path} to Virtuoso graph {graph_uri}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error deploying to Virtuoso: {e}")
            return False

    def _load_rdf_file(self, file_path: Path, graph_uri: str) -> bool:
        """Load RDF file into a specific graph."""
        self._validate_loader_inputs(file_path, graph_uri)
        file_name = self._escape_sql_literal(file_path.name)
        file_directory = self._escape_sql_literal(file_path.parent.as_posix())
        load_list_path = self._escape_sql_literal(file_path.as_posix())
        escaped_graph_uri = self._escape_sql_literal(graph_uri)

        delete_command = (
            "delete from DB.DBA.LOAD_LIST "  # noqa: S608  # nosec B608
            f"where LL_FILE = '{load_list_path}'"
        )
        if not self._execute_sql(delete_command):
            self.logger.error("Failed to clear the file's load_list entry")
            return False

        if not self._execute_sql(
            f"ld_dir('{file_directory}', '{file_name}', '{escaped_graph_uri}')"
        ):
            self.logger.error(f"Failed to execute ld_dir for {file_path}")
            return False

        if not self._execute_sql("rdf_loader_run()", timeout=7200):
            self.logger.error(f"Failed to run rdf_loader_run for {file_path}")
            return False

        if not self._execute_sql("checkpoint"):
            self.logger.error(f"Failed to execute checkpoint after loading {file_path}")
            return False

        return True

    @staticmethod
    def _escape_sql_literal(value: str) -> str:
        """Escape a string for use as a Virtuoso SQL literal."""
        return value.replace("'", "''")

    @classmethod
    def _validate_loader_inputs(cls, file_path: Path, graph_uri: str) -> None:
        """Reject loader values that are invalid paths or absolute graph IRIs."""
        path_text = file_path.as_posix()
        if (
            not file_path.name
            or file_path.name in {".", ".."}
            or cls._INVALID_SQL_INPUT.search(path_text)
        ):
            raise ValueError("RDF loader path contains invalid characters")

        if cls._INVALID_SQL_INPUT.search(graph_uri) or any(
            character.isspace() for character in graph_uri
        ):
            raise ValueError("Virtuoso graph URI contains invalid characters")
        parsed = urlsplit(graph_uri)
        if not parsed.scheme or not (parsed.netloc or parsed.path):
            raise ValueError("Virtuoso graph URI must be an absolute URI")

    def _execute_sql(self, sql_command: str, timeout: int = 300) -> bool:
        """Execute SQL commands via ISQL HTTP service.

        Args:
            sql_command: SQL command to execute
            timeout: Timeout in seconds

        Returns:
            True if execution succeeded
        """
        try:
            response = requests.post(
                f"{self.isql_service_url}/sql",
                json={
                    "query": sql_command,
                },
                headers={
                    "Authorization": f"Bearer {self.isql_service_token}",
                },
                timeout=timeout + 10,
            )

            if response.status_code == 200:
                self.logger.debug("SQL execution successful via HTTP service")
                return True
            else:
                self.logger.error(
                    f"HTTP request failed: {response.status_code} - {response.text}"
                )
                return False

        except requests.Timeout:
            self.logger.error(f"SQL execution timed out after {timeout} seconds")
            return False
        except Exception as e:
            self.logger.error(f"Error executing SQL via HTTP service: {e}")
            return False
