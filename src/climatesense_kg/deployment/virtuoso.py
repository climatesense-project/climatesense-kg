"""Virtuoso triplestore deployment utilities."""

from pathlib import Path

import requests

from .base import DeploymentHandler


class VirtuosoDeploymentHandler(DeploymentHandler):
    """Handles RDF data deployment to Virtuoso triplestore."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        graph_template: str,
        isql_service_url: str,
    ):
        """Initialize Virtuoso deployment handler.

        Args:
            host: Virtuoso server host
            port: Virtuoso SQL port
            user: Database username
            password: Database password
            graph_template: Graph name template
            isql_service_url: URL of ISQL HTTP service
        """
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.graph_template = graph_template
        self.isql_service_url = isql_service_url

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
