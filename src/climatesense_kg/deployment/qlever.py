"""QLever triplestore deployment utilities."""

from pathlib import Path

import requests

from .base import DeploymentHandler


class QLeverDeploymentHandler(DeploymentHandler):
    """Deploy RDF files to QLever through the Graph Store HTTP Protocol."""

    _CONTENT_TYPES = {
        ".nt": "application/n-triples",
        ".ttl": "text/turtle",
    }

    def __init__(
        self,
        endpoint: str,
        access_token: str,
        graph_template: str,
        timeout: int = 7200,
    ) -> None:
        super().__init__()
        if not access_token:
            raise ValueError("QLEVER_ACCESS_TOKEN is required for QLever deployment")

        self.endpoint = endpoint.rstrip("/")
        self.access_token = access_token
        self.graph_template = graph_template
        self.timeout = timeout

    def deploy(self, rdf_file_path: Path, source_name: str) -> bool:
        """Add an RDF file to the source's named graph in QLever."""
        if not rdf_file_path.exists():
            self.logger.error("RDF file not found: %s", rdf_file_path)
            return False

        content_type = self._CONTENT_TYPES.get(rdf_file_path.suffix.lower())
        if content_type is None:
            self.logger.error(
                "Unsupported RDF format for QLever deployment: %s",
                rdf_file_path.suffix or "<none>",
            )
            return False

        graph_uri = self.graph_template.replace("{SOURCE}", source_name)
        self.logger.info("Deploying %s to graph %s", rdf_file_path, graph_uri)

        try:
            with rdf_file_path.open("rb") as rdf_file:
                response = requests.post(
                    self.endpoint,
                    params={"graph": graph_uri},
                    headers={
                        "Authorization": f"Bearer {self.access_token}",
                        "Content-Type": content_type,
                    },
                    data=rdf_file,
                    timeout=self.timeout,
                )

            if 200 <= response.status_code < 300:
                self.logger.info(
                    "Successfully deployed %s to QLever graph %s",
                    rdf_file_path,
                    graph_uri,
                )
                return True

            response_body = response.text[:1000].replace(self.access_token, "***")
            self.logger.error(
                "QLever upload failed with HTTP %s: %s",
                response.status_code,
                response_body,
            )
            return False
        except requests.Timeout:
            self.logger.error("QLever upload timed out after %s seconds", self.timeout)
            return False
        except requests.RequestException as exc:
            message = str(exc).replace(self.access_token, "***")
            self.logger.error("QLever upload request failed: %s", message)
            return False
        except OSError as exc:
            self.logger.error("Failed to read RDF file %s: %s", rdf_file_path, exc)
            return False
