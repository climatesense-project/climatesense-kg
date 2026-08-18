"""
ClimateSense Knowledge Graph Pipeline

A modular, source-agnostic pipeline for building the ClimateSense knowledge graphs.
This pipeline ingests data from multiple fact-checking sources, normalizes them to a
canonical model, enriches the data, and outputs standardized RDF snapshots for
deployment to a triplestore.
"""

__version__ = "0.1.0"
__author__ = "ClimateSense"
USER_AGENT = (
    f"ClimateSense-KG/{__version__} "
    "(+https://github.com/climatesense-project/climatesense-kg; "
    "climatesense@lists.eurecom.fr)"
)
