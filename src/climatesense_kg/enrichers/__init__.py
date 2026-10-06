"""Enrichment module for enhancing claims with additional semantic information."""

from .base import Enricher
from .cards_enricher import CARDSEnricher
from .cimple_enricher import CimpleModelEnricher
from .dbpedia_spotlight_enricher import DBpediaSpotlightEnricher
from .opentapioca_enricher import OpenTapiocaEnricher
from .refined_enricher import RefinedEnricher
from .sparql_property_enricher import SparqlEntityPropertyEnricher

__all__ = [
    "CARDSEnricher",
    "CimpleModelEnricher",
    "DBpediaSpotlightEnricher",
    "Enricher",
    "OpenTapiocaEnricher",
    "RefinedEnricher",
    "SparqlEntityPropertyEnricher",
]
