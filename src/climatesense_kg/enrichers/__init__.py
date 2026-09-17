"""Enrichment module for enhancing claims with additional semantic information."""

from .base import Enricher
from .cimple_enricher import CimpleModelEnricher
from .dbpedia_property_enricher import DBpediaPropertyEnricher
from .dbpedia_spotlight_enricher import DBpediaSpotlightEnricher
from .opentapioca_enricher import OpenTapiocaEnricher
from .refined_enricher import RefinedEnricher

__all__ = [
    "CimpleModelEnricher",
    "DBpediaPropertyEnricher",
    "DBpediaSpotlightEnricher",
    "Enricher",
    "OpenTapiocaEnricher",
    "RefinedEnricher",
]
