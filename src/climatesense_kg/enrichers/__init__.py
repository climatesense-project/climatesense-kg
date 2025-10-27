"""Enrichment module for enhancing claims with additional semantic information."""

from .base import Enricher
from .bert_factors_enricher import BertFactorsEnricher
from .dbpedia_enricher import DBpediaEnricher
from .dbpedia_property_enricher import DBpediaPropertyEnricher
from .url_text_enricher import URLTextEnricher

__all__ = [
    "BertFactorsEnricher",
    "DBpediaEnricher",
    "DBpediaPropertyEnricher",
    "Enricher",
    "URLTextEnricher",
]
