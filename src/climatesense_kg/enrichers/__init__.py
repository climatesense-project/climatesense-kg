"""Enrichment module for enhancing claims with additional semantic information."""

from .base import Enricher
from .bert_factors_enricher import BertFactorsEnricher
from .composite_enricher import CompositeEnricher
from .dbpedia_enricher import DBpediaEnricher

__all__ = [
    "BertFactorsEnricher",
    "CompositeEnricher",
    "DBpediaEnricher",
    "Enricher",
]
