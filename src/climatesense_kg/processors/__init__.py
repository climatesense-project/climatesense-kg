"""Data processors for converting raw data to canonical format."""

from .claimreviewdata import ClaimReviewDataProcessor
from .dbkf import DbkfProcessor
from .defacto import DefactoProcessor
from .desmog import DesmogProcessor
from .euroclimatecheck import EuroClimateCheckProcessor

__all__ = [
    "ClaimReviewDataProcessor",
    "DbkfProcessor",
    "DefactoProcessor",
    "DesmogProcessor",
    "EuroClimateCheckProcessor",
]
