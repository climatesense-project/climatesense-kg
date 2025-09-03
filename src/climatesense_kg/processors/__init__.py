"""Data processors for converting raw data to canonical format."""

from .claimreviewdata import ClaimReviewDataProcessor
from .dbkf import DbkfProcessor
from .defacto import DefactoProcessor
from .euroclimatecheck import EuroClimateCheckProcessor

__all__ = [
    "ClaimReviewDataProcessor",
    "DbkfProcessor",
    "DefactoProcessor",
    "EuroClimateCheckProcessor",
]
