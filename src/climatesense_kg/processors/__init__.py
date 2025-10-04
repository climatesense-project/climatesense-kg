"""Data processors for converting raw data to canonical format."""

from .claimreviewdata import ClaimReviewDataProcessor
from .climafacts import ClimafactsProcessor
from .climatefever import ClimateFeverProcessor
from .dbkf import DbkfProcessor
from .defacto import DefactoProcessor
from .desmog import DesmogProcessor
from .euroclimatecheck import EuroClimateCheckProcessor

__all__ = [
    "ClaimReviewDataProcessor",
    "ClimafactsProcessor",
    "ClimateFeverProcessor",
    "DbkfProcessor",
    "DefactoProcessor",
    "DesmogProcessor",
    "EuroClimateCheckProcessor",
]
