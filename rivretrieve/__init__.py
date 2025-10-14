"""RivRetrieve: A Python package for retrieving global river gauge data."""

from .australia import AustraliaFetcher
from .base import RiverDataFetcher
from .brazil import BrazilFetcher
from .canada import CanadaFetcher
from .chile import ChileFetcher
from .france import FranceFetcher
from .japan import JapanFetcher
from .poland import PolandFetcher
from .slovenia import SloveniaFetcher
from .southafrica import SouthAfricaFetcher
from .uk import UKFetcher
from .uk_nrfa import UKNRFAFetcher
from .usa import USAFetcher

__version__ = "0.1.0"
