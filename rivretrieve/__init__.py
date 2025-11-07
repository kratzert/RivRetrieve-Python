"""RivRetrieve: A Python package for retrieving global river gauge data."""

from .australia import AustraliaFetcher
from .base import RiverDataFetcher
from .brazil import BrazilFetcher
from .canada import CanadaFetcher
from .chile import ChileFetcher
from .czech import CzechFetcher
from .france import FranceFetcher
from .germany_berlin import GermanyBerlinFetcher
from .japan import JapanFetcher
from .norway import NorwayFetcher
from .poland import PolandFetcher
from .portugal import PortugalFetcher
from .slovenia import SloveniaFetcher
from .southafrica import SouthAfricaFetcher
from .spain import SpainFetcher
from .uk_ea import UKEAFetcher
from .uk_nrfa import UKNRFAFetcher
from .usa import USAFetcher

__version__ = "0.1.0"
