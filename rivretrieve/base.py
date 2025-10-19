"""Base class for river data fetchers."""

import abc
from typing import Optional

import pandas as pd


class RiverDataFetcher(abc.ABC):
    """Abstract base class for fetching river gauge data."""

    def __init__(self):
        """Initializes the data fetcher."""
        pass

    @abc.abstractmethod
    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches the time series data for the given variable and date range.

        Args:
            gauge_id: The site-specific identifier for the gauge.
            variable: The variable to fetch, should be one of the values from constants.py
                (e.g., constants.DISCHARGE, constants.STAGE).
            start_date: Optional start date in 'YYYY-MM-DD' format.
            end_date: Optional end date in 'YYYY-MM-DD' format.

        Returns:
            A pandas DataFrame indexed by time (constants.TIME_INDEX) with a column
            for the requested variable (e.g., constants.DISCHARGE).
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available gauge IDs and metadata from a cached file."""
        pass

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata for the given site.

        Returns:
            A pandas DataFrame indexed by gauge_id, containing site metadata.
            Returns an empty DataFrame if metadata fetching is not supported for this fetcher.
        """
        # Default implementation returns an empty DataFrame.
        # Subclasses should override this method if metadata is available.
        return pd.DataFrame().set_index("gauge_id")

    @staticmethod
    @abc.abstractmethod
    def get_available_variables() -> tuple[str, ...]:
        """Returns a tuple of supported variables."""
        pass

    @abc.abstractmethod
    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> any:
        """Downloads the raw data from the source.

        Args:
            gauge_id: The site-specific identifier for the gauge.
            variable: The variable to fetch.
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.

        Returns:
            The raw downloaded data in a country-specific format.
        """
        pass

    @abc.abstractmethod
    def _parse_data(self, gauge_id: str, raw_data: any, variable: str) -> pd.DataFrame:
        """Parses the raw data into a standardized pandas DataFrame.

        Args:
            gauge_id: The site-specific identifier for the gauge.
            raw_data: The raw data from _download_data.
            variable: The variable being parsed.

        Returns:
            A pandas DataFrame with 'Date' and the variable column ('H' or 'Q').
        """
        pass
