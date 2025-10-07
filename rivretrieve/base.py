"""Base class for river data fetchers."""

import abc
from typing import Optional

import pandas as pd


class RiverDataFetcher(abc.ABC):
    """Abstract base class for fetching river gauge data."""

    def __init__(self, site_id: str):
        """Initializes the data fetcher.

        Args:
            site_id: The site-specific identifier for the gauge.
        """
        self.site_id = site_id

    @abc.abstractmethod
    def get_data(
        self,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches the time series data for the given variable and date range.

        Args:
            variable: The variable to fetch (e.g., 'stage' or 'discharge').
            start_date: Optional start date in 'YYYY-MM-DD' format.
            end_date: Optional end date in 'YYYY-MM-DD' format.

        Returns:
            A pandas DataFrame with 'Date' and the variable column ('H' or 'Q').
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available gauge sites.

        Returns:
            A pandas DataFrame containing site information.
        """
        pass

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata for the given site.

        Returns:
            A pandas DataFrame indexed by gauge_id, containing site metadata.
            Returns an empty DataFrame if metadata fetching is not supported for this fetcher.
        """
        # Default implementation returns an empty DataFrame.
        # Subclasses should override this method if metadata is available.
        return pd.DataFrame().set_index('gauge_id')

    @abc.abstractmethod
    def _download_data(self, variable: str, start_date: str, end_date: str) -> any:
        """Downloads the raw data from the source.

        Args:
            variable: The variable to fetch.
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.

        Returns:
            The raw downloaded data in a country-specific format.
        """
        pass

    @abc.abstractmethod
    def _parse_data(self, raw_data: any, variable: str) -> pd.DataFrame:
        """Parses the raw data into a standardized pandas DataFrame.

        Args:
            raw_data: The raw data from _download_data.
            variable: The variable being parsed.

        Returns:
            A pandas DataFrame with 'Date' and the variable column ('H' or 'Q').
        """
        pass
