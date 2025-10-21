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
        """Fetches and parses time series data for a specific gauge and variable.

        This method retrieves the requested data from the provider's API or data source,
        parses it, and returns it in a standardized pandas DataFrame format.

        Args:
            gauge_id: The site-specific identifier for the gauge.
            variable: The variable to fetch. Must be one of the strings listed
                in the fetcher's ``get_available_variables()`` output.
                These are typically defined in ``rivretrieve.constants``.
            start_date: Optional start date for the data retrieval in 'YYYY-MM-DD' format.
                If None, data is fetched from the earliest available date.
            end_date: Optional end date for the data retrieval in 'YYYY-MM-DD' format.
                If None, data is fetched up to the latest available date.

        Returns:
            pd.DataFrame: A pandas DataFrame indexed by datetime objects (``constants.TIME_INDEX``)
            with a single column named after the requested ``variable``. The DataFrame
            will be empty if no data is found for the given parameters.

        Raises:
            ValueError: If the requested ``variable`` is not supported by this fetcher.
            requests.exceptions.RequestException: If a network error occurs during data download.
            Exception: For other unexpected errors during data fetching or parsing.
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available gauge IDs and metadata from a cached file."""
        pass

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata from the data provider.

        .. warning:: This method is not implemented for all fetchers.
                     Check the specific fetcher's documentation.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.

        Raises:
            NotImplementedError: If the method is not implemented for the specific fetcher.
        """
        # Default implementation raises NotImplementedError.
        # Subclasses should override this method if live metadata fetching is available.
        raise NotImplementedError(f"{self.__class__.__name__} does not support fetching live metadata.")

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
