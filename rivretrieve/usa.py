"""Fetcher for USA river gauge data from USGS NWIS."""

import logging
from typing import Optional

import pandas as pd
from dataretrieval import nwis

from . import base, constants, utils

logger = logging.getLogger(__name__)


class USAFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the US Geological Survey (USGS) National Water Information System (NWIS).

    Data Source: USGS NWIS (https://waterservices.usgs.gov/)
    This fetcher uses the ``dataretrieval`` package.

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.STAGE_DAILY_MAX`` (m)
        - ``constants.STAGE_DAILY_MIN`` (m)
        - ``constants.STAGE_INSTANT`` (m)
    """

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available USA gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("usa")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.DISCHARGE_DAILY_MEAN,
            constants.DISCHARGE_INSTANT,
            constants.STAGE_DAILY_MEAN,
            constants.STAGE_DAILY_MAX,
            constants.STAGE_DAILY_MIN,
            constants.STAGE_INSTANT,
        )

    def _get_param_code(self, variable: str) -> str:
        if constants.STAGE in variable:
            return "00065"
        elif constants.DISCHARGE in variable:
            return "00060"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _get_column_name(self, variable: str) -> str:
        param_code = self._get_param_code(variable)
        if variable == constants.STAGE_DAILY_MAX:
            return f"{param_code}_Maximum"
        elif variable == constants.STAGE_DAILY_MIN:
            return f"{param_code}_Minimum"
        elif variable == constants.STAGE_DAILY_MEAN:
            return f"{param_code}_Mean"
        elif variable == constants.DISCHARGE_DAILY_MEAN:
            return f"{param_code}_Mean"
        elif variable == constants.DISCHARGE_INSTANT:
            return param_code
        elif variable == constants.STAGE_INSTANT:
            return param_code

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Downloads data using the dataretrieval package."""
        param_code = self._get_param_code(variable)
        try:
            if constants.DAILY in variable:
                df, meta = nwis.get_dv(
                    sites=gauge_id,
                    startDT=start_date,
                    endDT=end_date,
                    parameterCd=[param_code],
                )
            elif constants.INSTANTANEOUS in variable:
                df, meta = nwis.get_iv(
                    sites=gauge_id,
                    startDT=start_date,
                    endDT=end_date,
                    parameterCd=[param_code],
                )
            return df
        except Exception as e:
            logger.error(f"Error fetching NWIS data for site {gauge_id}, param {param_code}: {e}")
            return pd.DataFrame()

    def _parse_data(self, gauge_id: str, raw_data: pd.DataFrame, variable: str) -> pd.DataFrame:
        """Parses the DataFrame from dataretrieval."""

        if raw_data.empty:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        param_code = self._get_param_code(variable)
        value_col = self._get_column_name(variable)
        if value_col not in raw_data.columns:
            logger.warning(
                f"Could not find value column {value_col} for param {param_code} in data for site {gauge_id}"
            )
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = raw_data[[value_col]].copy()
        df.index.name = constants.TIME_INDEX
        df = df.reset_index()
        df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX].dt.date)

        # Unit conversion
        if variable.startswith(constants.STAGE):  # Feet to meters
            mult = 0.3048
        elif variable.startswith(constants.DISCHARGE):  # cfs to m3/s
            mult = 0.0283168466
        df[variable] = pd.to_numeric(df[value_col], errors="coerce") * mult

        return df[[constants.TIME_INDEX, variable]].dropna().set_index(constants.TIME_INDEX)

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
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
