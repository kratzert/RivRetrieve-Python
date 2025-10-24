"""Fetcher for Norwegian river gauge data from NVE."""

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from . import base, constants, utils

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join((os.path.dirname(__file__)), ".env"))

# Get credentials from environment variables
API_KEY = os.environ.get("NVE_API_KEY")


class NorwayFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the Norwegian Water Resources and Energy Directorate (NVE).

    Data Source: NVE HydAPI (https://hydapi.nve.no/)
    Requires an API key, which can be set in a ``.env`` file
    in the ``rivretrieve`` directory. Key in ``.env``: ``NVE_API_KEY``

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)
    """

    BASE_URL = "https://hydapi.nve.no/api/v1/"
    HEADERS = {"Accept": "application/json", "X-API-Key": API_KEY}
    PARAMETERS = {
        constants.STAGE_DAILY_MEAN: 1000,
        constants.DISCHARGE_DAILY_MEAN: 1001,
        constants.WATER_TEMPERATURE_DAILY_MEAN: 1003,
    }
    TIME_RESOLUTION_MINUTES = 1440  # Daily

    def __init__(self):
        super().__init__()
        if not API_KEY:
            logger.error("NVE API Key not provided. Please set NVE_API_KEY in your .env file.")

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available Norwegian gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("norway")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(NorwayFetcher.PARAMETERS.keys())

    def _get_station_metadata(self, active_flag: int) -> pd.DataFrame:
        """Retrieve metadata for active/inactive hydrometric stations."""
        if not API_KEY:
            logger.error("NVE API Key not available.")
            return pd.DataFrame()

        url = f"{self.BASE_URL}Stations?Active={active_flag}"
        s = utils.requests_retry_session()
        try:
            response = s.get(url, headers=self.HEADERS)
            response.raise_for_status()
            data = response.json()["data"]
            return pd.DataFrame(data)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching station metadata (Active={active_flag}): {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error processing station metadata (Active={active_flag}): {e}")
            return pd.DataFrame()

    def get_metadata(self) -> pd.DataFrame:
        """Download and process metadata for all Norwegian hydrometric stations."""
        if not API_KEY:
            logger.error("NVE API Key not set.")
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

        # Combine active and inactive stations
        metadata = pd.concat(
            [self._get_station_metadata(0), self._get_station_metadata(1)],
            ignore_index=True,
        )
        if metadata.empty:
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

        metadata = metadata.drop_duplicates(subset=["stationId"])

        rename_map = {
            "stationId": constants.GAUGE_ID,
            "stationName": constants.STATION_NAME,
            "latitude": constants.LATITUDE,
            "longitude": constants.LONGITUDE,
            "masl": constants.ALTITUDE,
            "catchmentArea": constants.AREA,
            "riverName": constants.RIVER,
        }
        metadata = metadata.rename(columns=rename_map)

        # NVE stationId is a string, e.g., "12.210.0"
        metadata[constants.GAUGE_ID] = metadata[constants.GAUGE_ID].astype(str)
        metadata = metadata.set_index(constants.GAUGE_ID)

        return metadata

    def _get_parameter_id(self, variable: str) -> int:
        if variable in self.PARAMETERS:
            return self.PARAMETERS[variable]
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Downloads raw data from the NVE API."""
        if not API_KEY:
            logger.error("NVE API Key not available.")
            return []

        parameter_id = self._get_parameter_id(variable)

        params = {
            "StationId": gauge_id,
            "Parameter": parameter_id,
            "ResolutionTime": self.TIME_RESOLUTION_MINUTES,
            "ReferenceTime": f"{start_date}/{end_date}",
        }
        s = utils.requests_retry_session()
        try:
            response = s.get(f"{self.BASE_URL}Observations", headers=self.HEADERS, params=params)
            response.raise_for_status()
            data = response.json().get("data", [])
            obs_list = []
            for item in data:
                meta = {k: item[k] for k in ["stationId", "parameter"] if k in item}
                for obs in item.get("observations", []):
                    obs_list.append({**meta, **obs})
            return obs_list
        except requests.exceptions.RequestException as e:
            logger.error(f"NVE API request failed for station {gauge_id}, parameter {parameter_id}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error processing NVE API response for station {gauge_id}: {e}")
            return []

    def _parse_data(self, gauge_id: str, raw_data: List[Dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses the raw JSON data."""
        if not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            df = pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            df = df.rename(columns={"time": constants.TIME_INDEX, "value": variable})
            df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], utc=True).dt.date
            df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX])
            df[variable] = pd.to_numeric(df[variable], errors="coerce")

            # Unit conversion: NVE API seems to provide data in standard units (m3/s, m, degC)

            return (
                df[[constants.TIME_INDEX, variable]]
                .dropna()
                .sort_values(by=constants.TIME_INDEX)
                .set_index(constants.TIME_INDEX)
            )
        except Exception as e:
            logger.error(f"Error parsing JSON data for site {gauge_id}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable.

        Args:
            gauge_id: The site-specific identifier for the gauge.
            variable: The variable to fetch. Must be one of the strings listed
                in the fetcher's ``get_available_variables()`` output.
            start_date: Optional start date for the data retrieval in 'YYYY-MM-DD' format.
            end_date: Optional end date for the data retrieval in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: A pandas DataFrame indexed by datetime objects (``constants.TIME_INDEX``)
            with a single column named after the requested ``variable``.
        """
        if not API_KEY:
            logger.error("NVE API Key not set.")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

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
