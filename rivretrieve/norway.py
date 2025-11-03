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

_NUMERIC_METADATA_COLUMNS = [
    constants.LATITUDE,
    constants.LONGITUDE,
    constants.ALTITUDE,
    constants.AREA,
    "drainageBasinAreaNorway",
    "gradient1085",
    "gradientRiver",
    "heightMinimum",
    "heightHypso10",
    "heightHypso20",
    "heightHypso30",
    "heightHypso40",
    "heightHypso50",
    "heightHypso60",
    "heightHypso70",
    "heightHypso80",
    "heightHypso90",
    "heightMaximum",
    "lengthKmBasin",
    "lengthKmRiver",
    "percentAgricul",
    "percentBog",
    "percentEffBog",
    "percentEffLake",
    "percentForest",
    "percentGlacier",
    "percentLake",
    "percentMountain",
    "percentUrban",
    "annualRunoff",
    "specificDischarge",
    "regulationArea",
    "areaReservoirs",
    "volumeReservoirs",
    "regulationPartReservoirs",
    "transferAreaIn",
    "transferAreaOut",
    "reservoirAreaIn",
    "reservoirAreaOut",
    "reservoirVolumeIn",
    "reservoirVolumeOut",
    "remainingArea",
    "numberReservoirs",
    "firstYearRegulation",
    "qNumberOfYears",
    "qStartYear",
    "qEndYear",
    "hm",
    "h5",
    "h10",
    "h20",
    "h50",
    "qm",
    "q5",
    "q10",
    "q20",
    "q50",
    "culHm",
    "culH5",
    "culH10",
    "culH20",
    "culH50",
    "culQm",
    "culQ5",
    "culQ10",
    "culQ20",
    "culQ50",
    "utmEast_Z33",
    "utmNorth_Z33",
    "utmEastGravi",
    "utmZoneGravi",
    "utmNorthGravi",
    "utmEastInlet",
    "utmNorthInlet",
    "utmEastOutlet",
    "utmNorthOutlet",
    "utmZoneInlet",
    "utmZoneOutlet",
]


class NorwayFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the Norwegian Water Resources and Energy Directorate (NVE).

    Data Source: NVE HydAPI (https://hydapi.nve.no/)
    Requires an API key. This can be provided directly to the constructor via the ``api_key``
    argument, or by setting the ``NVE_API_KEY`` environment variable in a ``.env`` file
    located in the ``rivretrieve`` directory. If an ``api_key`` is passed to the
    constructor, it takes precedence over the environment variable.

    Args:
        api_key (Optional[str]): The API key for the NVE HydAPI. If None,
            the fetcher will attempt to load it from the ``NVE_API_KEY``
            environment variable.

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)
        - ``constants.DISCHARGE_HOURLY_MEAN`` (m³/s)
        - ``constants.STAGE_HOURLY_MEAN`` (m)
        - ``constants.WATER_TEMPERATURE_HOURLY_MEAN`` (°C)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_INSTANT`` (m)
        - ``constants.WATER_TEMPERATURE_INSTANT`` (°C)

    Terms of use:
        - For API, see https://hydapi.nve.no/UserDocumentation/#termsofuse
    """

    BASE_URL = "https://hydapi.nve.no/api/v1/"
    PARAMETERS = {
        constants.STAGE_DAILY_MEAN: {"id": 1000, "resolution": 1440},
        constants.DISCHARGE_DAILY_MEAN: {"id": 1001, "resolution": 1440},
        constants.WATER_TEMPERATURE_DAILY_MEAN: {"id": 1003, "resolution": 1440},
        constants.STAGE_HOURLY_MEAN: {"id": 1000, "resolution": 60},
        constants.DISCHARGE_HOURLY_MEAN: {"id": 1001, "resolution": 60},
        constants.WATER_TEMPERATURE_HOURLY_MEAN: {"id": 1003, "resolution": 60},
        constants.STAGE_INSTANT: {"id": 1000, "resolution": 0},
        constants.DISCHARGE_INSTANT: {"id": 1001, "resolution": 0},
        constants.WATER_TEMPERATURE_INSTANT: {"id": 1003, "resolution": 0},

    }

    def __init__(self, api_key: Optional[str] = None):
        super().__init__()
        self.api_key = api_key or API_KEY
        if not self.api_key:
            logger.error(
                "NVE API Key not provided. Please set NVE_API_KEY in your .env file or pass it to the constructor."
            )
        self.headers = {"Accept": "application/json", "X-API-Key": self.api_key}

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
        if not self.api_key:
            logger.error("NVE API Key not available.")
            return pd.DataFrame()

        url = f"{self.BASE_URL}Stations?Active={active_flag}"
        s = utils.requests_retry_session()
        try:
            response = s.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()["data"]
            return pd.DataFrame(data)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching station metadata (Active={active_flag}): {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error processing station metadata (Active={active_flag}): {e}")
            return pd.DataFrame()

    def _parse_series_list(self, series_list: Optional[List[Dict[str, Any]]]) -> Dict[str, bool]:
        """Parses the seriesList to determine available variables and resolutions."""
        available = {var: False for var in self.PARAMETERS}
        if not series_list or not isinstance(series_list, list):
            return available

        for series in series_list:
            if not isinstance(series, dict):
                continue
            param_id = series.get("parameter")
            resolutions = series.get("resolutionList", [])
            if not isinstance(resolutions, list):
                continue

            for res_info in resolutions:
                if not isinstance(res_info, dict):
                    continue
                res_time = res_info.get("resTime")

                for var_name, var_params in self.PARAMETERS.items():
                    if var_params["id"] == param_id and var_params["resolution"] == res_time:
                        available[var_name] = True
        return available

    def get_metadata(self) -> pd.DataFrame:
        """Download and process metadata for all Norwegian hydrometric stations."""
        if not self.api_key:
            logger.error("NVE API Key not set.")
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

        # Combine active and inactive stations
        df_active = self._get_station_metadata(1)
        df_inactive = self._get_station_metadata(0)

        metadata_dfs = []
        if not df_active.empty:
            metadata_dfs.append(df_active)
        if not df_inactive.empty:
            metadata_dfs.append(df_inactive)

        if not metadata_dfs:
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

        metadata = pd.concat(metadata_dfs, ignore_index=True)
        if metadata.empty:
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

        metadata = metadata.drop_duplicates(subset=["stationId"])

        rename_map = {
            "stationId": constants.GAUGE_ID,
            "stationName": constants.STATION_NAME,
            "latitude": constants.LATITUDE,
            "longitude": constants.LONGITUDE,
            "masl": constants.ALTITUDE,
            "drainageBasinArea": constants.AREA,
            "riverName": constants.RIVER,
        }
        metadata = metadata.rename(columns=rename_map)

        # NVE stationId is a string, e.g., "12.210.0"
        metadata[constants.GAUGE_ID] = metadata[constants.GAUGE_ID].astype(str)
        metadata = metadata.set_index(constants.GAUGE_ID)

        for col in _NUMERIC_METADATA_COLUMNS:
            if col in metadata.columns:
                metadata[col] = pd.to_numeric(metadata[col], errors="coerce")

        # Parse seriesList to add variable availability columns
        availability_cols = list(self.PARAMETERS.keys())
        for col in availability_cols:
            metadata[col] = False

        if "seriesList" in metadata.columns:
            for index, row in metadata.iterrows():
                available_vars = self._parse_series_list(row["seriesList"])
                for var, is_available in available_vars.items():
                    metadata.loc[index, var] = is_available

        return metadata

    def _get_api_params(self, variable: str) -> Dict[str, int]:
        if variable in self.PARAMETERS:
            return self.PARAMETERS[variable]
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Downloads raw data from the NVE API."""
        if not self.api_key:
            logger.error("NVE API Key not available.")
            return []

        api_params = self._get_api_params(variable)

        params = {
            "StationId": gauge_id,
            "Parameter": api_params["id"],
            "ResolutionTime": api_params["resolution"],
            "ReferenceTime": f"{start_date}/{end_date}",
        }
        s = utils.requests_retry_session()
        try:
            response = s.get(f"{self.BASE_URL}Observations", headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json().get("data", [])
            obs_list = []
            for item in data:
                meta = {k: item[k] for k in ["stationId", "parameter"] if k in item}
                for obs in item.get("observations", []):
                    obs_list.append({**meta, **obs})
            return obs_list
        except requests.exceptions.RequestException as e:
            logger.error(f"NVE API request failed for station {gauge_id}, parameter {api_params['id']}: {e}")
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
            df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], utc=True)
            # Only convert to date if it's a daily variable
            if self._get_api_params(variable)["resolution"] == 1440:
                df[constants.TIME_INDEX] = df[constants.TIME_INDEX].dt.date
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
        if not self.api_key:
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
