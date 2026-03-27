"""Fetcher for Estonian river gauge data from EstModel."""

import logging
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class EstoniaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from EstModel.

    Data source:
        - https://estmodel.envir.ee/
        - https://estmodel.app/

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_DAILY_MAX`` (m³/s)
        - ``constants.DISCHARGE_DAILY_MIN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.STAGE_DAILY_MAX`` (m)
        - ``constants.STAGE_DAILY_MIN`` (m)
        - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)
        - ``constants.WATER_TEMPERATURE_DAILY_MAX`` (°C)
        - ``constants.WATER_TEMPERATURE_DAILY_MIN`` (°C)

    Data description and API:
        - see https://keskkonnaportaal.ee/et/avaandmed/hudroloogilise-seire-andmestik
        - see https://keskkonnaportaal.ee/et/avaandmed/keskkonna-ja-ilma-valdkonna-andmeteenused

    Terms of use:
        - see https://keskkonnaportaal.ee/et/avaandmed/hudroloogilise-seire-andmestik
    """

    BASE_URL = "https://estmodel.envir.ee"
    STATIONS_URL = f"{BASE_URL}/countries/EE/stations"
    GEOJSON_URL = "https://estmodel.app/countries/EE/stations.geojson"
    MEASUREMENTS_URL = BASE_URL + "/stations/{gauge_id}/measurements"
    COUNTRY = "Estonia"
    SOURCE = "EstModel JSON + GeoJSON"
    VAR_MAP = {
        constants.DISCHARGE_DAILY_MEAN: ("Q", "MEAN"),
        constants.DISCHARGE_DAILY_MAX: ("Q", "MAXIMUM"),
        constants.DISCHARGE_DAILY_MIN: ("Q", "MINIMUM"),
        constants.STAGE_DAILY_MEAN: ("H", "MEAN"),
        constants.STAGE_DAILY_MAX: ("H", "MAXIMUM"),
        constants.STAGE_DAILY_MIN: ("H", "MINIMUM"),
        constants.WATER_TEMPERATURE_DAILY_MEAN: ("T", "MEAN"),
        constants.WATER_TEMPERATURE_DAILY_MAX: ("T", "MAXIMUM"),
        constants.WATER_TEMPERATURE_DAILY_MIN: ("T", "MINIMUM"),
    }
    SUPPORTED_VARIABLES = tuple(VAR_MAP.keys())

    @staticmethod
    def _empty_result(variable: str) -> pd.DataFrame:
        """Returns a standardized empty time series result."""
        return pd.DataFrame(columns=[variable], index=pd.DatetimeIndex([], name=constants.TIME_INDEX))

    @staticmethod
    def _empty_metadata_frame() -> pd.DataFrame:
        """Returns a standardized empty metadata result."""
        columns = [
            constants.GAUGE_ID,
            constants.STATION_NAME,
            constants.RIVER,
            constants.LATITUDE,
            constants.LONGITUDE,
            constants.ALTITUDE,
            constants.AREA,
            constants.COUNTRY,
            constants.SOURCE,
        ]
        return pd.DataFrame(columns=columns).set_index(constants.GAUGE_ID)

    @staticmethod
    def _split_station_name(name: Any) -> tuple[str | None, str]:
        """Splits provider names like ``River: Station`` into river and location."""
        if not isinstance(name, str):
            return None, ""
        if ":" not in name:
            stripped_name = name.strip()
            return None, stripped_name
        river, location = name.split(":", 1)
        return river.strip() or None, location.strip()

    @staticmethod
    def _year_windows(start_date: str, end_date: str) -> list[int]:
        """Builds inclusive request years for the EstModel measurements endpoint."""
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        if pd.isna(start) or pd.isna(end) or start > end:
            return []
        return list(range(start.year, end.year + 1))

    @classmethod
    def _parse_station_metadata(cls, payload: Any) -> pd.DataFrame:
        """Parses station metadata from the EstModel JSON catalogue."""
        if not isinstance(payload, list) or not payload:
            return cls._empty_metadata_frame()

        df = pd.json_normalize(payload)
        if df.empty:
            return cls._empty_metadata_frame()

        df = df.rename(columns={"code": constants.GAUGE_ID, "name": constants.STATION_NAME})
        if constants.GAUGE_ID not in df.columns:
            return cls._empty_metadata_frame()

        if "type" in df.columns:
            df = df[df["type"] == "HYDROLOGICAL"].copy()
        if df.empty:
            return cls._empty_metadata_frame()

        if constants.STATION_NAME not in df.columns:
            df[constants.STATION_NAME] = np.nan

        split_names = df[constants.STATION_NAME].apply(cls._split_station_name)
        df[constants.RIVER] = [item[0] for item in split_names]
        df["location"] = [item[1] for item in split_names]

        for column in [
            constants.RIVER,
            constants.LATITUDE,
            constants.LONGITUDE,
            constants.ALTITUDE,
            constants.AREA,
            constants.COUNTRY,
            constants.SOURCE,
        ]:
            if column not in df.columns:
                df[column] = np.nan

        for column in [
            constants.AREA,
            "overlapArea",
            "countryArea",
            "calculationArea",
            "distance",
            constants.ALTITUDE,
        ]:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        df[constants.COUNTRY] = cls.COUNTRY
        df[constants.SOURCE] = cls.SOURCE
        return df

    @staticmethod
    def _parse_geojson_metadata(payload: Any) -> pd.DataFrame:
        """Parses station coordinates from the EstModel GeoJSON layer."""
        if not isinstance(payload, dict):
            return pd.DataFrame(columns=[constants.GAUGE_ID, constants.LATITUDE, constants.LONGITUDE])

        rows = []
        for feature in payload.get("features", []):
            properties = feature.get("properties") or {}
            geometry = feature.get("geometry") or {}
            coordinates = geometry.get("coordinates") or []
            gauge_id = str(properties.get("code") or properties.get("id") or "").strip()
            if not gauge_id:
                continue

            lon = pd.to_numeric(coordinates[0], errors="coerce") if len(coordinates) >= 2 else np.nan
            lat = pd.to_numeric(coordinates[1], errors="coerce") if len(coordinates) >= 2 else np.nan
            rows.append(
                {
                    constants.GAUGE_ID: gauge_id,
                    constants.LATITUDE: lat,
                    constants.LONGITUDE: lon,
                }
            )

        if not rows:
            return pd.DataFrame(columns=[constants.GAUGE_ID, constants.LATITUDE, constants.LONGITUDE])

        return pd.DataFrame(rows).drop_duplicates(subset=[constants.GAUGE_ID]).reset_index(drop=True)

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached metadata (if available)."""
        df = utils.load_cached_metadata_csv("estonia")
        if constants.SOURCE in df.columns:
            df[constants.SOURCE] = EstoniaFetcher.SOURCE
        return df.sort_index()

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and merges hydrological station metadata from EstModel."""
        session = utils.requests_retry_session()

        try:
            stations_response = session.get(self.STATIONS_URL, timeout=30)
            stations_response.raise_for_status()
            metadata_df = self._parse_station_metadata(stations_response.json())
        except (requests.exceptions.RequestException, ValueError) as exc:
            logger.error(f"Failed to fetch Estonian station metadata: {exc}")
            return self._empty_metadata_frame()

        try:
            geojson_response = session.get(self.GEOJSON_URL, timeout=30)
            geojson_response.raise_for_status()
            geojson_df = self._parse_geojson_metadata(geojson_response.json())
        except (requests.exceptions.RequestException, ValueError) as exc:
            logger.warning(f"Failed to fetch Estonian station coordinates; continuing without GeoJSON merge: {exc}")
            geojson_df = pd.DataFrame(columns=[constants.GAUGE_ID, constants.LATITUDE, constants.LONGITUDE])

        if geojson_df.empty:
            metadata_df[constants.LATITUDE] = pd.to_numeric(metadata_df[constants.LATITUDE], errors="coerce")
            metadata_df[constants.LONGITUDE] = pd.to_numeric(metadata_df[constants.LONGITUDE], errors="coerce")
        else:
            metadata_df = metadata_df.drop(columns=[constants.LATITUDE, constants.LONGITUDE], errors="ignore")
            metadata_df = metadata_df.merge(geojson_df, on=constants.GAUGE_ID, how="left")

        metadata_df[constants.ALTITUDE] = pd.to_numeric(metadata_df[constants.ALTITUDE], errors="coerce")
        metadata_df = metadata_df.dropna(subset=[constants.GAUGE_ID])
        metadata_df = metadata_df.drop_duplicates(subset=[constants.GAUGE_ID])
        return metadata_df.set_index(constants.GAUGE_ID).sort_index()

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        """Returns a tuple of supported variables."""
        return EstoniaFetcher.SUPPORTED_VARIABLES

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Downloads raw JSON data for a gauge and variable."""
        if variable not in self.VAR_MAP:
            raise ValueError(f"Unsupported variable: {variable}")

        parameter_code, aggregation_code = self.VAR_MAP[variable]
        request_years = self._year_windows(start_date, end_date)
        if not request_years:
            return []

        session = utils.requests_retry_session()
        url = self.MEASUREMENTS_URL.format(gauge_id=gauge_id)
        records: list[dict[str, Any]] = []

        for year in request_years:
            params = {
                "parameter": parameter_code,
                "type": aggregation_code,
                "start-year": year,
                "end-year": year,
            }
            logger.info(f"Fetching EstModel data for {gauge_id}: {params}")
            response = session.get(url, params=params, timeout=40)
            response.raise_for_status()

            payload = response.json()
            if isinstance(payload, list):
                records.extend(payload)

        return records

    def _parse_data(self, gauge_id: str, raw_data: list[dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses EstModel JSON into a standardized DataFrame."""
        if not isinstance(raw_data, list) or not raw_data:
            return self._empty_result(variable)

        df = pd.DataFrame(raw_data)
        if "startDate" not in df.columns or "value" not in df.columns:
            logger.warning(f"Unexpected EstModel payload for {gauge_id}/{variable}.")
            return self._empty_result(variable)

        df = df.rename(columns={"startDate": constants.TIME_INDEX, "value": variable})
        df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], errors="coerce")
        df[variable] = pd.to_numeric(df[variable], errors="coerce")

        df = (
            df[[constants.TIME_INDEX, variable]]
            .dropna()
            .drop_duplicates(subset=[constants.TIME_INDEX])
            .sort_values(constants.TIME_INDEX)
            .set_index(constants.TIME_INDEX)
        )
        df.index.name = constants.TIME_INDEX
        return df

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

        start_date_dt = pd.Timestamp(start_date)
        end_date_dt = pd.Timestamp(end_date)

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch EstModel data for {gauge_id}/{variable}: {exc}")
            return self._empty_result(variable)
        except ValueError:
            raise
        except Exception as exc:
            logger.error(f"Failed to parse EstModel data for {gauge_id}/{variable}: {exc}")
            return self._empty_result(variable)

        return df.loc[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
