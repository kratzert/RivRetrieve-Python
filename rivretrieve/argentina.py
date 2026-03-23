"""Fetcher for Argentina river data from INA Alerta."""

import io
import logging
from datetime import timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class ArgentinaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from INA Alerta.

    Data source:
        - https://alerta.ina.gob.ar/pub/gui

    Supported variables:
        - constants.DISCHARGE_DAILY_MEAN (m³/s)

    Data description and API:
        - API explorer: https://alerta.ina.gob.ar/pub/gui
        - observed series index endpoint: https://alerta.ina.gob.ar/a5/obs/puntual/series
        - observed data endpoint: https://alerta.ina.gob.ar/a5/getObservaciones

    Terms of use:
        - see https://www.ina.gob.ar/alerta/index.php
    """

    BASE_URL = "https://alerta.ina.gob.ar/a5"

    VARIABLE_CONFIG = {
        constants.DISCHARGE_DAILY_MEAN: {
            "var_id": 40,
            "general_category": "Hydrology",
        }
    }

    @staticmethod
    def _empty_result(variable: str) -> pd.DataFrame:
        """Returns an empty standardized RivRetrieve time series frame."""
        return pd.DataFrame(columns=[constants.TIME_INDEX, variable]).set_index(constants.TIME_INDEX)

    @staticmethod
    def _empty_metadata() -> pd.DataFrame:
        """Returns an empty metadata frame indexed by gauge ID."""
        return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

    @staticmethod
    def _safe_numeric(value: Any) -> float:
        value = pd.to_numeric(value, errors="coerce")
        return np.nan if pd.isna(value) else float(value)

    @classmethod
    def _standardize_metadata_frame(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return cls._empty_metadata()

        rename_map = {
            "sitecode": constants.GAUGE_ID,
            "nombre": constants.STATION_NAME,
            "rio": constants.RIVER,
            "lat": constants.LATITUDE,
            "lon": constants.LONGITUDE,
            "pais": constants.COUNTRY,
        }
        df = df.rename(columns=rename_map).copy()

        required_columns = [
            constants.GAUGE_ID,
            constants.STATION_NAME,
            constants.RIVER,
            constants.LATITUDE,
            constants.LONGITUDE,
            constants.COUNTRY,
            constants.SOURCE,
            constants.ALTITUDE,
            constants.AREA,
        ]
        for column in required_columns:
            if column not in df.columns:
                df[column] = np.nan

        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str)
        df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
        df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")
        df[constants.ALTITUDE] = pd.to_numeric(df[constants.ALTITUDE], errors="coerce")
        df[constants.AREA] = pd.to_numeric(df[constants.AREA], errors="coerce")
        df[constants.COUNTRY] = "Argentina"
        df[constants.SOURCE] = "INA Alerta"

        return df.set_index(constants.GAUGE_ID, drop=True)

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Argentina gauge metadata."""
        df = utils.load_cached_metadata_csv("argentina").reset_index()
        return ArgentinaFetcher._standardize_metadata_frame(df)

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(ArgentinaFetcher.VARIABLE_CONFIG.keys())

    def _build_url(self, path: str) -> str:
        return f"{self.BASE_URL}/{path.lstrip('/')}"

    def _get_json(self, path: str, params: dict[str, Any]) -> Any:
        session = utils.requests_retry_session()
        response = session.get(self._build_url(path), params=params, timeout=25)
        response.raise_for_status()
        return response.json()

    def _fetch_series_index(self, variable: str) -> pd.DataFrame:
        """Fetches the provider's series index for the requested variable."""
        config = self.VARIABLE_CONFIG[variable]
        payload = self._get_json(
            "obs/puntual/series",
            params={
                "format": "geojson",
                "var_id": config["var_id"],
                "GeneralCategory": config["general_category"],
                "data_availability": "h",
            },
        )

        features = payload.get("features", [])
        rows = []
        for feature in features:
            properties = feature.get("properties", {}) or {}
            geometry = feature.get("geometry", {}) or {}
            coordinates = geometry.get("coordinates", [np.nan, np.nan]) or [np.nan, np.nan]

            rows.append(
                {
                    constants.GAUGE_ID: str(properties.get("estacion_id")),
                    constants.STATION_NAME: properties.get("nombre"),
                    constants.RIVER: properties.get("rio"),
                    constants.LONGITUDE: self._safe_numeric(coordinates[0] if len(coordinates) > 0 else np.nan),
                    constants.LATITUDE: self._safe_numeric(coordinates[1] if len(coordinates) > 1 else np.nan),
                    "series_id": properties.get("id", properties.get("series_id")),
                    "proc_id": properties.get("proc_id"),
                    "var_id": properties.get("var_id", config["var_id"]),
                    "unit": properties.get("unidad"),
                }
            )

        return pd.DataFrame(
            rows,
            columns=[
                constants.GAUGE_ID,
                constants.STATION_NAME,
                constants.RIVER,
                constants.LONGITUDE,
                constants.LATITUDE,
                "series_id",
                "proc_id",
                "var_id",
                "unit",
            ],
        )

    def _fetch_station_details(self, gauge_id: str) -> dict[str, float]:
        """Fetches station-level metadata details for one Argentina gauge."""
        try:
            payload = self._get_json(
                f"obs/puntual/estaciones/{gauge_id}",
                params={"format": "json", "get_drainage_basin": "true"},
            )
        except requests.exceptions.RequestException as exc:
            logger.warning(f"Could not fetch metadata details for station {gauge_id}: {exc}")
            return {constants.ALTITUDE: np.nan, constants.AREA: np.nan}

        altitude = self._safe_numeric(payload.get("altitud"))

        area = np.nan
        drainage_basin = payload.get("drainage_basin") or {}
        basin_properties = drainage_basin.get("properties") or {}
        raw_area = self._safe_numeric(basin_properties.get("area"))
        if not np.isnan(raw_area):
            area = raw_area / 1e6

        return {constants.ALTITUDE: altitude, constants.AREA: area}

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata for Argentina stations from INA Alerta.

        Combines the observed-series index with station detail payloads and
        returns a DataFrame indexed by ``constants.GAUGE_ID``.
        """
        try:
            series_index = self._fetch_series_index(constants.DISCHARGE_DAILY_MEAN)
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Argentina metadata: {exc}")
            return self._empty_metadata()

        if series_index.empty:
            return self._empty_metadata()

        metadata = series_index.drop_duplicates(subset=[constants.GAUGE_ID]).copy()
        metadata[constants.COUNTRY] = "Argentina"
        metadata[constants.SOURCE] = "INA Alerta"

        details = [self._fetch_station_details(gauge_id) for gauge_id in metadata[constants.GAUGE_ID]]
        metadata[constants.ALTITUDE] = [item[constants.ALTITUDE] for item in details]
        metadata[constants.AREA] = [item[constants.AREA] for item in details]

        return self._standardize_metadata_frame(metadata.reset_index(drop=True))

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> str:
        """Downloads raw csvless observations from INA Alerta."""
        if variable not in self.VARIABLE_CONFIG:
            raise ValueError(f"Unsupported variable: {variable}")

        series_index = self._fetch_series_index(variable)
        if series_index.empty:
            logger.warning(f"No Argentina series found for variable {variable}")
            return ""

        station_rows = series_index[series_index[constants.GAUGE_ID] == str(gauge_id)]
        if station_rows.empty:
            logger.warning(f"No Argentina series found for station {gauge_id} and variable {variable}")
            return ""

        series_id = station_rows.iloc[0]["series_id"]
        if pd.isna(series_id):
            logger.warning(f"Missing series_id for station {gauge_id} and variable {variable}")
            return ""

        request_end_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")

        session = utils.requests_retry_session()
        response = session.get(
            self._build_url("getObservaciones"),
            params={
                "tipo": "puntual",
                "series_id": int(series_id),
                "timestart": start_date,
                "timeend": request_end_date,
                "format": "csvless",
                "no_id": "true",
            },
            timeout=25,
        )
        response.raise_for_status()
        return response.text

    def _parse_data(self, gauge_id: str, raw_data: str, variable: str) -> pd.DataFrame:
        """Parses INA csvless payloads into a standardized DataFrame."""
        if not raw_data or not raw_data.strip() or raw_data.strip() == "null":
            return self._empty_result(variable)

        try:
            df = pd.read_csv(io.StringIO(raw_data), header=None)
        except Exception as exc:
            logger.error(f"Failed to parse Argentina csvless payload for station {gauge_id}: {exc}")
            return self._empty_result(variable)

        if df.shape[1] < 2:
            return self._empty_result(variable)

        timestamps = pd.to_datetime(df.iloc[:, 0], errors="coerce", utc=True)
        timestamps = timestamps.dt.tz_localize(None)
        if variable == constants.DISCHARGE_DAILY_MEAN:
            timestamps = timestamps.dt.normalize()

        parsed = pd.DataFrame(
            {
                constants.TIME_INDEX: timestamps,
                variable: pd.to_numeric(df.iloc[:, -1], errors="coerce"),
            }
        )

        return (
            parsed.dropna()
            .drop_duplicates(subset=[constants.TIME_INDEX])
            .sort_values(constants.TIME_INDEX)
            .set_index(constants.TIME_INDEX)
        )

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable.

        This method retrieves the requested data from the provider's API,
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
            if df.empty:
                return self._empty_result(variable)
            start_day = pd.to_datetime(start_date).date()
            end_day = pd.to_datetime(end_date).date()
            index_days = pd.Index(df.index.date)
            return df[(index_days >= start_day) & (index_days <= end_day)]
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to download Argentina data for {gauge_id}/{variable}: {exc}")
            return self._empty_result(variable)
        except Exception as exc:
            logger.error(f"Failed to parse Argentina data for {gauge_id}/{variable}: {exc}")
            return self._empty_result(variable)
