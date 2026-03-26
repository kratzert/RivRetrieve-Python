"""Fetcher for Swedish hydrological data from SMHI HydroObs."""

import logging
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class SwedenFetcher(base.RiverDataFetcher):
    """Fetches Swedish hydrological time series and station metadata from SMHI HydroObs.

    Data source:
        https://opendata.smhi.se/hydroobs/

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)
        - ``constants.DISCHARGE_MONTHLY_MEAN`` (m³/s)

    Data description and API:
        - see https://opendata.smhi.se/hydroobs/resources/parameter

    Terms of use:
        - see https://www.smhi.se/data/om-smhis-data/smhis-datapolicy

    Notes:
        - SMHI documents ``Vattenstånd`` and ``Vattendragstemperatur`` as daily measurements.
          RivRetrieve normalizes these Sweden series to the daily mean constants for a consistent
          daily-resolution API.
    """

    BASE_URL = "https://opendata-download-hydroobs.smhi.se/api/version/1.0"
    COUNTRY = "Sweden"
    SOURCE = "SMHI HydroObs"
    TIMESTAMP_OFFSET = pd.Timedelta(hours=2)
    VARIABLE_CONFIG = {
        constants.DISCHARGE_DAILY_MEAN: {"parameter_id": 1, "scale": 1.0},
        constants.DISCHARGE_INSTANT: {"parameter_id": 2, "scale": 1.0},
        constants.STAGE_DAILY_MEAN: {"parameter_id": 3, "scale": 0.01},
        constants.WATER_TEMPERATURE_DAILY_MEAN: {"parameter_id": 4, "scale": 1.0},
        constants.DISCHARGE_MONTHLY_MEAN: {"parameter_id": 10, "scale": 1.0},
    }

    @staticmethod
    def _empty_result(variable: str) -> pd.DataFrame:
        """Returns a standardized empty time series result."""
        return pd.DataFrame(columns=[variable], index=pd.DatetimeIndex([], name=constants.TIME_INDEX))

    @staticmethod
    def _empty_metadata_frame() -> pd.DataFrame:
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
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Swedish gauge metadata."""
        return utils.load_cached_metadata_csv("sweden")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        """Returns a tuple of supported variables."""
        return tuple(SwedenFetcher.VARIABLE_CONFIG.keys())

    @classmethod
    def _format_query_datetime(cls, date_str: str, end_of_day: bool = False) -> str:
        suffix = "23:59Z" if end_of_day else "00:00Z"
        return f"{date_str}T{suffix}"

    @classmethod
    def _to_local_timestamp(cls, value: Any) -> pd.Timestamp:
        timestamp = pd.to_datetime(value, unit="ms", utc=True, errors="coerce")
        if pd.isna(timestamp):
            return pd.NaT
        # SMHI labels archive timestamps using Swedish summer time (UTC+2) throughout the year.
        return (timestamp + cls.TIMESTAMP_OFFSET).tz_localize(None)

    @classmethod
    def _to_local_series(cls, values: pd.Series) -> pd.Series:
        timestamps = pd.to_datetime(values, unit="ms", utc=True, errors="coerce")
        return (timestamps + cls.TIMESTAMP_OFFSET).dt.tz_localize(None)

    @staticmethod
    def _normalize_area(value: Any) -> float:
        area = pd.to_numeric(value, errors="coerce")
        if pd.notna(area) and area < 0:
            return np.nan
        return area

    @classmethod
    def _build_parameter_url(cls, parameter_id: int) -> str:
        return f"{cls.BASE_URL}/parameter/{parameter_id}.json"

    @classmethod
    def _build_data_url(cls, gauge_id: str, parameter_id: int) -> str:
        return f"{cls.BASE_URL}/parameter/{parameter_id}/station/{gauge_id}/period/corrected-archive/data.json"

    def _download_json(self, url: str, params: Optional[dict[str, str]] = None) -> dict[str, Any]:
        session = utils.requests_retry_session()
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    def _fetch_parameter_metadata(self, parameter_id: int) -> pd.DataFrame:
        """Fetches and standardizes station metadata for a single SMHI parameter."""
        url = self._build_parameter_url(parameter_id)

        try:
            payload = self._download_json(url)
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Swedish metadata for parameter {parameter_id}: {exc}")
            return self._empty_metadata_frame()
        except ValueError as exc:
            logger.error(f"Failed to decode Swedish metadata for parameter {parameter_id}: {exc}")
            return self._empty_metadata_frame()

        stations = payload.get("station", [])
        if not isinstance(stations, list) or not stations:
            return self._empty_metadata_frame()

        rows = []
        for station in stations:
            row = dict(station)
            gauge_id = str(station.get("id") or station.get("key") or "").strip()
            if not gauge_id:
                continue

            row[constants.GAUGE_ID] = gauge_id
            row[constants.STATION_NAME] = station.get("name")
            row[constants.RIVER] = station.get("catchmentName")
            row[constants.LATITUDE] = pd.to_numeric(station.get("latitude"), errors="coerce")
            row[constants.LONGITUDE] = pd.to_numeric(station.get("longitude"), errors="coerce")
            row[constants.ALTITUDE] = np.nan
            row[constants.AREA] = self._normalize_area(station.get("catchmentSize"))
            row[constants.COUNTRY] = self.COUNTRY
            row[constants.SOURCE] = self.SOURCE
            row["parameter_id"] = parameter_id
            row["from"] = self._to_local_timestamp(station.get("from"))
            row["to"] = self._to_local_timestamp(station.get("to"))
            rows.append(row)

        if not rows:
            return self._empty_metadata_frame()

        df = pd.DataFrame(rows)
        return df.set_index(constants.GAUGE_ID)

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata for all supported Swedish parameters."""
        frames = []
        for config in self.VARIABLE_CONFIG.values():
            frame = self._fetch_parameter_metadata(config["parameter_id"])
            if not frame.empty:
                frames.append(frame)

        if not frames:
            return self._empty_metadata_frame()

        df = pd.concat(frames, axis=0)
        df = df[~df.index.duplicated(keep="first")]
        df.index = df.index.astype(str)
        return df.sort_index()

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> dict[str, Any]:
        """Downloads raw JSON time-series data from SMHI."""
        config = self.VARIABLE_CONFIG.get(variable)
        if config is None:
            raise ValueError(f"Unsupported variable: {variable}")

        params = {
            "from": self._format_query_datetime(start_date),
            "to": self._format_query_datetime(end_date, end_of_day=True),
        }
        url = self._build_data_url(gauge_id, config["parameter_id"])
        return self._download_json(url, params=params)

    def _parse_data(self, gauge_id: str, raw_data: dict[str, Any], variable: str) -> pd.DataFrame:
        """Parses raw SMHI JSON into a standardized time-series DataFrame."""
        values = raw_data.get("value", []) if isinstance(raw_data, dict) else []
        if not isinstance(values, list) or not values:
            return self._empty_result(variable)

        df = pd.DataFrame(values)
        required_columns = {"date", "value"}
        if df.empty or not required_columns.issubset(df.columns):
            return self._empty_result(variable)

        result = pd.DataFrame(
            {
                constants.TIME_INDEX: self._to_local_series(df["date"]),
                variable: pd.to_numeric(df["value"], errors="coerce"),
            }
        )

        scale = self.VARIABLE_CONFIG[variable]["scale"]
        result[variable] = result[variable] * scale
        result = result.dropna(subset=[constants.TIME_INDEX, variable])
        if result.empty:
            return self._empty_result(variable)

        result = result.sort_values(constants.TIME_INDEX)
        result = result.drop_duplicates(subset=[constants.TIME_INDEX], keep="last")
        return result.set_index(constants.TIME_INDEX)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)
        except Exception as exc:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {exc}")
            return self._empty_result(variable)

        if df.empty:
            return self._empty_result(variable)

        start_date_dt = pd.to_datetime(start_date)
        end_date_dt = pd.to_datetime(end_date)
        if constants.INSTANTANEOUS in variable or constants.HOURLY in variable:
            end_date_dt = end_date_dt + pd.Timedelta(days=1)
            return df[(df.index >= start_date_dt) & (df.index < end_date_dt)]

        return df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
