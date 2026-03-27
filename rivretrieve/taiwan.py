"""Fetcher for Taiwanese river gauge data from the Water Resources Agency."""

import logging
import os
import re
import warnings
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from pyproj import Transformer
from urllib3.exceptions import InsecureRequestWarning

from . import base, constants, utils

logger = logging.getLogger(__name__)

_DOTNET_DATE_RE = re.compile(r"/Date\((\d+)\)/")
_TWD97_TM2_TO_WGS84 = Transformer.from_crs("EPSG:3826", "EPSG:4326", always_xy=True)


class TaiwanFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Taiwan's Water Resources Agency.

    Data source:
        - Metadata comes from the Water Data Integration Cloud Platform OpenAPI:
          https://opendata.wra.gov.tw/openapi/swagger/index.html
        - Historical discharge and water-level values come from Water Resources Agency HydroInfo:
          https://gweb.wra.gov.tw/HydroInfo/

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_HOURLY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.STAGE_HOURLY_MEAN`` (m)

    Data description and API:
        - HydroInfo user manual:
          https://gweb.wra.gov.tw/Hydroinfo/ExDoc/%E7%B3%BB%E7%B5%B1%E6%93%8D%E4%BD%9C%E6%89%8B%E5%86%8A%28113%E5%B9%B4%29.pdf
        - Water Data Integration Cloud Platform OpenAPI:
          https://opendata.wra.gov.tw/openapi/swagger/index.html

    Terms of use:
        - Open Government Data License, version 1.0:
          https://data.gov.tw/en/license
    """

    TIMEZONE = ZoneInfo("Asia/Taipei")
    COUNTRY = "Taiwan"
    SOURCE = "Water Resources Agency (Taiwan)"
    METADATA_URL = "https://opendata.wra.gov.tw/api/v2/9332bd66-0213-4380-a5d5-a43e7be49255"
    HYDROINFO_BASE_URL = "https://gweb.wra.gov.tw/HydroInfo/Comm"
    METADATA_PAGE_SIZE = 1000
    VARIABLE_CONFIG = {
        constants.DISCHARGE_DAILY_MEAN: {
            "endpoint": "GetStDIDayList",
            "value_field": "DAVG",
            "hourly": False,
            "stage": False,
        },
        constants.DISCHARGE_HOURLY_MEAN: {
            "endpoint": "GetStDIHourList",
            "value_field": "HAVG",
            "hourly": True,
            "stage": False,
        },
        constants.STAGE_DAILY_MEAN: {
            "endpoint": "GetStLeDayList",
            "value_field": "DAVG",
            "hourly": False,
            "stage": True,
        },
        constants.STAGE_HOURLY_MEAN: {
            "endpoint": "GetStLeHourList",
            "value_field": "HAVG",
            "hourly": True,
            "stage": True,
        },
    }

    def __init__(self):
        super().__init__()
        self._cached_metadata: Optional[pd.DataFrame] = None

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
        """Retrieves cached Taiwanese gauge metadata."""
        return utils.load_cached_metadata_csv("taiwan")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(TaiwanFetcher.VARIABLE_CONFIG.keys())

    @staticmethod
    def _parse_twd97_coordinates(value: Any) -> tuple[Optional[float], Optional[float]]:
        if not isinstance(value, str) or not value.strip():
            return None, None

        parts = value.split()
        if len(parts) != 2:
            return None, None

        try:
            x = float(parts[0])
            y = float(parts[1])
        except ValueError:
            return None, None

        lon, lat = _TWD97_TM2_TO_WGS84.transform(x, y)
        return lon, lat

    @classmethod
    def _normalize_zero_point(cls, value: Any) -> float:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return float("nan")

        zero_point = float(numeric)
        # WRA mixes true meter values with integer centimeter encodings.
        if abs(zero_point) >= 100 and zero_point.is_integer():
            zero_point = zero_point / 100.0

        return zero_point

    @staticmethod
    def _request_json(request_callable, url: str, **kwargs) -> Any:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InsecureRequestWarning)
            response = request_callable(url, verify=False, **kwargs)
        response.raise_for_status()
        return response.json()

    @classmethod
    def _build_metadata_frame(cls, records: list[dict[str, Any]]) -> pd.DataFrame:
        rows = []
        for record in records:
            gauge_id = str(record.get("basinidentifier") or "").strip()
            if not gauge_id:
                continue

            lon, lat = cls._parse_twd97_coordinates(record.get("locationbytwd97_xy"))
            zero_point = cls._normalize_zero_point(record.get("elevationofwaterlevelzeropoint"))
            watershed_area = pd.to_numeric(record.get("watershedarea"), errors="coerce")

            row = dict(record)
            row.update(
                {
                    constants.GAUGE_ID: gauge_id,
                    constants.STATION_NAME: record.get("observatoryname"),
                    constants.RIVER: record.get("rivername"),
                    constants.LATITUDE: lat,
                    constants.LONGITUDE: lon,
                    constants.ALTITUDE: zero_point,
                    constants.AREA: watershed_area,
                    constants.COUNTRY: cls.COUNTRY,
                    constants.SOURCE: cls.SOURCE,
                    "zero_point_elevation_m": zero_point,
                }
            )
            rows.append(row)

        if not rows:
            return cls._empty_metadata_frame()

        df = pd.DataFrame(rows)
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
        df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")
        df[constants.ALTITUDE] = pd.to_numeric(df[constants.ALTITUDE], errors="coerce")
        df[constants.AREA] = pd.to_numeric(df[constants.AREA], errors="coerce")

        return df.set_index(constants.GAUGE_ID).sort_index()

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and parses Taiwanese flow-station metadata from the WRA OpenAPI."""
        session = utils.requests_retry_session()
        page = 1
        records: list[dict[str, Any]] = []

        try:
            while True:
                page_records = self._request_json(
                    session.get,
                    self.METADATA_URL,
                    params={"page": page, "size": self.METADATA_PAGE_SIZE, "format": "JSON"},
                    timeout=60,
                )
                if not isinstance(page_records, list) or not page_records:
                    break

                records.extend(page_records)
                if len(page_records) < self.METADATA_PAGE_SIZE:
                    break
                page += 1
        except Exception as exc:
            logger.error(f"Failed to fetch Taiwanese metadata: {exc}")
            return self._empty_metadata_frame()

        df = self._build_metadata_frame(records)
        self._cached_metadata = df
        return df

    def _get_metadata_frame(self) -> pd.DataFrame:
        if self._cached_metadata is None:
            cache_path = os.path.join(os.path.dirname(__file__), "cached_site_data", "taiwan_sites.csv")
            if os.path.exists(cache_path):
                self._cached_metadata = self.get_cached_metadata()
            else:
                self._cached_metadata = self.get_metadata()
        return self._cached_metadata

    def _get_stage_zero_point(self, gauge_id: str) -> Optional[float]:
        metadata = self._get_metadata_frame()
        if gauge_id not in metadata.index:
            return None

        zero_point = pd.to_numeric(
            metadata.loc[gauge_id, "zero_point_elevation_m"],
            errors="coerce",
        )
        return None if pd.isna(zero_point) else float(zero_point)

    @staticmethod
    def _parse_daily_dates(raw_dates: pd.Series) -> pd.Series:
        return pd.to_datetime(raw_dates, errors="coerce")

    @classmethod
    def _parse_dotnet_dates(cls, raw_dates: pd.Series) -> pd.Series:
        milliseconds = pd.to_numeric(raw_dates.astype(str).str.extract(_DOTNET_DATE_RE)[0], errors="coerce")
        timestamps = pd.to_datetime(milliseconds, unit="ms", utc=True, errors="coerce")
        # Hourly payloads are encoded as UTC milliseconds but represent Taiwan local time.
        return timestamps.dt.tz_convert(cls.TIMEZONE).dt.tz_localize(None)

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Downloads raw JSON time series data from the HydroInfo backend."""
        config = self.VARIABLE_CONFIG[variable]
        session = utils.requests_retry_session()
        data = self._request_json(
            session.post,
            f"{self.HYDROINFO_BASE_URL}/{config['endpoint']}",
            data={"ST_NO": gauge_id},
            timeout=180,
        )
        return data if isinstance(data, list) else []

    def _parse_data(self, gauge_id: str, raw_data: list[dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses HydroInfo JSON payloads into the standard RivRetrieve layout."""
        if not raw_data:
            return self._empty_result(variable)

        config = self.VARIABLE_CONFIG[variable]
        df = pd.DataFrame(raw_data)
        if df.empty or config["value_field"] not in df.columns:
            return self._empty_result(variable)

        if config["hourly"]:
            if "YYMMDD" not in df.columns:
                return self._empty_result(variable)
            times = self._parse_dotnet_dates(df["YYMMDD"])
        else:
            if "yymmdd" not in df.columns:
                return self._empty_result(variable)
            times = self._parse_daily_dates(df["yymmdd"])

        values = pd.to_numeric(df[config["value_field"]], errors="coerce")
        parsed = pd.DataFrame({constants.TIME_INDEX: times, variable: values}).dropna(
            subset=[constants.TIME_INDEX, variable]
        )

        if config["stage"]:
            zero_point = self._get_stage_zero_point(gauge_id)
            if zero_point is not None:
                parsed[variable] = parsed[variable] - zero_point
            else:
                logger.warning(
                    f"Missing zero-point elevation for gauge {gauge_id}; returning raw water-level elevations."
                )

        parsed = parsed.sort_values(constants.TIME_INDEX).drop_duplicates(subset=[constants.TIME_INDEX], keep="last")
        return parsed.set_index(constants.TIME_INDEX)

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
                These are typically defined in ``rivretrieve.constants``.
            start_date: Optional start date for the data retrieval in 'YYYY-MM-DD' format.
                If None, data is fetched from the earliest available date.
            end_date: Optional end date for the data retrieval in 'YYYY-MM-DD' format.
                If None, data is fetched up to the latest available date.

        Returns:
            pd.DataFrame: A pandas DataFrame indexed by datetime objects
            (``constants.TIME_INDEX``) with a single column named after the
            requested ``variable``. The DataFrame will be empty if no data is found
            for the given parameters.

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
        except Exception as exc:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {exc}")
            return self._empty_result(variable)

        if df.empty:
            return self._empty_result(variable)

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        if variable in (constants.DISCHARGE_HOURLY_MEAN, constants.STAGE_HOURLY_MEAN):
            end_dt = end_dt + pd.Timedelta(days=1)
            return df[(df.index >= start_dt) & (df.index < end_dt)]

        return df[(df.index >= start_dt) & (df.index <= end_dt)]
