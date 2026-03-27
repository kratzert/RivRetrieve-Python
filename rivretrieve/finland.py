"""Fetcher for Finnish river gauge data from the SYKE hydrology API."""

import logging
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests
from pyproj import Transformer

from . import base, constants, utils

logger = logging.getLogger(__name__)


class FinlandFetcher(base.RiverDataFetcher):
    """Fetches Finnish river gauge data from the SYKE hydrology API.

    Data source:
        https://wwwi3.ymparisto.fi/i3/paasivu/eng/etusivu/etusivu.htm

    Supported variables:
        - constants.DISCHARGE_DAILY_MEAN (m³/s)
        - constants.STAGE_DAILY_MEAN (m)
        - constants.WATER_TEMPERATURE_DAILY_MEAN (°C)

    Data description and API:
        - https://rajapinnat.ymparisto.fi/api/Hydrologiarajapinta/1.1/

    Terms of use:
        - https://www.syke.fi/en/environmental-data/use-license-and-responsibilities
    """

    ODATA_URL = "https://rajapinnat.ymparisto.fi/api/Hydrologiarajapinta/1.1/odata"
    METADATA_URL = f"{ODATA_URL}/Paikka"
    COUNTRY = "Finland"
    SOURCE = "Finnish Environment Institute (SYKE)"
    METADATA_PAGE_SIZE = 500
    MAX_TIMESERIES_WINDOW_DAYS = 365
    SUPPORTED_VARIABLE_COLUMN = "supported_variable"
    VARIABLE_MAP = {
        constants.DISCHARGE_DAILY_MEAN: {
            "resource": "Virtaama",
            "scale": 1.0,
            "supported_suure_id": 2,
        },
        constants.STAGE_DAILY_MEAN: {
            "resource": "Vedenkorkeus",
            "scale": 0.01,
            "supported_suure_id": 1,
        },
        constants.WATER_TEMPERATURE_DAILY_MEAN: {
            "resource": "LampoPintavesi",
            "scale": 1.0,
            "supported_suure_id": 11,
        },
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
            FinlandFetcher.SUPPORTED_VARIABLE_COLUMN,
        ]
        return pd.DataFrame(columns=columns).set_index(constants.GAUGE_ID)

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Finnish gauge metadata."""
        return utils.load_cached_metadata_csv("finland")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(FinlandFetcher.VARIABLE_MAP.keys())

    @classmethod
    def _metadata_filter(cls) -> str:
        supported_ids = sorted({config["supported_suure_id"] for config in cls.VARIABLE_MAP.values()})
        return " or ".join([f"Suure_Id eq {supported_id}" for supported_id in supported_ids])

    @staticmethod
    def _sort_by_gauge_id(df: pd.DataFrame) -> pd.DataFrame:
        gauge_ids = pd.to_numeric(df[constants.GAUGE_ID], errors="coerce")
        result = df.copy()
        result["_gauge_sort"] = gauge_ids
        result = result.sort_values(["_gauge_sort", constants.GAUGE_ID], na_position="last")
        return result.drop(columns="_gauge_sort")

    @classmethod
    def _split_date_windows(cls, start_date: str, end_date: str) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        if pd.isna(start) or pd.isna(end) or start > end:
            return []

        windows = []
        current = start
        while current <= end:
            window_end = min(end, current + pd.Timedelta(days=cls.MAX_TIMESERIES_WINDOW_DAYS - 1))
            windows.append((current, window_end))
            current = window_end + pd.Timedelta(days=1)
        return windows

    @staticmethod
    def _fetch_odata_records(
        session: requests.Session,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        headers = {"Accept": "application/json"}
        records: list[dict[str, Any]] = []
        next_url = url
        next_params = params

        while next_url:
            response = session.get(next_url, headers=headers, params=next_params, timeout=60)
            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("Unexpected OData payload.")

            page_records = payload.get("value", [])
            if not isinstance(page_records, list):
                raise ValueError("Unexpected OData record list.")

            records.extend(page_records)
            next_url = payload.get("odata.nextLink")
            next_params = None

        return records

    @staticmethod
    def _transform_coordinates(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        easting = pd.to_numeric(df.get("KoordErTmIta"), errors="coerce")
        northing = pd.to_numeric(df.get("KoordErTmPohj"), errors="coerce")
        longitude = np.full(len(df), np.nan)
        latitude = np.full(len(df), np.nan)
        valid = easting.notna() & northing.notna()

        if valid.any():
            transformer = Transformer.from_crs("EPSG:3067", "EPSG:4326", always_xy=True)
            lon_values, lat_values = transformer.transform(easting[valid].to_numpy(), northing[valid].to_numpy())
            longitude[valid.to_numpy()] = lon_values
            latitude[valid.to_numpy()] = lat_values

        return longitude, latitude

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata for supported Finnish hydrology stations."""
        params = {
            "$filter": self._metadata_filter(),
            "$orderby": "Paikka_Id asc",
            "$top": self.METADATA_PAGE_SIZE,
        }
        session = utils.requests_retry_session()

        try:
            records = self._fetch_odata_records(session, self.METADATA_URL, params=params)
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Finnish metadata: {exc}")
            raise
        except ValueError as exc:
            logger.error(f"Failed to decode Finnish metadata: {exc}")
            raise

        if not records:
            return self._empty_metadata_frame()

        df = pd.DataFrame(records)
        if df.empty:
            return self._empty_metadata_frame()

        rename_map = {
            "Paikka_Id": constants.GAUGE_ID,
            "Nimi": constants.STATION_NAME,
            "PaaVesalNimi": constants.RIVER,
        }
        df = df.rename(columns=rename_map)
        df[self.SUPPORTED_VARIABLE_COLUMN] = df["Suure_Id"].map(
            {config["supported_suure_id"]: variable for variable, config in self.VARIABLE_MAP.items()}
        )

        longitude, latitude = self._transform_coordinates(df)
        df[constants.LONGITUDE] = longitude
        df[constants.LATITUDE] = latitude

        standard_columns = [
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
        for column in standard_columns:
            if column not in df.columns:
                df[column] = np.nan

        df[constants.COUNTRY] = self.COUNTRY
        df[constants.SOURCE] = self.SOURCE
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
        df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")
        df[constants.ALTITUDE] = pd.to_numeric(df[constants.ALTITUDE], errors="coerce")
        df[constants.AREA] = pd.to_numeric(df[constants.AREA], errors="coerce")

        df = self._sort_by_gauge_id(df.drop_duplicates(subset=[constants.GAUGE_ID]))
        df = df.set_index(constants.GAUGE_ID)
        return df

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Downloads raw JSON data from the SYKE OData API."""
        config = self.VARIABLE_MAP[variable]
        resource = config["resource"]
        windows = self._split_date_windows(start_date, end_date)

        try:
            paikka_id = int(str(gauge_id).strip())
        except ValueError as exc:
            raise ValueError("Finland gauge_id must be a numeric Paikka_Id.") from exc

        session = utils.requests_retry_session()
        records: list[dict[str, Any]] = []

        try:
            for window_start, window_end in windows:
                params = {
                    "$filter": (
                        f"Paikka_Id eq {paikka_id} and "
                        f"Aika ge datetime'{window_start.strftime('%Y-%m-%d')}T00:00:00' and "
                        f"Aika le datetime'{window_end.strftime('%Y-%m-%d')}T23:59:59'"
                    ),
                    "$orderby": "Aika asc",
                    "$top": self.METADATA_PAGE_SIZE,
                }
                records.extend(self._fetch_odata_records(session, f"{self.ODATA_URL}/{resource}", params=params))
            return records
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Finnish data for {gauge_id}: {exc}")
            raise
        except ValueError as exc:
            logger.error(f"Failed to decode Finnish data for {gauge_id}: {exc}")
            raise

    def _parse_data(self, gauge_id: str, raw_data: list[dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses raw SYKE payloads into the standard RivRetrieve layout."""
        if not raw_data:
            return self._empty_result(variable)

        config = self.VARIABLE_MAP[variable]
        df = pd.DataFrame(raw_data)
        if df.empty or "Aika" not in df.columns or "Arvo" not in df.columns:
            return self._empty_result(variable)

        df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(df["Aika"], errors="coerce").dt.floor("D"),
                variable: pd.to_numeric(df["Arvo"], errors="coerce") * config["scale"],
            }
        )
        df = df.dropna(subset=[constants.TIME_INDEX, variable])
        if df.empty:
            return self._empty_result(variable)

        df = df.drop_duplicates(subset=[constants.TIME_INDEX], keep="last").sort_values(constants.TIME_INDEX)
        return df.set_index(constants.TIME_INDEX)

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

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        return df[(df.index >= start_dt) & (df.index <= end_dt)]
