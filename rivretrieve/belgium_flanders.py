"""Fetcher for Belgium-Flanders river gauge data from the HIC KiWIS service."""

import logging
import math
import re
from typing import Any, Optional

import numpy as np
import pandas as pd

from . import base, constants, utils

logger = logging.getLogger(__name__)


class BelgiumFlandersFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the HIC KiWIS service for Flanders.

    Data source:
        - website: https://hicws.vlaanderen.be/KiWIS/KiWIS

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)

    Data description and API:
        - HIC KiWIS endpoint: https://hicws.vlaanderen.be/KiWIS/KiWIS
        - webservices manual: https://hicws.vlaanderen.be/Manual_for_the_use_of_webservices_HIC.pdf

    Terms of use:
        - see https://hicws.vlaanderen.be/

    Notes:
        - The HIC service exposes parameter groups instead of RivRetrieve-native variable names.
        - This fetcher translates HIC series into RivRetrieve daily-mean variables.
        - Metadata excludes the virtual discharge-only group ``260592`` from the upstream service.
    """

    BASE_URL = "https://hicws.vlaanderen.be/KiWIS/KiWIS"
    SOURCE = "Hydrological Information Centre - HIC (Flanders)"
    COUNTRY = "Belgium"
    LOCAL_TIMEZONE = "Europe/Brussels"
    VIRTUAL_GROUP_ID = "260592"
    VARIABLE_MAP = {
        constants.DISCHARGE_DAILY_MEAN: {
            "group_id": "156169",
            "unit": "m^3/s",
        },
        constants.STAGE_DAILY_MEAN: {
            "group_id": "156162",
            "unit": "m",
        },
        constants.WATER_TEMPERATURE_DAILY_MEAN: {
            "group_id": "156200",
            "unit": "degC",
        },
    }

    def __init__(self):
        self._timeseries_map_cache: dict[str, pd.DataFrame] = {}

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Belgium-Flanders gauge metadata."""
        return utils.load_cached_metadata_csv("belgium_flanders")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(BelgiumFlandersFetcher.VARIABLE_MAP.keys())

    @staticmethod
    def _empty_data_frame(variable: str) -> pd.DataFrame:
        return pd.DataFrame(columns=[constants.TIME_INDEX, variable]).set_index(constants.TIME_INDEX)

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
            "vertical_datum",
        ]
        return pd.DataFrame(columns=columns).set_index(constants.GAUGE_ID)

    def _request_json(self, params: dict[str, Any]) -> Any:
        session = utils.requests_retry_session(retries=6, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
        response = session.get(self.BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _parse_header_table(payload: Any) -> pd.DataFrame:
        """Parses KiWIS header-row JSON tables."""
        if not isinstance(payload, list) or not payload:
            return pd.DataFrame()

        if not isinstance(payload[0], list):
            return pd.DataFrame(payload)

        headers = [str(value).strip() for value in payload[0]]
        rows = payload[1:]
        if not rows:
            return pd.DataFrame(columns=headers)
        return pd.DataFrame(rows, columns=headers)

    @staticmethod
    def _parse_values_payload(payload: Any) -> pd.DataFrame:
        """Parses KiWIS ``getTimeseriesValues`` JSON payloads."""
        if not isinstance(payload, list) or not payload:
            return pd.DataFrame()

        first = payload[0]
        if not isinstance(first, dict):
            return pd.DataFrame()

        columns = first.get("columns", [])
        if isinstance(columns, str):
            columns = [column.strip() for column in columns.split(",")]

        data = first.get("data", [])
        if not data:
            return pd.DataFrame(columns=columns or ["Timestamp", "Value"])

        return pd.DataFrame(data, columns=columns)

    @staticmethod
    def _parse_area_km2(value: Any) -> float:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return np.nan

        text = str(value).strip()
        if not text:
            return np.nan

        match = re.search(r"[-+]?\d+(?:[.,]\d+)?", text)
        if not match:
            return np.nan

        area = pd.to_numeric(match.group(0).replace(",", "."), errors="coerce")
        if pd.isna(area) or abs(area - 1.0) < 1e-9:
            return np.nan
        return float(area)

    @staticmethod
    def _split_station_name(raw_name: Any) -> tuple[Optional[str], Optional[str]]:
        if raw_name is None or (isinstance(raw_name, float) and math.isnan(raw_name)):
            return None, None

        name = str(raw_name).strip()
        if "/" not in name:
            return name or None, None

        station_name, river = name.split("/", 1)
        station_name = station_name.strip() or None
        river = river.strip() or None
        return station_name, river

    def _get_station_list(self) -> pd.DataFrame:
        params = {
            "service": "kisters",
            "type": "queryServices",
            "request": "getStationList",
            "datasource": 4,
            "format": "json",
            "returnfields": "station_no,station_name,station_latitude,station_longitude,site_name,ca_sta",
            "ca_sta_returnfields": "",
        }
        return self._parse_header_table(self._request_json(params))

    def _get_timeseries_map(self, variable: str) -> pd.DataFrame:
        if variable in self._timeseries_map_cache:
            return self._timeseries_map_cache[variable].copy()

        config = self.VARIABLE_MAP[variable]
        params = {
            "service": "kisters",
            "type": "queryServices",
            "request": "getTimeseriesList",
            "timeseriesgroup_id": config["group_id"],
            "datasource": 4,
            "format": "json",
            "returnfields": "station_no,ts_id",
        }
        df = self._parse_header_table(self._request_json(params))
        if df.empty:
            parsed = pd.DataFrame(columns=[constants.GAUGE_ID, "ts_id"])
        else:
            parsed = (
                df.rename(columns={"station_no": constants.GAUGE_ID})[[constants.GAUGE_ID, "ts_id"]]
                .dropna(subset=[constants.GAUGE_ID, "ts_id"])
                .assign(
                    **{
                        constants.GAUGE_ID: lambda frame: frame[constants.GAUGE_ID].astype(str).str.strip(),
                        "ts_id": lambda frame: frame["ts_id"].astype(str).str.strip(),
                    }
                )
                .drop_duplicates()
            )

        self._timeseries_map_cache[variable] = parsed
        return parsed.copy()

    def _get_virtual_station_ids(self) -> set[str]:
        params = {
            "service": "kisters",
            "type": "queryServices",
            "request": "getTimeseriesList",
            "timeseriesgroup_id": self.VIRTUAL_GROUP_ID,
            "datasource": 4,
            "format": "json",
            "returnfields": "station_no,ts_id",
        }
        df = self._parse_header_table(self._request_json(params))
        if df.empty or "station_no" not in df.columns:
            return set()
        return set(df["station_no"].dropna().astype(str).str.strip())

    def get_metadata(self) -> pd.DataFrame:
        """Fetches live metadata for stations with supported Belgium-Flanders variables."""
        station_df = self._get_station_list()
        if station_df.empty:
            return self._empty_metadata_frame()

        supported_station_ids = set()
        for variable in self.get_available_variables():
            supported_station_ids.update(self._get_timeseries_map(variable)[constants.GAUGE_ID].tolist())

        if not supported_station_ids:
            return self._empty_metadata_frame()

        virtual_station_ids = self._get_virtual_station_ids()
        station_df = station_df.rename(columns={"station_no": constants.GAUGE_ID})
        station_df[constants.GAUGE_ID] = station_df[constants.GAUGE_ID].astype(str).str.strip()
        station_df = station_df[station_df[constants.GAUGE_ID].isin(supported_station_ids)]
        station_df = station_df[~station_df[constants.GAUGE_ID].isin(virtual_station_ids)]

        records = []
        for _, row in station_df.iterrows():
            station_name, river_from_name = self._split_station_name(row.get("station_name"))
            river = river_from_name or row.get("river_name")
            records.append(
                {
                    constants.GAUGE_ID: row.get(constants.GAUGE_ID),
                    constants.STATION_NAME: station_name,
                    constants.RIVER: river,
                    constants.LATITUDE: pd.to_numeric(row.get("station_latitude"), errors="coerce"),
                    constants.LONGITUDE: pd.to_numeric(row.get("station_longitude"), errors="coerce"),
                    constants.ALTITUDE: pd.to_numeric(row.get("ALTITUDE"), errors="coerce"),
                    constants.AREA: self._parse_area_km2(row.get("CATCHMENT_SIZE")),
                    constants.COUNTRY: self.COUNTRY,
                    constants.SOURCE: self.SOURCE,
                    "vertical_datum": row.get("station_gauge_datum_postfix"),
                }
            )

        if not records:
            return self._empty_metadata_frame()

        df = pd.DataFrame(records)
        df = df.dropna(subset=[constants.LATITUDE, constants.LONGITUDE])
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return df.set_index(constants.GAUGE_ID)

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> list[pd.DataFrame]:
        ts_map = self._get_timeseries_map(variable)
        ts_ids = ts_map.loc[ts_map[constants.GAUGE_ID] == str(gauge_id), "ts_id"].dropna().unique().tolist()

        if not ts_ids:
            return []

        start_ts = f"{start_date}T00:00:00Z"
        end_ts = f"{end_date}T23:59:59Z"
        payloads: list[pd.DataFrame] = []

        for ts_id in ts_ids:
            params = {
                "service": "kisters",
                "type": "queryServices",
                "request": "getTimeseriesValues",
                "format": "json",
                "datasource": 4,
                "ts_id": ts_id,
                "from": start_ts,
                "to": end_ts,
                "returnfields": "Timestamp,Value,Quality Code,Quality Code Name,Quality Code Description",
            }
            payload = self._parse_values_payload(self._request_json(params))
            if not payload.empty:
                payloads.append(payload)

        return payloads

    def _parse_data(self, gauge_id: str, raw_data: list[pd.DataFrame], variable: str) -> pd.DataFrame:
        if not raw_data:
            return self._empty_data_frame(variable)

        df = pd.concat(raw_data, ignore_index=True)
        if df.empty or "Timestamp" not in df.columns or "Value" not in df.columns:
            return self._empty_data_frame(variable)

        timestamps = pd.to_datetime(df["Timestamp"], utc=True, errors="coerce")
        timestamps = timestamps.dt.tz_convert(self.LOCAL_TIMEZONE).dt.tz_localize(None)

        parsed = pd.DataFrame(
            {
                constants.TIME_INDEX: timestamps,
                variable: pd.to_numeric(df["Value"], errors="coerce"),
            }
        ).dropna(subset=[constants.TIME_INDEX, variable])

        if parsed.empty:
            return self._empty_data_frame(variable)

        parsed[constants.TIME_INDEX] = parsed[constants.TIME_INDEX].dt.floor("D")
        parsed = parsed.groupby(constants.TIME_INDEX, as_index=False)[variable].mean()
        return parsed.set_index(constants.TIME_INDEX).sort_index()

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(str(gauge_id), variable, start_date, end_date)
            df = self._parse_data(str(gauge_id), raw_data, variable)
        except Exception as exc:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {exc}")
            return self._empty_data_frame(variable)

        if df.empty:
            return df

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        return df[(df.index >= start_dt) & (df.index <= end_dt)]
