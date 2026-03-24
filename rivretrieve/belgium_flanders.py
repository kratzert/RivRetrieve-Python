"""Fetcher for Belgium-Flanders river gauge data from HIC and VMM KiWIS services."""

import logging
import math
import re
from typing import Any, Optional

import numpy as np
import pandas as pd

from . import base, constants, utils

logger = logging.getLogger(__name__)


class BelgiumFlandersFetcher(base.RiverDataFetcher):
    """Fetches river gauge data for Flanders from HIC and VMM KiWIS services.

    Data source:
        - HIC KiWIS: https://hicws.vlaanderen.be/KiWIS/KiWIS
        - VMM KiWIS: https://download.waterinfo.be/tsmdownload/KiWIS/KiWIS

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)

    Data description and API:
        - HIC webservices manual: https://hicws.vlaanderen.be/Manual_for_the_use_of_webservices_HIC.pdf
        - pywaterinfo tutorial documenting the Flemish KiWIS backends:
          https://fluves.github.io/pywaterinfo/tutorial.html

    Terms of use:
        - HIC: https://hicws.vlaanderen.be/
        - VMM waterinfo: https://www.waterinfo.be/

    Notes:
        - HIC mainly covers navigable waterways.
        - VMM provides a separate KiWIS backend covering additional Flemish stations,
          including non-navigable waters.
        - This fetcher keeps the RivRetrieve variable surface area limited to daily
          discharge, stage, and water temperature.
    """

    COUNTRY = "Belgium"
    LOCAL_TIMEZONE = "Europe/Brussels"
    SOURCE = "Hydrological Information Centre - HIC (Flanders) / Flemish Environment Agency - VMM"
    PROVIDERS = {
        "hic": {
            "base_url": "https://hicws.vlaanderen.be/KiWIS/KiWIS",
            "datasource": 4,
            "source": "Hydrological Information Centre - HIC (Flanders)",
            "station_list_returnfields": "station_no,station_name,station_latitude,station_longitude,site_name,ca_sta",
            "ca_sta_returnfields": "",
            "virtual_group_id": "260592",
            "variable_map": {
                constants.DISCHARGE_DAILY_MEAN: {"group_id": "156169", "unit": "m^3/s"},
                constants.STAGE_DAILY_MEAN: {"group_id": "156162", "unit": "m"},
                constants.WATER_TEMPERATURE_DAILY_MEAN: {"group_id": "156200", "unit": "degC"},
            },
        },
        "vmm": {
            "base_url": "https://download.waterinfo.be/tsmdownload/KiWIS/KiWIS",
            "datasource": 1,
            "source": "Flemish Environment Agency - VMM",
            "station_list_returnfields": (
                "station_no,station_name,station_latitude,station_longitude,site_name,river_name"
            ),
            "virtual_group_id": None,
            "variable_map": {
                constants.DISCHARGE_DAILY_MEAN: {"group_id": "192893", "unit": "m^3/s"},
                constants.STAGE_DAILY_MEAN: {"group_id": "192782", "unit": "m"},
                constants.WATER_TEMPERATURE_DAILY_MEAN: {"group_id": "325066", "unit": "degC"},
            },
        },
    }

    def __init__(self):
        self._timeseries_map_cache: dict[tuple[str, str], pd.DataFrame] = {}
        self._station_list_cache: dict[str, pd.DataFrame] = {}

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Belgium-Flanders gauge metadata."""
        return utils.load_cached_metadata_csv("belgium_flanders")

    @classmethod
    def get_available_variables(cls) -> tuple[str, ...]:
        variables = []
        for provider_config in cls.PROVIDERS.values():
            for variable in provider_config["variable_map"]:
                if variable not in variables:
                    variables.append(variable)
        return tuple(variables)

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
            "provider",
            "vertical_datum",
        ]
        return pd.DataFrame(columns=columns).set_index(constants.GAUGE_ID)

    def _request_json(self, provider: str, params: dict[str, Any]) -> Any:
        config = self.PROVIDERS[provider]
        session = utils.requests_retry_session(retries=6, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
        response = session.get(config["base_url"], params=params, timeout=60)
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

        station_name, river = name.rsplit("/", 1)
        station_name = station_name.strip() or None
        river = river.strip() or None
        return station_name, river

    @staticmethod
    def _normalize_string(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        text = str(value).strip()
        if not text or text == "---":
            return None
        return text

    def _get_station_list(self, provider: str) -> pd.DataFrame:
        if provider in self._station_list_cache:
            return self._station_list_cache[provider].copy()

        config = self.PROVIDERS[provider]
        params = {
            "service": "kisters",
            "type": "queryServices",
            "request": "getStationList",
            "datasource": config["datasource"],
            "format": "json",
            "returnfields": config["station_list_returnfields"],
        }
        if "ca_sta_returnfields" in config:
            params["ca_sta_returnfields"] = config["ca_sta_returnfields"]

        station_list = self._parse_header_table(self._request_json(provider, params))
        self._station_list_cache[provider] = station_list
        return station_list.copy()

    def _get_timeseries_map(self, provider: str, variable: str) -> pd.DataFrame:
        cache_key = (provider, variable)
        if cache_key in self._timeseries_map_cache:
            return self._timeseries_map_cache[cache_key].copy()

        config = self.PROVIDERS[provider]
        variable_config = config["variable_map"].get(variable)
        if variable_config is None:
            parsed = pd.DataFrame(columns=[constants.GAUGE_ID, "ts_id", "provider"])
            self._timeseries_map_cache[cache_key] = parsed
            return parsed.copy()

        params = {
            "service": "kisters",
            "type": "queryServices",
            "request": "getTimeseriesList",
            "timeseriesgroup_id": variable_config["group_id"],
            "datasource": config["datasource"],
            "format": "json",
            "returnfields": "station_no,ts_id",
        }
        df = self._parse_header_table(self._request_json(provider, params))
        if df.empty:
            parsed = pd.DataFrame(columns=[constants.GAUGE_ID, "ts_id", "provider"])
        else:
            parsed = (
                df.rename(columns={"station_no": constants.GAUGE_ID})[[constants.GAUGE_ID, "ts_id"]]
                .dropna(subset=[constants.GAUGE_ID, "ts_id"])
                .assign(
                    **{
                        constants.GAUGE_ID: lambda frame: frame[constants.GAUGE_ID].astype(str).str.strip(),
                        "ts_id": lambda frame: frame["ts_id"].astype(str).str.strip(),
                        "provider": provider,
                    }
                )
                .drop_duplicates()
            )

        self._timeseries_map_cache[cache_key] = parsed
        return parsed.copy()

    def _get_virtual_station_ids(self, provider: str) -> set[str]:
        config = self.PROVIDERS[provider]
        virtual_group_id = config.get("virtual_group_id")
        if not virtual_group_id:
            return set()

        params = {
            "service": "kisters",
            "type": "queryServices",
            "request": "getTimeseriesList",
            "timeseriesgroup_id": virtual_group_id,
            "datasource": config["datasource"],
            "format": "json",
            "returnfields": "station_no,ts_id",
        }
        df = self._parse_header_table(self._request_json(provider, params))
        if df.empty or "station_no" not in df.columns:
            return set()
        return set(df["station_no"].dropna().astype(str).str.strip())

    def _get_provider_metadata(self, provider: str) -> pd.DataFrame:
        station_df = self._get_station_list(provider)
        if station_df.empty:
            return self._empty_metadata_frame()

        supported_station_ids = set()
        for variable in self.get_available_variables():
            supported_station_ids.update(self._get_timeseries_map(provider, variable)[constants.GAUGE_ID].tolist())

        if not supported_station_ids:
            return self._empty_metadata_frame()

        virtual_station_ids = self._get_virtual_station_ids(provider)
        station_df = station_df.rename(columns={"station_no": constants.GAUGE_ID})
        station_df[constants.GAUGE_ID] = station_df[constants.GAUGE_ID].astype(str).str.strip()
        station_df = station_df[station_df[constants.GAUGE_ID].isin(supported_station_ids)]
        if virtual_station_ids:
            station_df = station_df[~station_df[constants.GAUGE_ID].isin(virtual_station_ids)]

        config = self.PROVIDERS[provider]
        records = []
        for _, row in station_df.iterrows():
            station_name, river_from_name = self._split_station_name(row.get("station_name"))
            river = self._normalize_string(row.get("river_name")) or river_from_name
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
                    constants.SOURCE: config["source"],
                    "provider": provider,
                    "vertical_datum": self._normalize_string(row.get("station_gauge_datum_postfix")),
                }
            )

        if not records:
            return self._empty_metadata_frame()

        df = pd.DataFrame(records)
        df = df.dropna(subset=[constants.LATITUDE, constants.LONGITUDE])
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return df.set_index(constants.GAUGE_ID)

    def get_metadata(self) -> pd.DataFrame:
        """Fetches live metadata for HIC and VMM stations in Flanders."""
        frames = [self._get_provider_metadata(provider) for provider in self.PROVIDERS]
        frames = [frame.reset_index() for frame in frames if not frame.empty]
        if not frames:
            return self._empty_metadata_frame()

        metadata = pd.concat(frames, ignore_index=True)
        metadata = metadata.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return metadata.set_index(constants.GAUGE_ID)

    def _get_station_timeseries_entries(self, gauge_id: str, variable: str) -> pd.DataFrame:
        frames = []
        for provider in self.PROVIDERS:
            ts_map = self._get_timeseries_map(provider, variable)
            if not ts_map.empty:
                frames.append(ts_map)

        if not frames:
            return pd.DataFrame(columns=[constants.GAUGE_ID, "ts_id", "provider"])

        station_map = pd.concat(frames, ignore_index=True)
        return station_map[station_map[constants.GAUGE_ID] == str(gauge_id)].copy()

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> list[pd.DataFrame]:
        station_entries = self._get_station_timeseries_entries(gauge_id, variable)
        if station_entries.empty:
            return []

        start_ts = f"{start_date}T00:00:00Z"
        end_ts = f"{end_date}T23:59:59Z"
        payloads: list[pd.DataFrame] = []

        for _, entry in station_entries.iterrows():
            provider = entry["provider"]
            datasource = self.PROVIDERS[provider]["datasource"]
            params = {
                "service": "kisters",
                "type": "queryServices",
                "request": "getTimeseriesValues",
                "format": "json",
                "datasource": datasource,
                "ts_id": entry["ts_id"],
                "from": start_ts,
                "to": end_ts,
                "returnfields": "Timestamp,Value,Quality Code,Quality Code Name,Quality Code Description",
            }
            payload = self._parse_values_payload(self._request_json(provider, params))
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
        """Fetches and parses time series data for a specific Flemish gauge and variable."""
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
