"""Fetcher for Ireland river gauge data from the OPW waterlevel.ie service.

Data source:
    - website: https://waterlevel.ie/
    - station metadata endpoint: https://waterlevel.ie/hydro-data/data/internet/stations/stations.json

Supported variables:
    - ``constants.DISCHARGE_DAILY_MIN`` (m³/s)
    - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
    - ``constants.DISCHARGE_DAILY_MAX`` (m³/s)
    - ``constants.STAGE_DAILY_MIN`` (m)
    - ``constants.STAGE_DAILY_MEAN`` (m)
    - ``constants.STAGE_DAILY_MAX`` (m)
    - ``constants.WATER_TEMPERATURE_DAILY_MIN`` (°C)
    - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)
    - ``constants.WATER_TEMPERATURE_DAILY_MAX`` (°C)

Data description and API:
    - see https://waterlevel.ie/

Terms of use:
    - see https://waterlevel.ie/

Notes:
    - Gauge IDs in RivRetrieve use the stripped OPW ``station_no`` identifier.
      The fetcher accepts either stripped or zero-padded IDs and pads internally
      when building archive URLs.
    - Metadata is filtered to station IDs in the republication-safe range ``1..41000``.
"""

import logging
import re
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class IrelandOPWFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Ireland's Office of Public Works."""

    BASE_URL = "https://waterlevel.ie"
    METADATA_URL = f"{BASE_URL}/hydro-data/data/internet/stations/stations.json"
    SOURCE = "Office of Public Works (OPW) Ireland"
    COUNTRY = "Ireland"
    VALID_ID_MIN = 1
    VALID_ID_MAX = 41000
    VARIABLE_MAP = {
        constants.STAGE_DAILY_MIN: {
            "parameter_code": "S",
            "preferred_shortname": "WEB.Day.Min",
        },
        constants.STAGE_DAILY_MEAN: {
            "parameter_code": "S",
            "preferred_shortname": "WEB.Day.Mean",
        },
        constants.STAGE_DAILY_MAX: {
            "parameter_code": "S",
            "preferred_shortname": "WEB.Day.Max",
        },
        constants.DISCHARGE_DAILY_MIN: {
            "parameter_code": "Q",
            "preferred_shortname": "WEB.Day.Min",
        },
        constants.DISCHARGE_DAILY_MEAN: {
            "parameter_code": "Q",
            "preferred_shortname": "WEB.Day.Mean",
        },
        constants.DISCHARGE_DAILY_MAX: {
            "parameter_code": "Q",
            "preferred_shortname": "WEB.Day.Max",
        },
        constants.WATER_TEMPERATURE_DAILY_MIN: {
            "parameter_code": "TWater",
            "preferred_shortname": "WEB.Day.Min-Water-Temp",
        },
        constants.WATER_TEMPERATURE_DAILY_MEAN: {
            "parameter_code": "TWater",
            "preferred_shortname": "WEB.Day.Mean-Water-Temp",
        },
        constants.WATER_TEMPERATURE_DAILY_MAX: {
            "parameter_code": "TWater",
            "preferred_shortname": "WEB.Day.Max-Water-Temp",
        },
    }

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Ireland-OPW gauge metadata."""
        return utils.load_cached_metadata_csv("ireland_opw")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(IrelandOPWFetcher.VARIABLE_MAP.keys())

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

    @classmethod
    def _is_republishable_station_id(cls, gauge_id: str) -> bool:
        try:
            gauge_id_int = int(str(gauge_id).strip())
        except (TypeError, ValueError):
            return False
        return cls.VALID_ID_MIN <= gauge_id_int <= cls.VALID_ID_MAX

    @staticmethod
    def _normalize_gauge_id(gauge_id: Any, pad: bool = False) -> str:
        text = str(gauge_id).strip()
        if not text:
            return text

        try:
            stripped = str(int(text))
        except ValueError:
            stripped = text.lstrip("0") or "0"

        return stripped.zfill(5) if pad else stripped

    @staticmethod
    def _parse_area_km2(value: Any) -> float:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return np.nan

        match = re.search(r"[-+]?\d+(?:[.,]\d+)?", str(value))
        if not match:
            return np.nan
        return float(match.group(0).replace(",", "."))

    @staticmethod
    def _build_timeseries_url(gauge_id: str, parameter_code: str) -> str:
        padded_id = IrelandOPWFetcher._normalize_gauge_id(gauge_id, pad=True)
        return (
            f"{IrelandOPWFetcher.BASE_URL}/hydro-data/data/internet/stations/0/"
            f"{padded_id}/{parameter_code}/year.json"
        )

    @staticmethod
    def _select_series_payload(raw_data: Any, preferred_shortname: str) -> Optional[dict[str, Any]]:
        if isinstance(raw_data, dict):
            candidates = [raw_data]
        elif isinstance(raw_data, list):
            candidates = [item for item in raw_data if isinstance(item, dict) and "data" in item]
        else:
            return None

        if not candidates:
            return None

        for candidate in candidates:
            if str(candidate.get("ts_shortname", "")).strip() == preferred_shortname:
                return candidate

        for candidate in candidates:
            if "mean" in str(candidate.get("ts_shortname", "")).lower():
                return candidate

        return candidates[0]

    @staticmethod
    def _extract_series_frame(series_payload: dict[str, Any], variable: str) -> pd.DataFrame:
        rows = series_payload.get("data") or []
        if not rows:
            return IrelandOPWFetcher._empty_data_frame(variable)

        columns_raw = series_payload.get("columns", "Timestamp,Value")
        columns = [column.strip() for column in str(columns_raw).split(",")]
        timestamp_idx = columns.index("Timestamp") if "Timestamp" in columns else 0
        value_idx = columns.index("Value") if "Value" in columns else 1

        frame = pd.DataFrame(rows)
        if frame.empty or frame.shape[1] <= max(timestamp_idx, value_idx):
            return IrelandOPWFetcher._empty_data_frame(variable)

        timestamps = pd.to_datetime(frame.iloc[:, timestamp_idx], utc=True, errors="coerce").dt.tz_localize(None)
        values = pd.to_numeric(frame.iloc[:, value_idx], errors="coerce")

        parsed = pd.DataFrame({constants.TIME_INDEX: timestamps, variable: values}).dropna(
            subset=[constants.TIME_INDEX, variable]
        )
        if parsed.empty:
            return IrelandOPWFetcher._empty_data_frame(variable)

        parsed[constants.TIME_INDEX] = parsed[constants.TIME_INDEX].dt.floor("D")
        parsed = parsed.groupby(constants.TIME_INDEX, as_index=False)[variable].mean()
        return parsed.set_index(constants.TIME_INDEX).sort_index()

    def get_metadata(self) -> pd.DataFrame:
        """Fetches live station metadata from waterlevel.ie."""
        session = utils.requests_retry_session(retries=6, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))

        try:
            response = session.get(self.METADATA_URL, timeout=60)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Ireland-OPW metadata: {exc}")
            raise
        except ValueError as exc:
            logger.error(f"Failed to decode Ireland-OPW metadata: {exc}")
            raise

        if not isinstance(payload, list) or not payload:
            return self._empty_metadata_frame()

        records = []
        for item in payload:
            if not isinstance(item, dict):
                continue

            gauge_id = self._normalize_gauge_id(item.get("station_no", ""))
            if not self._is_republishable_station_id(gauge_id):
                continue

            river = item.get("WTO_OBJECT") or item.get("catchment_name")
            records.append(
                {
                    constants.GAUGE_ID: gauge_id,
                    constants.STATION_NAME: item.get("station_name"),
                    constants.RIVER: river,
                    constants.LATITUDE: pd.to_numeric(item.get("station_latitude"), errors="coerce"),
                    constants.LONGITUDE: pd.to_numeric(item.get("station_longitude"), errors="coerce"),
                    constants.ALTITUDE: pd.to_numeric(item.get("station_gauge_datum"), errors="coerce"),
                    constants.AREA: self._parse_area_km2(item.get("CATCHMENT_SIZE")),
                    constants.COUNTRY: self.COUNTRY,
                    constants.SOURCE: self.SOURCE,
                    "vertical_datum": item.get("station_gauge_datum_unit"),
                }
            )

        if not records:
            return self._empty_metadata_frame()

        df = pd.DataFrame(records)
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return df.set_index(constants.GAUGE_ID)

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> Any:
        del start_date, end_date

        config = self.VARIABLE_MAP[variable]
        url = self._build_timeseries_url(gauge_id, config["parameter_code"])
        session = utils.requests_retry_session(retries=6, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))

        response = session.get(url, timeout=60)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json()

    def _parse_data(self, gauge_id: str, raw_data: Any, variable: str) -> pd.DataFrame:
        del gauge_id

        series_payload = self._select_series_payload(raw_data, self.VARIABLE_MAP[variable]["preferred_shortname"])
        if not series_payload:
            return self._empty_data_frame(variable)
        return self._extract_series_frame(series_payload, variable)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific OPW gauge and variable."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        normalized_gauge_id = self._normalize_gauge_id(gauge_id)
        if not self._is_republishable_station_id(normalized_gauge_id):
            logger.warning(f"Gauge ID {gauge_id} is outside the supported Ireland-OPW republication range.")
            return self._empty_data_frame(variable)

        try:
            raw_data = self._download_data(normalized_gauge_id, variable, start_date, end_date)
            df = self._parse_data(normalized_gauge_id, raw_data, variable)
        except Exception as exc:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {exc}")
            return self._empty_data_frame(variable)

        if df.empty:
            return df

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        return df[(df.index >= start_dt) & (df.index <= end_dt)]
