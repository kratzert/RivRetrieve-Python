"""Fetcher for Thailand river gauge data from the ThaiWater public API."""

import logging
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class ThailandFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the ThaiWater public API.

    Data source:
        - official ThaiWater website: https://www.thaiwater.net/
        - official/public ThaiWater standards portal: https://standard.thaiwater.net/

    Supported variables:
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.STAGE_INSTANT`` (m)
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)

    Data description and API:
        - official/public station metadata endpoint:
          https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_load
        - official/public time-series endpoint:
          https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_graph
        - example graph request:
          https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_graph?station_type=tele_waterlevel&station_id=1&start_date=2025-01-01&end_date=2025-01-03
        - official ThaiWater telemetry station project background:
          https://www.hii.or.th/en/research-development/project-highlights/2024/02/08/2021-automated-telemetry-station-enhancement-project-to-support-national-water-management/
        - official government catalog entry:
          https://gdcatalog.go.th/en/dataset/gdpublish-water-level

    Terms of use:
        - see https://www.thaiwater.net/
        - see https://standard.thaiwater.net/
        - use the public ThaiWater endpoints according to provider terms and availability

    Notes:
        - This v1 implementation uses the broad ``waterlevel_load`` inventory and
          ``waterlevel_graph`` time-series endpoints.
        - The narrower ``flow`` endpoints are intentionally not used in v1.
        - Long ranges are requested in smaller windows because the upstream
          ``waterlevel_graph`` endpoint currently truncates oversized requests to
          about the most recent year.
        - Stage values are parsed from ``waterlevel_graph.data.graph_data[].value``.
        - The live metadata currently populates ``waterlevel_msl`` while
          ``waterlevel_m`` appears to be mostly null.
        - This fetcher therefore interprets graph ``value`` as stage relative to
          mean sea level (MSL). This is an inference from current source payloads
          and should be revisited if ThaiWater changes the API semantics.
        - Discharge is only available for a subset of stations. Station-variable
          combinations without discharge data return an empty DataFrame.
    """

    BASE_URL = "https://api-v3.thaiwater.net/api/v1/thaiwater30/public"
    METADATA_URL = f"{BASE_URL}/waterlevel_load"
    GRAPH_URL = f"{BASE_URL}/waterlevel_graph"
    SOURCE = "ThaiWater public API / Hydro-Informatics Institute (HII)"
    COUNTRY = "Thailand"
    LOCAL_TIMEZONE = "Asia/Bangkok"
    VERTICAL_DATUM = "MSL"
    MAX_WINDOW_DAYS = 365
    VARIABLE_MAP = {
        constants.STAGE_DAILY_MEAN: {
            "field": "value",
            "aggregate_daily": True,
        },
        constants.STAGE_INSTANT: {
            "field": "value",
            "aggregate_daily": False,
        },
        constants.DISCHARGE_DAILY_MEAN: {
            "field": "discharge",
            "aggregate_daily": True,
        },
        constants.DISCHARGE_INSTANT: {
            "field": "discharge",
            "aggregate_daily": False,
        },
    }

    def __init__(self):
        self._metadata_cache: Optional[pd.DataFrame] = None

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Thailand gauge metadata."""
        return utils.load_cached_metadata_csv("thailand")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(ThailandFetcher.VARIABLE_MAP.keys())

    @classmethod
    def _split_windows(cls, start_date: str, end_date: str) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        if pd.isna(start) or pd.isna(end) or start > end:
            return []

        windows = []
        current = start
        while current <= end:
            window_end = min(end, current + pd.Timedelta(days=cls.MAX_WINDOW_DAYS - 1))
            windows.append((current, window_end))
            current = window_end + pd.Timedelta(days=1)
        return windows

    @staticmethod
    def _empty_data_frame(variable: str) -> pd.DataFrame:
        return pd.DataFrame(columns=[constants.TIME_INDEX, variable]).set_index(constants.TIME_INDEX)

    @staticmethod
    def _empty_metadata_frame() -> pd.DataFrame:
        columns = [
            constants.GAUGE_ID,
            constants.STATION_NAME,
            "station_name_local",
            constants.RIVER,
            constants.LATITUDE,
            constants.LONGITUDE,
            constants.ALTITUDE,
            constants.AREA,
            constants.COUNTRY,
            constants.SOURCE,
            "station_code",
            "station_type",
            "agency",
            "basin",
            "province",
            "district",
            "subdistrict",
            "vertical_datum",
        ]
        return pd.DataFrame(columns=columns).set_index(constants.GAUGE_ID)

    def _request_json(self, url: str, params: Optional[dict[str, Any]] = None) -> Any:
        session = utils.requests_retry_session(retries=6, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _clean_text(value: Any) -> Optional[str]:
        if value is None:
            return None

        text = str(value).strip()
        if not text or text.lower() == "nan":
            return None
        return text

    @classmethod
    def _pick_localized_text(cls, value: Any, preferred_languages: tuple[str, ...] = ("en", "th")) -> Optional[str]:
        if isinstance(value, dict):
            for language in preferred_languages:
                text = cls._clean_text(value.get(language))
                if text:
                    return text

            for text in value.values():
                cleaned = cls._clean_text(text)
                if cleaned:
                    return cleaned

            return None

        return cls._clean_text(value)

    @classmethod
    def _parse_metadata_payload(cls, payload: Any) -> pd.DataFrame:
        rows = payload.get("waterlevel_data", {}).get("data", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list) or not rows:
            return cls._empty_metadata_frame()

        records = []
        for row in rows:
            if not isinstance(row, dict):
                continue

            station = row.get("station", {}) if isinstance(row.get("station"), dict) else {}
            geocode = row.get("geocode", {}) if isinstance(row.get("geocode"), dict) else {}
            basin = row.get("basin", {}) if isinstance(row.get("basin"), dict) else {}
            agency = row.get("agency", {}) if isinstance(row.get("agency"), dict) else {}

            gauge_id = cls._clean_text(station.get("id"))
            station_type = cls._clean_text(row.get("station_type") or station.get("tele_station_type"))
            if gauge_id is None or station_type != "tele_waterlevel":
                continue

            station_name = cls._pick_localized_text(station.get("tele_station_name"))
            station_name_local = cls._pick_localized_text(station.get("tele_station_name"), preferred_languages=("th", "en"))
            records.append(
                {
                    constants.GAUGE_ID: gauge_id,
                    constants.STATION_NAME: station_name or station_name_local,
                    "station_name_local": station_name_local,
                    constants.RIVER: cls._clean_text(row.get("river_name")),
                    constants.LATITUDE: pd.to_numeric(station.get("tele_station_lat"), errors="coerce"),
                    constants.LONGITUDE: pd.to_numeric(station.get("tele_station_long"), errors="coerce"),
                    constants.ALTITUDE: np.nan,
                    constants.AREA: np.nan,
                    constants.COUNTRY: cls.COUNTRY,
                    constants.SOURCE: cls.SOURCE,
                    "station_code": cls._clean_text(station.get("tele_station_oldcode")),
                    "station_type": station_type,
                    "agency": cls._pick_localized_text(agency.get("agency_name")),
                    "basin": cls._pick_localized_text(basin.get("basin_name")),
                    "province": cls._pick_localized_text(geocode.get("province_name")),
                    "district": cls._pick_localized_text(geocode.get("amphoe_name")),
                    "subdistrict": cls._pick_localized_text(geocode.get("tumbon_name")),
                    "vertical_datum": cls.VERTICAL_DATUM,
                }
            )

        if not records:
            return cls._empty_metadata_frame()

        df = pd.DataFrame(records)
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return df.set_index(constants.GAUGE_ID)

    def get_metadata(self) -> pd.DataFrame:
        """Fetches live metadata for ThaiWater telemetered water-level stations."""
        if self._metadata_cache is not None:
            return self._metadata_cache.copy()

        try:
            payload = self._request_json(self.METADATA_URL)
            metadata = self._parse_metadata_payload(payload)
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Thailand metadata: {exc}")
            raise
        except ValueError as exc:
            logger.error(f"Failed to decode Thailand metadata: {exc}")
            raise

        self._metadata_cache = metadata.copy()
        return metadata

    @classmethod
    def _parse_graph_payload(cls, payload: Any) -> pd.DataFrame:
        graph_data = payload.get("data", {}).get("graph_data", []) if isinstance(payload, dict) else []
        if not isinstance(graph_data, list) or not graph_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, "value", "discharge"])

        df = pd.DataFrame(graph_data)
        if "datetime" not in df.columns:
            return pd.DataFrame(columns=[constants.TIME_INDEX, "value", "discharge"])

        timestamps = pd.to_datetime(df["datetime"], errors="coerce").dt.tz_localize(cls.LOCAL_TIMEZONE).dt.tz_localize(None)
        parsed = pd.DataFrame(
            {
                constants.TIME_INDEX: timestamps,
                "value": pd.to_numeric(df.get("value"), errors="coerce"),
                "discharge": pd.to_numeric(df.get("discharge"), errors="coerce"),
            }
        )
        return parsed.dropna(subset=[constants.TIME_INDEX]).sort_values(constants.TIME_INDEX)

    @classmethod
    def _parse_graph_payloads(cls, payloads: list[Any]) -> pd.DataFrame:
        frames = [cls._parse_graph_payload(payload) for payload in payloads]
        frames = [frame for frame in frames if not frame.empty]
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame(columns=[constants.TIME_INDEX, "value", "discharge"])

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> list[Any]:
        payloads: list[Any] = []
        for window_start, window_end in self._split_windows(start_date, end_date):
            params = {
                "station_type": "tele_waterlevel",
                "station_id": str(gauge_id),
                "start_date": window_start.strftime("%Y-%m-%d"),
                "end_date": window_end.strftime("%Y-%m-%d"),
            }
            payloads.append(self._request_json(self.GRAPH_URL, params=params))
        return payloads

    def _parse_data(self, gauge_id: str, raw_data: list[Any], variable: str) -> pd.DataFrame:
        if not raw_data:
            return self._empty_data_frame(variable)

        config = self.VARIABLE_MAP[variable]
        df = self._parse_graph_payloads(raw_data)
        if df.empty or config["field"] not in df.columns:
            return self._empty_data_frame(variable)

        parsed = (
            df[[constants.TIME_INDEX, config["field"]]]
            .rename(columns={config["field"]: variable})
            .dropna(subset=[variable])
            .sort_values(constants.TIME_INDEX)
        )
        if parsed.empty:
            return self._empty_data_frame(variable)

        if config["aggregate_daily"]:
            parsed[constants.TIME_INDEX] = parsed[constants.TIME_INDEX].dt.floor("D")
            parsed = parsed.groupby(constants.TIME_INDEX, as_index=False)[variable].mean()
        else:
            parsed = parsed.drop_duplicates(subset=[constants.TIME_INDEX], keep="last")

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
        if "instantaneous" in variable or "hourly" in variable:
            end_dt = end_dt + pd.Timedelta(days=1)
            return df[(df.index >= start_dt) & (df.index < end_dt)]

        return df[(df.index >= start_dt) & (df.index <= end_dt)]
