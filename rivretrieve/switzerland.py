"""Fetcher for Swiss river gauge data from the BAFU hydro API."""

import logging
from io import StringIO
from typing import Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class SwitzerlandFetcher(base.RiverDataFetcher):
    """Fetches Swiss river gauge data from the Existenz.ch BAFU hydro API.

    Data source:
        https://api.existenz.ch/#hydro

    Supported variables:
        - constants.DISCHARGE_DAILY_MEAN (m³/s)
        - constants.DISCHARGE_INSTANT (m³/s)
        - constants.STAGE_DAILY_MEAN (m)
        - constants.STAGE_INSTANT (m)
        - constants.WATER_TEMPERATURE_DAILY_MEAN (°C)
        - constants.WATER_TEMPERATURE_INSTANT (°C)

    Data description and API:
        - see https://api.existenz.ch/docs/apiv1/

    Terms of use:
        - see https://api.existenz.ch
    """

    BASE_URL = "https://api.existenz.ch/apiv1"
    METADATA_URL = f"{BASE_URL}/hydro/locations"
    INFLUX_URL = "https://influx.konzept.space/api/v2/query?org=api.existenz.ch"
    INFLUX_TOKEN = "0yLbh-D7RMe1sX1iIudFel8CcqCI8sVfuRTaliUp56MgE6kub8-nSd05_EJ4zTTKt0lUzw8zcO73zL9QhC3jtA=="
    SOURCE = "Swiss Federal Office for the Environment FOEN / BAFU"
    COUNTRY = "Switzerland"
    MAX_WINDOW_DAYS = 366
    VARIABLE_MAP = {
        constants.DISCHARGE_DAILY_MEAN: {
            "parameters": ("flow", "flow_ls"),
            "preferred": "flow",
            "fallback": "flow_ls",
            "aggregate_daily": True,
        },
        constants.DISCHARGE_INSTANT: {
            "parameters": ("flow", "flow_ls"),
            "preferred": "flow",
            "fallback": "flow_ls",
            "aggregate_daily": False,
        },
        constants.STAGE_DAILY_MEAN: {
            "parameters": ("height_abs", "height"),
            "preferred": "height_abs",
            "fallback": "height",
            "aggregate_daily": True,
        },
        constants.STAGE_INSTANT: {
            "parameters": ("height_abs", "height"),
            "preferred": "height_abs",
            "fallback": "height",
            "aggregate_daily": False,
        },
        constants.WATER_TEMPERATURE_DAILY_MEAN: {
            "parameters": ("temperature",),
            "preferred": "temperature",
            "fallback": None,
            "aggregate_daily": True,
        },
        constants.WATER_TEMPERATURE_INSTANT: {
            "parameters": ("temperature",),
            "preferred": "temperature",
            "fallback": None,
            "aggregate_daily": False,
        },
    }

    @staticmethod
    def _empty_result(variable: str) -> pd.DataFrame:
        """Returns a standardized empty time series result."""
        return pd.DataFrame(columns=[variable], index=pd.DatetimeIndex([], name=constants.TIME_INDEX))

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Swiss gauge metadata."""
        return utils.load_cached_metadata_csv("switzerland")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(SwitzerlandFetcher.VARIABLE_MAP.keys())

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

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata for all stations from the hydro locations endpoint.

        Maps the provider response to the standard RivRetrieve metadata columns and
        returns a DataFrame indexed by ``constants.GAUGE_ID``.
        """
        session = utils.requests_retry_session()

        try:
            response = session.get(self.METADATA_URL, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Swiss metadata: {exc}")
            raise
        except ValueError as exc:
            logger.error(f"Failed to decode Swiss metadata: {exc}")
            raise

        payload = data.get("payload", data) if isinstance(data, dict) else data
        if not isinstance(payload, dict) or not payload:
            return self._empty_metadata_frame()

        rows = []
        for station_key, station in payload.items():
            details = station.get("details", {}) if isinstance(station, dict) else {}
            river = details.get("water-body-name") or details.get("water_body_name")
            rows.append(
                {
                    constants.GAUGE_ID: str(details.get("id") or station_key).strip(),
                    constants.STATION_NAME: details.get("name"),
                    constants.RIVER: river,
                    constants.LATITUDE: pd.to_numeric(details.get("lat"), errors="coerce"),
                    constants.LONGITUDE: pd.to_numeric(details.get("lon"), errors="coerce"),
                    constants.ALTITUDE: np.nan,
                    constants.AREA: np.nan,
                    constants.COUNTRY: self.COUNTRY,
                    constants.SOURCE: self.SOURCE,
                }
            )

        if not rows:
            return self._empty_metadata_frame()

        df = pd.DataFrame(rows)
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        return df.set_index(constants.GAUGE_ID).sort_index()

    @classmethod
    def _build_flux_query(
        cls, gauge_id: str, parameter_names: tuple[str, ...], start: pd.Timestamp, end: pd.Timestamp
    ) -> str:
        fields_expr = " or ".join([f'r["_field"] == "{name}"' for name in parameter_names])
        start_str = start.strftime("%Y-%m-%dT00:00:00Z")
        stop_str = (end + pd.Timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        return (
            'from(bucket: "existenzApi")'
            f" |> range(start: {start_str}, stop: {stop_str})"
            ' |> filter(fn: (r) => r["_measurement"] == "hydro")'
            f' |> filter(fn: (r) => r["loc"] == "{gauge_id}")'
            f" |> filter(fn: (r) => {fields_expr})"
        )

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> list[str]:
        """Downloads raw CSV responses from the official InfluxDB archive."""
        config = self.VARIABLE_MAP[variable]
        windows = self._split_windows(start_date, end_date)
        if not windows:
            return []

        session = utils.requests_retry_session()
        payloads: list[str] = []
        headers = {
            "Authorization": f"Token {self.INFLUX_TOKEN}",
            "Accept": "application/csv",
            "Content-type": "application/vnd.flux",
        }

        for window_start, window_end in windows:
            query = self._build_flux_query(gauge_id, config["parameters"], window_start, window_end)

            try:
                response = session.post(self.INFLUX_URL, headers=headers, data=query, timeout=60)
                response.raise_for_status()
            except requests.exceptions.RequestException as exc:
                logger.error(f"Failed to fetch Swiss data for {gauge_id}: {exc}")
                raise

            if response.text.strip():
                payloads.append(response.text)

        return payloads

    @staticmethod
    def _parse_timeseries_payload(payload: str) -> pd.DataFrame:
        if not payload.strip():
            return pd.DataFrame(columns=["station_id", "parameter_code", constants.TIME_INDEX, "value"])

        df = pd.read_csv(StringIO(payload), comment="#")
        if df.empty:
            return pd.DataFrame(columns=["station_id", "parameter_code", constants.TIME_INDEX, "value"])

        required_columns = {"_time", "_value", "_field", "loc"}
        if not required_columns.issubset(df.columns):
            return pd.DataFrame(columns=["station_id", "parameter_code", constants.TIME_INDEX, "value"])

        result = pd.DataFrame(
            {
                "station_id": df["loc"].astype(str),
                "parameter_code": df["_field"].astype(str),
                constants.TIME_INDEX: pd.to_datetime(df["_time"], utc=True, errors="coerce").dt.tz_localize(None),
                "value": pd.to_numeric(df["_value"], errors="coerce"),
            }
        )
        return result.dropna(subset=[constants.TIME_INDEX, "value"])

    @classmethod
    def _parse_timeseries_payloads(cls, payloads: list[str]) -> pd.DataFrame:
        frames = [cls._parse_timeseries_payload(payload) for payload in payloads]
        frames = [frame for frame in frames if not frame.empty]
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame(columns=["station_id", "parameter_code", constants.TIME_INDEX, "value"])

    @staticmethod
    def _apply_parameter_preference(df: pd.DataFrame, preferred: str, fallback: Optional[str]) -> pd.DataFrame:
        if df.empty or fallback is None:
            return df[df["parameter_code"] == preferred]

        rank_map = {preferred: 0, fallback: 1}
        result = df.copy()
        result["_rank"] = result["parameter_code"].map(rank_map).fillna(99)
        result = result.sort_values([constants.TIME_INDEX, "_rank"])
        result = result.drop_duplicates(subset=[constants.TIME_INDEX], keep="first")
        return result.drop(columns="_rank")

    @staticmethod
    def _convert_units(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["value"] = pd.to_numeric(result["value"], errors="coerce").astype(float)
        flow_ls_mask = result["parameter_code"] == "flow_ls"
        result.loc[flow_ls_mask, "value"] = result.loc[flow_ls_mask, "value"] / 1000.0
        return result

    def _parse_data(self, gauge_id: str, raw_data: list[str], variable: str) -> pd.DataFrame:
        """Parses raw Swiss API payloads into the standard RivRetrieve layout."""
        if not raw_data:
            return self._empty_result(variable)

        config = self.VARIABLE_MAP[variable]
        df = self._parse_timeseries_payloads(raw_data)
        if df.empty:
            return self._empty_result(variable)
        df = df[df["station_id"] == str(gauge_id)]
        df = df[df["parameter_code"].isin(config["parameters"])]
        df = df.dropna(subset=[constants.TIME_INDEX, "value"])
        if df.empty:
            return self._empty_result(variable)

        df = self._apply_parameter_preference(df, config["preferred"], config["fallback"])
        df = self._convert_units(df)
        df = df.drop_duplicates(subset=[constants.TIME_INDEX], keep="first").sort_values(constants.TIME_INDEX)

        if config["aggregate_daily"]:
            df[constants.TIME_INDEX] = df[constants.TIME_INDEX].dt.floor("D")
            df = df.groupby(constants.TIME_INDEX, as_index=False)["value"].mean()
        else:
            df = df[[constants.TIME_INDEX, "value"]]

        df = df.rename(columns={"value": variable}).dropna(subset=[variable])
        return df.set_index(constants.TIME_INDEX).sort_index()

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable.

        This method retrieves the requested data from the provider's API or archive,
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
        if "instantaneous" in variable or "hourly" in variable:
            end_dt = end_dt + pd.Timedelta(days=1)
            return df[(df.index >= start_dt) & (df.index < end_dt)]

        return df[(df.index >= start_dt) & (df.index <= end_dt)]
