"""Fetcher for South Korea (WAMIS) river gauge data."""

import logging
from typing import Optional

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from . import base, constants, utils

logger = logging.getLogger(__name__)


class SouthKoreaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from South Korea's WAMIS Open API.

    Data Source:
        - Han River Flood Control Office (WAMIS Open API, http://www.wamis.go.kr)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.STAGE_HOURLY_MEAN`` (m)

    Data and API description:
        - see WAMIS Open API: http://wamis.go.kr:8080/wamisweb/flw/w15.do

    Terms of use:
        - see https://www.hrfco.go.kr/web/openapi/policy.do
    """

    # --- API endpoints ---
    URL_DISCHARGE = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/flw_dtdata"
    URL_STAGE_DAILY = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_dtdata"
    URL_STAGE_HOURLY = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_hrdata"
    URL_STATION_LIST = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_dubwlobs"
    URL_STATION_INFO = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_obsinfo"

    # --- Public API methods ---
    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached metadata for South Korea (if available)."""
        return utils.load_cached_metadata_csv("korea")

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and parses site metadata from WAMIS."""
        try:
            # Step 1: fetch list of station IDs
            resp = requests.get(self.URL_STATION_LIST, params={"output": "json"}, timeout=30)
            resp.raise_for_status()
            stations = resp.json().get("list", [])
            if not stations:
                logger.warning("No stations found in WAMIS wl_dubwlobs.")
                return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)
            station_ids = [s["obscd"] for s in stations if "obscd" in s]
        except Exception as e:
            logger.error(f"Failed to fetch WAMIS station list: {e}")
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

        df_all = pd.DataFrame()
        for sid in tqdm(station_ids, desc="Fetching WAMIS metadata"):
            try:
                r = requests.get(self.URL_STATION_INFO, params={"obscd": sid, "output": "json"}, timeout=10)
                data = r.json()
                if data.get("result", {}).get("code") == "success" and "list" in data:
                    df = pd.json_normalize(data["list"])
                    df_all = pd.concat([df_all, df], ignore_index=True)
            except Exception:
                continue

        if df_all.empty:
            logger.warning("No metadata records retrieved from WAMIS.")
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

        # --- Rename + convert ---
        df_all = df_all.rename(
            columns={
                "wlobscd": constants.GAUGE_ID,
                "obsnmeng": constants.STATION_NAME,
                "rivnm": constants.RIVER,
                "gdt": constants.ALTITUDE,
                "bsnara": constants.AREA,
                "lon": "longitude_dms",
                "lat": "latitude_dms",
            }
        )

        def dms_to_decimal(dms: str) -> float:
            if not isinstance(dms, str) or not dms.strip():
                return np.nan
            try:
                d, m, s = map(float, dms.split("-"))
                return d + m / 60 + s / 3600
            except Exception:
                return np.nan

        df_all[constants.LONGITUDE] = df_all["longitude_dms"].apply(dms_to_decimal)
        df_all[constants.LATITUDE] = df_all["latitude_dms"].apply(dms_to_decimal)
        df_all[constants.ALTITUDE] = pd.to_numeric(df_all[constants.ALTITUDE], errors="coerce")
        df_all[constants.AREA] = pd.to_numeric(df_all[constants.AREA], errors="coerce")
        df_all[constants.COUNTRY] = "South Korea"
        df_all[constants.SOURCE] = "WAMIS Open API"

        keep = [
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

        df_final = df_all[keep].dropna(subset=[constants.GAUGE_ID]).drop_duplicates(subset=[constants.GAUGE_ID])
        return df_final.set_index(constants.GAUGE_ID)

    # --- Variable support ---
    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.DISCHARGE_DAILY_MEAN,
            constants.STAGE_DAILY_MEAN,
            constants.STAGE_HOURLY_MEAN,
        )

    # --- Internal helpers ---
    def _get_endpoint(self, variable: str) -> tuple[str, str, str]:
        """Map variable to endpoint and JSON field names."""
        if variable == constants.DISCHARGE_DAILY_MEAN:
            return self.URL_DISCHARGE, "ymd", "fw"
        elif variable == constants.STAGE_DAILY_MEAN:
            return self.URL_STAGE_DAILY, "ymd", "wl"
        elif variable == constants.STAGE_HOURLY_MEAN:
            return self.URL_STAGE_HOURLY, "ymdh", "wl"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Downloads raw WAMIS data year by year (handles partial years correctly)."""
        s = utils.requests_retry_session()
        all_data = []

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        years = range(start_dt.year, end_dt.year + 1)

        url, date_field, value_field = self._get_endpoint(variable)

        for year in years:
            start_chunk = max(start_dt, pd.Timestamp(year=year, month=1, day=1))
            end_chunk = min(end_dt, pd.Timestamp(year=year, month=12, day=31))

            params = {
                "obscd": gauge_id,
                "startdt": start_chunk.strftime("%Y%m%d"),
                "enddt": end_chunk.strftime("%Y%m%d"),
                "output": "json",
            }

            try:
                r = s.get(url, params=params, timeout=30)
                r.raise_for_status()
                js = r.json()

                if not isinstance(js, dict) or "list" not in js:
                    continue

                df = pd.DataFrame(js["list"])
                if df.empty or date_field not in df.columns or value_field not in df.columns:
                    continue

                df = df.rename(columns={date_field: "time", value_field: variable})
                df["time"] = pd.to_datetime(
                    df["time"],
                    format="%Y%m%d%H" if variable == constants.STAGE_HOURLY_MEAN else "%Y%m%d",
                    errors="coerce",
                )
                df[variable] = pd.to_numeric(df[variable], errors="coerce")
                df.loc[df[variable] <= -777, variable] = np.nan
                all_data.append(df)

            except Exception as e:
                logger.warning(f"Failed {variable} fetch for {gauge_id} ({year}): {e}")
                continue

        if not all_data:
            return pd.DataFrame(columns=["time", variable])

        df_all = pd.concat(all_data, ignore_index=True)
        df_all = df_all.dropna(subset=["time", variable])
        df_all = df_all[(df_all["time"] >= start_dt) & (df_all["time"] <= end_dt)]
        df_all = df_all.drop_duplicates(subset="time", keep="first")
        df_all = df_all.sort_values("time").reset_index(drop=True)
        return df_all

    def _parse_data(self, gauge_id: str, raw_data: pd.DataFrame, variable: str) -> pd.DataFrame:
        """Ensures consistent time index and format."""
        if raw_data.empty:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = raw_data.copy()
        df = df.dropna(subset=["time", variable])
        df = df.sort_values("time")
        df = df.set_index("time")
        df.index = df.index.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT")
        return df[[variable]]

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
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)

            if df.empty:
                logger.debug(f"No {variable} data returned for gauge {gauge_id}.")
                return df

            # Final date filter
            start_dt = pd.to_datetime(start_date).tz_localize("UTC")
            end_dt = pd.to_datetime(end_date).tz_localize("UTC")
            df = df[(df.index >= start_dt) & (df.index <= end_dt)]
            return df

        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
