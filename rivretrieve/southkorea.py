"""Fetcher for South Korea river gauge data from WAMIS (Water Resources Management Information System)."""

import logging
from typing import Optional

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from . import base, constants, utils

logger = logging.getLogger(__name__)


def _dms_to_decimal(dms: str) -> float:
    """Convert DMS format (e.g., '126-59-43') to decimal degrees."""
    if not isinstance(dms, str) or not dms.strip():
        return np.nan
    try:
        d, m, s = map(float, dms.split("-"))
        return d + m / 60 + s / 3600
    except Exception:
        return np.nan


class SouthKoreaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the Korean WAMIS Open API.

    Data source:
        - WAMIS Open API (http://www.wamis.go.kr)

    Supported variables:
        - constants.DISCHARGE_DAILY_MEAN (m³/s)
        - constants.STAGE_DAILY_MEAN (m)
        - constants.STAGE_INSTANT (m)
    """

    # API Endpoints
    BASE_URL_FLOW = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/flw_dtdata"
    BASE_URL_STAGE_DAILY = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_dtdata"
    BASE_URL_STAGE_HOURLY = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_hrdata"
    METADATA_LIST_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_dubwlobs"
    METADATA_DETAIL_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_obsinfo"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached metadata if available."""
        return utils.load_cached_metadata_csv("south_korea")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.DISCHARGE_DAILY_MEAN,
            constants.STAGE_DAILY_MEAN,
            constants.STAGE_INSTANT,
        )

    # -------------------------------------------------------------------------
    # Metadata retrieval
    # -------------------------------------------------------------------------
    def get_metadata(self) -> pd.DataFrame:
        """Fetch metadata for all WAMIS gauging stations."""
        try:
            resp = requests.get(self.METADATA_LIST_URL, params={"output": "json"}, timeout=30)
            resp.raise_for_status()
            stations = resp.json().get("list", [])
            if not stations:
                logger.warning("No stations found in wl_dubwlobs.")
                return pd.DataFrame()
            station_ids = [s["obscd"] for s in stations if "obscd" in s]
        except Exception as e:
            logger.error(f"Failed to fetch station list: {e}")
            return pd.DataFrame()

        df_all = pd.DataFrame()
        for sid in tqdm(station_ids, desc="Fetching WAMIS metadata"):
            try:
                r = requests.get(self.METADATA_DETAIL_URL, params={"obscd": sid, "output": "json"}, timeout=10)
                data = r.json()
                if data.get("result", {}).get("code") == "success" and "list" in data:
                    df = pd.json_normalize(data["list"])
                    df_all = pd.concat([df_all, df], ignore_index=True)
            except Exception:
                continue

        if df_all.empty:
            logger.warning("No metadata records retrieved.")
            return pd.DataFrame()

        # Standardize
        df_all["gauge_id"] = df_all["wlobscd"].astype(str)
        df_all["station_name"] = df_all.get("obsnmeng", df_all.get("obsnm"))
        df_all["river"] = df_all.get("rivnm", None)
        df_all["longitude"] = df_all["lon"].apply(_dms_to_decimal)
        df_all["latitude"] = df_all["lat"].apply(_dms_to_decimal)
        df_all["altitude"] = pd.to_numeric(df_all["gdt"], errors="coerce")
        df_all["area"] = pd.to_numeric(df_all["bsnara"], errors="coerce")
        df_all["country"] = "Korea"
        df_all["source"] = "WAMIS Open API"

        keep_cols = [
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

        df_final = df_all[keep_cols].dropna(subset=[constants.GAUGE_ID])
        df_final = df_final.drop_duplicates(subset=[constants.GAUGE_ID]).reset_index(drop=True)
        return df_final

    # -------------------------------------------------------------------------
    # Data retrieval
    # -------------------------------------------------------------------------
    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Downloads raw WAMIS JSON data (looping per year for reliability)."""
        variable = variable.lower()
        if variable == constants.DISCHARGE_DAILY_MEAN:
            url = self.BASE_URL_FLOW
            value_field, date_field = "fw", "ymd"
        elif variable == constants.STAGE_DAILY_MEAN:
            url = self.BASE_URL_STAGE_DAILY
            value_field, date_field = "wl", "ymd"
        elif variable == constants.STAGE_INSTANT:
            url = self.BASE_URL_STAGE_HOURLY
            value_field, date_field = "wl", "ymdh"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        years = range(start_dt.year, end_dt.year + 1)

        all_data = []

        for year in years:
            if variable == constants.DISCHARGE_DAILY_MEAN:
                params = {"obscd": gauge_id, "year": str(year), "output": "json"}
            else:
                start_chunk = max(start_dt, pd.Timestamp(year=year, month=1, day=1))
                end_chunk = min(end_dt, pd.Timestamp(year=year, month=12, day=31))
                params = {
                    "obscd": gauge_id,
                    "startdt": start_chunk.strftime("%Y%m%d"),
                    "enddt": end_chunk.strftime("%Y%m%d"),
                    "output": "json",
                }

            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict) or "list" not in data:
                    continue
                df = pd.DataFrame(data["list"])
                if df.empty or date_field not in df.columns or value_field not in df.columns:
                    continue
                df = df.rename(columns={date_field: constants.TIME_INDEX, value_field: variable})
                df[constants.TIME_INDEX] = pd.to_datetime(
                    df[constants.TIME_INDEX],
                    format="%Y%m%d%H" if variable == constants.STAGE_INSTANT else "%Y%m%d",
                    errors="coerce",
                )
                df[variable] = pd.to_numeric(df[variable], errors="coerce")
                df.loc[df[variable] <= -777, variable] = np.nan
                all_data.append(df)
            except Exception as e:
                logger.warning(f"Failed fetching {variable} for {gauge_id} ({year}): {e}")
                continue

        if not all_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df_all = pd.concat(all_data, ignore_index=True)
        df_all = df_all.dropna(subset=[constants.TIME_INDEX, variable])
        df_all = df_all[(df_all[constants.TIME_INDEX] >= start_dt) & (df_all[constants.TIME_INDEX] <= end_dt)]
        df_all = df_all.drop_duplicates(subset=constants.TIME_INDEX, keep="first")
        df_all = df_all.sort_values(constants.TIME_INDEX).reset_index(drop=True)
        return df_all

    def _parse_data(self, gauge_id: str, raw_data: pd.DataFrame, variable: str) -> pd.DataFrame:
        """WAMIS returns already structured JSON — no extra parsing required."""
        return raw_data.set_index(constants.TIME_INDEX)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches standardized river data from WAMIS."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            df_raw = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, df_raw, variable)
            return df
        except Exception as e:
            logger.error(f"Failed to get data for {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
