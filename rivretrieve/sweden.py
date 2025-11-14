"""Fetcher for Swedish hydrological data from SMHI HydroObs."""

import logging
from typing import Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class SwedenFetcher(base.RiverDataFetcher):
    """Fetches hydrological time series and station metadata from SMHI HydroObs.

    Data source:
        https://opendata.smhi.se/hydroobs/api

    Supported variables in this fetcher:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_INSTANT`` (cm)
        - ``constants.WATER_TEMPERATURE_INSTANT`` (°C)
        - ``constants.DISCHARGE_MONTHLY_MEAN`` (m³/s)

    Full list of parameters available in HydroObs not yet implemented here:
    - see https://opendata.smhi.se/hydroobs/resources/parameter

    Terms of use:
        - see: https://www.smhi.se/data/om-smhis-data/smhis-datapolicy
    """

    BASE_URL = "https://opendata-download-hydroobs.smhi.se/api/version/latest/"

    # Supported RivRetrieve variables
    SUPPORTED_VARIABLES = {
        constants.DISCHARGE_DAILY_MEAN: 1,
        constants.DISCHARGE_INSTANT: 2,
        constants.STAGE_INSTANT: 3,
        constants.WATER_TEMPERATURE_INSTANT: 4,
        constants.DISCHARGE_MONTHLY_MEAN: 10,
    }


    PARAMETER_UNITS = {
        1: "m³/s",
        2: "m³/s",
        3: "cm",
        4: "°C",
        10: "m³/s",
    }

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        return utils.load_cached_metadata_csv("sweden")

    def _fetch_parameter_metadata(self, pid: int, variable_name: str) -> pd.DataFrame:
        """Fetches metadata for ONE SMHI parameter and preserves all raw fields."""
        url = f"{self.BASE_URL}parameter/{pid}.json"
        logger.info(f"Fetching SMHI metadata from {url}")

        try:
            r = requests.get(url, timeout=40)
            r.raise_for_status()
            js = r.json()
        except Exception as e:
            logger.error(f"Failed to load metadata for pid={pid}: {e}")
            return pd.DataFrame()

        stations = js.get("station", [])

        rows = []
        for st in stations:
            row = dict(st)  # keep all raw SMHI fields untouched

            # Renamed standardized fields
            row[constants.GAUGE_ID] = str(st.get("id"))
            row[constants.STATION_NAME] = st.get("name")
            row[constants.RIVER] = st.get("catchmentName")
            row[constants.AREA] = st.get("catchmentSize")
            row[constants.LATITUDE] = st.get("latitude")
            row[constants.LONGITUDE] = st.get("longitude")

            # Add standard RivRetrieve meta fields
            row[constants.COUNTRY] = "Sweden"
            row[constants.SOURCE] = "SMHI HydroObs"
            row[constants.ALTITUDE] = np.nan  # SMHI does not provide altitude

            # Convert times if present
            row["from"] = pd.to_datetime(st.get("from"), unit="ms", errors="coerce")
            row["to"] = pd.to_datetime(st.get("to"), unit="ms", errors="coerce")

            rows.append(row)

        return pd.DataFrame(rows)

    def get_metadata(self) -> pd.DataFrame:
        """Fetches SMHI metadata for all supported parameters (1,2,3,4,10)."""
        frames = []

        for variable, pid in self.SUPPORTED_VARIABLES.items():
            df = self._fetch_parameter_metadata(pid, variable)
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        df = df.drop_duplicates(subset=constants.GAUGE_ID, keep="first")

        # fix types
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str)
        df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
        df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")

        return df

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(SwedenFetcher.SUPPORTED_VARIABLES.keys())

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str):
        """Downloads the raw CSV string from SMHI."""
        pid = self.SUPPORTED_VARIABLES.get(variable)
        if pid is None:
            raise ValueError(f"Unsupported variable: {variable}")

        url = (
            f"{self.BASE_URL}parameter/{pid}/station/{gauge_id}"
            f"/period/corrected-archive/data.csv"
        )

        logger.info(f"Fetching SMHI data from {url}")
        r = requests.get(url, timeout=40)
        r.raise_for_status()

        return r.text

    def _parse_data(self, gauge_id: str, raw_text: str, variable: str) -> pd.DataFrame:
        """Parses SMHI CSV into standardized RivRetrieve format.

        Returns:
            DataFrame indexed by 'time' with:
                - variable column (float)
                - quality column (string)
        """
        if not raw_text:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable, "quality"])

        lines = raw_text.splitlines()
        records = []

        for ln in lines:
            # Lines starting with date YYYY-MM-DD
            if len(ln) >= 10 and ln[4] == "-" and ln[7] == "-":
                parts = ln.split(";")
                dt = parts[0].strip()
                val = parts[1].strip() if len(parts) > 1 else None
                qual = parts[2].strip() if len(parts) > 2 else None
                records.append((dt, val, qual))

        if not records:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable, "quality"])

        df = pd.DataFrame(records, columns=[constants.TIME_INDEX, variable, "quality"])

        df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], errors="coerce")
        df[variable] = pd.to_numeric(df[variable], errors="coerce")
        df["quality"] = df["quality"].astype("string")

        df = df.dropna(subset=[constants.TIME_INDEX])
        df = df.sort_values(constants.TIME_INDEX)

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

        raw = self._download_data(gauge_id, variable, start_date, end_date)
        df = self._parse_data(gauge_id, raw, variable)

        if df.empty:
            return df

        return df.loc[(df.index >= start_date) & (df.index <= end_date)]
