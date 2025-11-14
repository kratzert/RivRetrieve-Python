"""Fetcher for Estonia river gauge data from EstModel + WISKI."""

import logging
from typing import Optional

import numpy as np
import pandas as pd
import requests
import re
import unicodedata
from pyproj import Transformer

from . import base, constants, utils

logger = logging.getLogger(__name__)


class EstoniaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from EstModel (Estonia) with WISKI geolocation.

    Data sources:
        - EstModel hydrological platform (https://estmodel.envir.ee)
        - Estonian Geoportal WISKI hydrology database (WFS) (https://inspire.geoportaal.ee/geoserver/EF_hydrojaamad/wfs)

    Supported variables:
        - constants.DISCHARGE_DAILY_MEAN (m³/s)
        - constants.DISCHARGE_DAILY_MAX (m³/s)
        - constants.DISCHARGE_DAILY_MIN (m³/s)
        - constants.STAGE_DAILY_MEAN (m)
        - constants.STAGE_DAILY_MAX (m)
        - constants.STAGE_DAILY_MIN (m)
        - constants.WATER_TEMPERATURE_DAILY_MEAN (°C)
        - constants.WATER_TEMPERATURE_DAILY_MAX (°C)
        - constants.WATER_TEMPERATURE_DAILY_MIN (°C)

    API description:
        https://estmodel.envir.ee/stations

    Metadata:
        - Station list via EstModel (coordinates incomplete)
        - Coordinates merged from WISKI WFS (EPSG:3301 to EPSG:4326)

    Terms of use:
        - see https://keskkonnaportaal.ee/et/avaandmed
    """

    # Variable Mapping
    VAR_MAP = {
        constants.DISCHARGE_DAILY_MEAN: ("Q", "MEAN"),
        constants.DISCHARGE_DAILY_MAX: ("Q", "MAXIMUM"),
        constants.DISCHARGE_DAILY_MIN: ("Q", "MINIMUM"),

        constants.STAGE_DAILY_MEAN: ("H", "MEAN"),
        constants.STAGE_DAILY_MAX: ("H", "MAXIMUM"),
        constants.STAGE_DAILY_MIN: ("H", "MINIMUM"),

        constants.WATER_TEMPERATURE_DAILY_MEAN: ("T", "MEAN"),
        constants.WATER_TEMPERATURE_DAILY_MAX: ("T", "MAXIMUM"),
        constants.WATER_TEMPERATURE_DAILY_MIN: ("T", "MINIMUM"),
    }

    SUPPORTED_VARIABLES = tuple(VAR_MAP.keys())

    # Cached Metadata
    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached metadata (if available)."""
        return utils.load_cached_metadata_csv("estonia")

    # Metadata Download
    def get_metadata(self) -> pd.DataFrame:
        """Downloads and merges hydrological station metadata from EstModel and WISKI.

        Produces standardized columns:
            - gauge_id
            - station_name
            - river
            - latitude
            - longitude
            - area
            - altitude
            - country
            - source

        Coordinates are converted from EPSG:3301 (WISKI) to EPSG:4326.
        """

        # ---------- Helpers ----------
        def normalize_name(name: str) -> str:
            if not isinstance(name, str):
                return ""
            name = name.lower()
            name = ''.join(c for c in unicodedata.normalize("NFKD", name)
                           if not unicodedata.combining(c))
            name = re.sub(r"[-/:.,]", " ", name)
            name = re.sub(r"h[üu]dro\w*|jaam", "", name)
            return re.sub(r"\s+", " ", name).strip()

        def extract_river(s):
            if not isinstance(s, str):
                return None
            parts = s.split(":")
            return parts[0].strip() if len(parts) > 1 else None

        def extract_loc(s):
            if not isinstance(s, str):
                return ""
            parts = s.split(":")
            return parts[1].strip() if len(parts) > 1 else s.strip()

        # Fetch EstModel
        url_est = "https://estmodel.envir.ee/stations"
        logger.info(f"Fetching Estonian metadata from {url_est}")

        r1 = requests.get(url_est, timeout=40)
        r1.raise_for_status()
        data_est = r1.json()

        df_est = pd.DataFrame(data_est)
        df_est = df_est.rename(columns={"code": constants.GAUGE_ID,
                                        "name": constants.STATION_NAME})

        df_est["river"] = df_est[constants.STATION_NAME].apply(extract_river)
        df_est["location"] = df_est[constants.STATION_NAME].apply(extract_loc)

        # Keep only hydrological stations
        df_est = df_est[df_est.get("type", "") == "HYDROLOGICAL"].copy()

        # Add standardized missing fields
        for col in [
            constants.GAUGE_ID, constants.STATION_NAME,
            constants.RIVER, constants.LATITUDE, constants.LONGITUDE,
            constants.ALTITUDE, constants.AREA,
            constants.COUNTRY, constants.SOURCE
        ]:
            if col not in df_est.columns:
                df_est[col] = np.nan

        # WISKI geometry
        url_wfs = (
            "https://inspire.geoportaal.ee/geoserver/EF_hydrojaamad/wfs"
            "?request=GetFeature&service=WFS&version=2.0.0"
            "&outputFormat=application/json"
            "&typeNames=EF_hydrojaamad:EF.EnvironmentalMonitoringFacilities"
        )
        logger.info(f"Fetching WISKI metadata from {url_wfs}")

        r2 = requests.get(url_wfs, timeout=40)
        r2.raise_for_status()

        transformer = Transformer.from_crs("EPSG:3301", "EPSG:4326", always_xy=True)

        wiski_rows = []
        for f in r2.json().get("features", []):
            props = f.get("properties", {})
            coords = f.get("geometry", {}).get("coordinates", [None, None])
            if None in coords:
                continue
            lon, lat = transformer.transform(coords[0], coords[1])
            props["latitude"] = lat
            props["longitude"] = lon
            wiski_rows.append(props)

        df_wiski = pd.DataFrame(wiski_rows)

        # Merge (by fuzzy-matching)
        for i, row in df_est.iterrows():
            name_est = normalize_name(row.get("location", ""))
            for _, wrow in df_wiski.iterrows():
                name_wiski = normalize_name(wrow.get("name", ""))
                if name_est and (name_est in name_wiski or name_wiski in name_est):
                    df_est.at[i, constants.LATITUDE] = wrow["latitude"]
                    df_est.at[i, constants.LONGITUDE] = wrow["longitude"]
                    break

        # Add final constants
        df_est[constants.COUNTRY] = "Estonia"
        df_est[constants.SOURCE] = "EstModel + WISKI"

        df_est[constants.LATITUDE] = pd.to_numeric(df_est[constants.LATITUDE], errors="coerce")
        df_est[constants.LONGITUDE] = pd.to_numeric(df_est[constants.LONGITUDE], errors="coerce")

        df_est[constants.GAUGE_ID] = df_est[constants.GAUGE_ID].astype(str).str.strip()

        df_est = df_est.dropna(subset=[constants.GAUGE_ID]).reset_index(drop=True)
        return df_est

    # Variables
    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return EstoniaFetcher.SUPPORTED_VARIABLES

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str):
        """Downloads JSON hydrological data from EstModel."""
        if variable not in self.VAR_MAP:
            raise ValueError(f"Unsupported variable: {variable}")

        param, dtype = self.VAR_MAP[variable]

        base_url = f"https://estmodel.envir.ee/stations/{gauge_id}/measurements"
        params = {
            "parameter": param,
            "type": dtype,
            "start-year": pd.to_datetime(start_date).year,
            "end-year": pd.to_datetime(end_date).year,
        }

        logger.info(f"Fetching EstModel data: {base_url}?{params}")

        try:
            r = requests.get(base_url, params=params, timeout=40)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Failed to download EstModel data for {gauge_id}/{variable}: {e}")
            return []

    def _parse_data(self, gauge_id: str, raw_data: list, variable: str) -> pd.DataFrame:
        """Parses EstModel data into a standardized DataFrame."""
        if not isinstance(raw_data, list) or not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = pd.DataFrame(raw_data)

        if "startDate" not in df or "value" not in df:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = df.rename(columns={"startDate": constants.TIME_INDEX, "value": variable})
        df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], errors="coerce")
        df[variable] = pd.to_numeric(df[variable], errors="coerce")

        df = (
            df[[constants.TIME_INDEX, variable]]
            .dropna()
            .drop_duplicates(subset=[constants.TIME_INDEX])
            .sort_values(constants.TIME_INDEX)
            .set_index(constants.TIME_INDEX)
        )

        return df

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses a time series for a specific station and variable."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        raw = self._download_data(gauge_id, variable, start_date, end_date)
        df = self._parse_data(gauge_id, raw, variable)

        return df.loc[(df.index >= start_date) & (df.index <= end_date)]
