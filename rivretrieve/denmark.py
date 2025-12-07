"""Fetcher for Danish river gauge data from VandA/H Miljøportal."""

import logging
import re
from typing import Optional

import numpy as np
import pandas as pd
import requests
from pyproj import Transformer

from . import base, constants, utils

logger = logging.getLogger(__name__)

AREA_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:km2|km²|km\^2)", re.IGNORECASE)


class DenmarkFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from VandA/H Miljøportal.

    Data source:
        https://kemidata.miljoeportal.dk/?mt=Hydrometri&sw=25798376

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_INSTANT`` (m)

    Data description and API:
        - see https://vandah.miljoeportal.dk/api/swagger/index.html

    Terms of use:
        - see https://miljoeportal.dk/dataansvar/vilkaar-for-brug/
    """

    METADATA_URL = "https://vandah.miljoeportal.dk/api/stations"
    BASE_URLS = {
        "discharge": "https://vandah.miljoeportal.dk/api/water-flows",
        "stage": "https://vandah.miljoeportal.dk/api/water-levels",
    }

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached metadata (if available)."""
        return utils.load_cached_metadata_csv("denmark_vandah")

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and parses station metadata from VandA/H Miljøportal."""
        try:
            logger.info(f"Fetching VandA/H metadata from {self.METADATA_URL}")
            resp = requests.get(self.METADATA_URL, headers={"Accept": "application/json"}, timeout=30)
            resp.raise_for_status()
            stations = resp.json()

            if not isinstance(stations, list) or not stations:
                logger.warning("No stations found in metadata response.")
                return pd.DataFrame()

            df = pd.json_normalize(stations)

            transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
            lons, lats, areas, rivers = [], [], [], []

            for s in stations:
                x, y, srid = self._extract_lonlat(s)
                lon = lat = None
                if x is not None and y is not None:
                    try:
                        srid = (srid or "").lower()
                        if any(k in srid for k in ["25832", "utm32", "etrs89"]):
                            lon, lat = transformer.transform(x, y)
                        else:
                            lon, lat = float(x), float(y)
                    except Exception:
                        lon = lat = None
                lons.append(lon)
                lats.append(lat)
                areas.append(self._parse_area(s.get("description") or ""))
                name = (s.get("name") or "").strip()
                rivers.append(name.split(",")[0].strip() if name else None)

            df["latitude"] = lats
            df["longitude"] = lons
            df["area"] = areas
            df["river"] = rivers

            rename_map = {"stationId": constants.GAUGE_ID, "name": constants.STATION_NAME}
            df = df.rename(columns=rename_map)

            for col in [
                constants.GAUGE_ID,
                constants.STATION_NAME,
                constants.RIVER,
                constants.LATITUDE,
                constants.LONGITUDE,
                constants.ALTITUDE,
                constants.AREA,
                constants.COUNTRY,
                constants.SOURCE,
            ]:
                if col not in df.columns:
                    df[col] = np.nan

            df[constants.COUNTRY] = "Denmark"
            df[constants.SOURCE] = "VandA/H Miljøportal"
            df[constants.ALTITUDE] = None

            df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
            df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")
            df[constants.AREA] = pd.to_numeric(df[constants.AREA], errors="coerce")
            df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()

            logger.info(f"Fetched {len(df)} VandA/H metadata records.")
            return df.reset_index(drop=True)

        except Exception as e:
            logger.error(f"Failed to fetch VandA/H metadata: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.DISCHARGE_DAILY_MEAN,
            constants.STAGE_DAILY_MEAN,
            constants.DISCHARGE_INSTANT,
            constants.STAGE_INSTANT,
        )

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Downloads raw data for a gauge and variable."""
        var_map = {
            constants.DISCHARGE_DAILY_MEAN: ("discharge", False),
            constants.STAGE_DAILY_MEAN: ("stage", False),
            constants.DISCHARGE_INSTANT: ("discharge", True),
            constants.STAGE_INSTANT: ("stage", True),
        }

        if variable not in var_map:
            raise ValueError(f"Unsupported variable: {variable}")

        var_base, instantaneous = var_map[variable]
        base_url = self.BASE_URLS[var_base]

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        start_iso = start_dt.strftime("%Y-%m-%dT00:00Z")
        end_iso = end_dt.strftime("%Y-%m-%dT23:59Z")

        params = {"stationId": gauge_id, "from": start_iso, "to": end_iso, "format": "json"}

        try:
            logger.info(f"Fetching {variable} for {gauge_id} from {base_url}")
            r = requests.get(base_url, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()

        except Exception as e:
            logger.error(f"Request failed for {gauge_id} ({variable}): {e}")
            return pd.DataFrame()

        if not isinstance(data, list) or not data:
            logger.warning(f"No data found for {gauge_id} ({variable})")
            return pd.DataFrame()

        records = []
        for item in data:
            for res in item.get("results", []):
                t = res.get("measurementDateTime")
                val = res.get("result")
                #val_corr = res.get("resultElevationCorrected")
                if t is None:
                    continue
                ts = pd.to_datetime(t, utc=True, errors="coerce")
                if ts is pd.NaT:
                    continue

                # We use the stage results, not the elevation corrected.
                #if var_base == "stage":
                #    records.append({"time": ts, variable: pd.to_numeric(val_corr, errors="coerce")})
                #else:
                #    records.append({"time": ts, variable: pd.to_numeric(val, errors="coerce")})
                records.append({"time": ts, variable: pd.to_numeric(val, errors="coerce")})

        df = pd.DataFrame(records).dropna().sort_values("time")
        if df.empty:
            return pd.DataFrame()

        # Replace invalids
        df.loc[df[variable] <= -777, variable] = np.nan

        # Aggregate to daily means if not instantaneous
        if not instantaneous:
            df = df.set_index("time").resample("1D").mean().dropna(how="all").reset_index()

        # Convert units
        if "discharge" in variable:
            df[variable] = df[variable] * 0.001  # L/s to m³/s

        # Convert units
        if "stage" in variable:
            df[variable] = df[variable] * 0.01  # cm to m

        return df

    def _parse_data(self, gauge_id: str, raw_data: pd.DataFrame, variable: str) -> pd.DataFrame:
        """Standardize column names and filtering."""
        if raw_data.empty:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
        df = raw_data.rename(columns={"time": constants.TIME_INDEX})
        return df[[constants.TIME_INDEX, variable]].dropna().set_index(constants.TIME_INDEX)

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
            raw_df = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_df, variable)
            return df.loc[(df.index >= start_date) & (df.index <= end_date)]
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    @staticmethod
    def _parse_area(description: str) -> Optional[float]:
        if not description or not isinstance(description, str):
            return None
        m = AREA_RE.search(description)
        if not m:
            return None
        val = m.group(1).replace(",", ".")
        try:
            return float(val)
        except ValueError:
            return None

    @staticmethod
    def _extract_lonlat(station) -> tuple[Optional[float], Optional[float], str]:
        loc = station.get("location") if isinstance(station.get("location"), dict) else {}
        if not loc or loc.get("x") is None or loc.get("y") is None:
            mps = station.get("measurementPoints") or []
            if mps and isinstance(mps, list):
                mploc = mps[0].get("location") if isinstance(mps[0].get("location"), dict) else {}
                loc = mploc if mploc.get("x") is not None and mploc.get("y") is not None else {}
        if not loc:
            return None, None, None
        return loc.get("x"), loc.get("y"), (loc.get("srid") or "").lower()
