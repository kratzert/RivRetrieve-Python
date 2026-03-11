"""Fetcher for Danish river gauge data from VandA/H Miljoportal."""

import logging
import math
import re
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)

AREA_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:km2|km\xb2|km\^2)", re.IGNORECASE)


class DenmarkFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Denmark's VandA/H Miljoportal.

    Data Source: VandA/H Miljoportal (https://vandah.miljoeportal.dk/)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m3/s)
        - ``constants.DISCHARGE_INSTANT`` (m3/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.STAGE_INSTANT`` (m)
    """

    BASE_URL = "https://vandah.miljoeportal.dk/api"
    SOURCE = "VandA/H Miljoportal"
    COUNTRY = "Denmark"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available Danish gauge IDs and metadata."""
        return utils.load_cached_metadata_csv("denmark")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.DISCHARGE_DAILY_MEAN,
            constants.DISCHARGE_INSTANT,
            constants.STAGE_DAILY_MEAN,
            constants.STAGE_INSTANT,
        )

    @staticmethod
    def _parse_area(description: str) -> Optional[float]:
        if not description or not isinstance(description, str):
            return None

        match = AREA_RE.search(description)
        if not match:
            return None

        value = match.group(1).replace(",", ".")
        try:
            return float(value)
        except ValueError:
            return None

    @staticmethod
    def _extract_lonlat(station: dict[str, Any]) -> tuple[Optional[float], Optional[float], str]:
        """Extract coordinates from station or its first measurement point."""
        location = station.get("location") if isinstance(station.get("location"), dict) else {}
        if not location or location.get("x") is None or location.get("y") is None:
            measurement_points = station.get("measurementPoints") or []
            if measurement_points and isinstance(measurement_points, list):
                point_location = (
                    measurement_points[0].get("location")
                    if isinstance(measurement_points[0].get("location"), dict)
                    else {}
                )
                if point_location.get("x") is not None and point_location.get("y") is not None:
                    location = point_location

        if not location:
            return None, None, ""

        return location.get("x"), location.get("y"), str(location.get("srid") or "").lower()

    @staticmethod
    def _utm32_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
        """Converts EPSG:25832 coordinates to WGS84 using the UTM zone 32N formula."""
        a = 6378137.0
        f = 1 / 298.257223563
        e2 = f * (2 - f)
        e_prime_sq = e2 / (1 - e2)
        k0 = 0.9996

        x = easting - 500000.0
        y = northing
        lon0 = math.radians(9.0)

        m = y / k0
        mu = m / (a * (1 - e2 / 4 - 3 * e2**2 / 64 - 5 * e2**3 / 256))
        e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))

        j1 = 3 * e1 / 2 - 27 * e1**3 / 32
        j2 = 21 * e1**2 / 16 - 55 * e1**4 / 32
        j3 = 151 * e1**3 / 96
        j4 = 1097 * e1**4 / 512

        fp = mu + j1 * math.sin(2 * mu) + j2 * math.sin(4 * mu) + j3 * math.sin(6 * mu) + j4 * math.sin(8 * mu)
        sin_fp = math.sin(fp)
        cos_fp = math.cos(fp)
        tan_fp = math.tan(fp)

        c1 = e_prime_sq * cos_fp**2
        t1 = tan_fp**2
        n1 = a / math.sqrt(1 - e2 * sin_fp**2)
        r1 = a * (1 - e2) / (1 - e2 * sin_fp**2) ** 1.5
        d = x / (n1 * k0)

        lat = fp - (n1 * tan_fp / r1) * (
            d**2 / 2
            - (5 + 3 * t1 + 10 * c1 - 4 * c1**2 - 9 * e_prime_sq) * d**4 / 24
            + (61 + 90 * t1 + 298 * c1 + 45 * t1**2 - 252 * e_prime_sq - 3 * c1**2) * d**6 / 720
        )
        lon = lon0 + (
            d
            - (1 + 2 * t1 + c1) * d**3 / 6
            + (5 - 2 * c1 + 28 * t1 - 3 * c1**2 + 8 * e_prime_sq + 24 * t1**2) * d**5 / 120
        ) / cos_fp

        return math.degrees(lat), math.degrees(lon)

    @classmethod
    def _to_wgs84(cls, x: Any, y: Any, srid: str) -> tuple[Optional[float], Optional[float]]:
        if x is None or y is None:
            return None, None

        try:
            if srid and ("25832" in srid or "utm32" in srid or "etrs89" in srid):
                lat, lon = cls._utm32_to_wgs84(float(x), float(y))
                return lon, lat

            if "4326" in srid or "wgs84" in srid or not srid:
                return float(x), float(y)

            return float(x), float(y)
        except Exception:
            return None, None

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata for all stations from the VandA/H API."""
        url = f"{self.BASE_URL}/stations"
        headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 (compatible)"}
        session = utils.requests_retry_session()

        try:
            response = session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            stations = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Danish stations list: {e}")
            raise
        except ValueError as e:
            logger.error(f"Error decoding Danish stations list: {e}")
            raise

        if not isinstance(stations, list) or not stations:
            return pd.DataFrame().set_index(constants.GAUGE_ID)

        df = pd.json_normalize(stations)
        latitudes = []
        longitudes = []
        areas = []
        rivers = []

        for station in stations:
            x, y, srid = self._extract_lonlat(station)
            lon, lat = self._to_wgs84(x, y, srid)
            longitudes.append(lon)
            latitudes.append(lat)
            areas.append(self._parse_area(station.get("description") or ""))

            name = (station.get("name") or "").strip()
            rivers.append(name.split(",")[0].strip() if name else None)

        df[constants.LATITUDE] = latitudes
        df[constants.LONGITUDE] = longitudes
        df[constants.AREA] = areas
        df[constants.RIVER] = rivers

        df = df.rename(columns={"stationId": constants.GAUGE_ID, "name": constants.STATION_NAME})

        standard_columns = [
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
        for column in standard_columns:
            if column not in df.columns:
                df[column] = np.nan

        df[constants.COUNTRY] = self.COUNTRY
        df[constants.SOURCE] = self.SOURCE
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
        df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")
        df[constants.AREA] = pd.to_numeric(df[constants.AREA], errors="coerce")

        return df.reset_index(drop=True).set_index(constants.GAUGE_ID)

    @staticmethod
    def _get_variable_parts(variable: str) -> tuple[str, bool]:
        if variable == constants.DISCHARGE_DAILY_MEAN:
            return "discharge", False
        if variable == constants.DISCHARGE_INSTANT:
            return "discharge", True
        if variable == constants.STAGE_DAILY_MEAN:
            return "stage", False
        if variable == constants.STAGE_INSTANT:
            return "stage", True
        raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Downloads raw JSON data from the VandA/H API."""
        variable_base, instantaneous = self._get_variable_parts(variable)
        endpoint = "water-flows" if variable_base == constants.DISCHARGE else "water-levels"
        url = f"{self.BASE_URL}/{endpoint}"

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        if instantaneous:
            duration_years = (end_dt - start_dt).days / 365.25
            if duration_years > 3:
                raise MemoryError(
                    "Please request less than 3 years of instantaneous (10-minute) data to avoid excessive memory use."
                )

        params = {
            "stationId": gauge_id,
            "format": "json",
            "from": start_dt.strftime("%Y-%m-%dT00:00Z"),
            "to": end_dt.strftime("%Y-%m-%dT23:59Z"),
        }

        try:
            response = utils.requests_retry_session().get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Danish data for site {gauge_id}: {e}")
            raise
        except ValueError as e:
            logger.error(f"Error decoding Danish data for site {gauge_id}: {e}")
            raise

        if not isinstance(data, list):
            return []

        return data

    def _parse_data(self, gauge_id: str, raw_data: list[dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses raw VandA/H JSON into a standardized DataFrame."""
        if not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        variable_base, instantaneous = self._get_variable_parts(variable)
        records = []

        for item in raw_data:
            for result in item.get("results", []):
                timestamp = result.get("measurementDateTime")
                if timestamp is None:
                    continue

                try:
                    parsed_time = pd.to_datetime(timestamp, utc=True).tz_localize(None)
                except Exception:
                    continue

                if variable_base == constants.STAGE:
                    value = pd.to_numeric(result.get("resultElevationCorrected"), errors="coerce")
                else:
                    value = pd.to_numeric(result.get("result"), errors="coerce")

                records.append({constants.TIME_INDEX: parsed_time, variable: value})

        if not records:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = pd.DataFrame(records).sort_values(constants.TIME_INDEX).reset_index(drop=True)
        df.loc[df[variable] <= -777, variable] = np.nan

        if len(df) > 2_000_000 and instantaneous:
            raise MemoryError(f"Too much data returned ({len(df)} rows) for {gauge_id}. Try a shorter period.")

        if variable_base == constants.DISCHARGE:
            df[variable] = df[variable] * 0.001  # L/s -> m3/s
        else:
            df[variable] = df[variable] * 0.01  # cm -> m

        if not instantaneous:
            df = (
                df.set_index(constants.TIME_INDEX)[[variable]]
                .resample("1D")
                .mean()
                .dropna(how="all")
                .reset_index()
            )

        return df.dropna(subset=[variable]).set_index(constants.TIME_INDEX)

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

            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            return df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
