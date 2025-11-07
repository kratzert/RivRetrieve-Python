"""Fetcher for Berlin river gauge data from Wasserportal Berlin."""

import logging
from io import StringIO
from typing import Optional

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pyproj import Transformer

from . import base, constants, utils

logger = logging.getLogger(__name__)


class GermanyBerlinFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Wasserportal Berlin.

    Data source:
        https://wasserportal.berlin.de/

    Supported variables:
        - constants.STAGE_DAILY_MEAN (m)
        - constants.DISCHARGE_DAILY_MEAN (m³/s)
        - constants.WATER_TEMPERATURE_DAILY_MEAN (°C)
        - constants.STAGE_INSTANT (m)
        - constants.DISCHARGE_INSTANT (m³/s)

    Data description and API:
        - see https://wasserportal.berlin.de/download/

    Terms of use:
        - see https://daten.berlin.de/impressum
    """

    METADATA_URL = "https://wasserportal.berlin.de/start.php?anzeige=tabelle_ow&messanzeige=ms_all"
    BASE_URL = (
        "https://wasserportal.berlin.de/station.php"
        "?anzeige=d&station={id}&thema={thema}&sreihe={frequency}&smode=c&sdatum={start_date}"
    )

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached metadata (if available)."""
        return utils.load_cached_metadata_csv("germany_berlin")

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and parses site metadata from Wasserportal Berlin.

        Keeps all original columns, but renames and standardizes key fields.
        Converts UTM33N (EPSG:32633) → WGS84 (EPSG:4326) coordinates.
        """
        try:
            logger.info(f"Fetching Berlin metadata from {self.METADATA_URL}")
            resp = requests.get(self.METADATA_URL, timeout=20)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table")
            if table is None:
                raise ValueError("No table found in metadata page.")

            df = pd.read_html(str(table))[0]
            df.columns = [c.strip() for c in df.columns]

            rename_map = {
                "Messstellen- nummer": constants.GAUGE_ID,
                "Messstellen- name": constants.STATION_NAME,
                "Gewässer": constants.RIVER,
                "Rechts- wert": "utm_easting",
                "Hoch- wert": "utm_northing",
            }
            df = df.rename(columns=rename_map)

            # Convert numeric UTM coordinates
            if "utm_easting" in df.columns and "utm_northing" in df.columns:
                df["utm_easting"] = pd.to_numeric(df["utm_easting"], errors="coerce")
                df["utm_northing"] = pd.to_numeric(df["utm_northing"], errors="coerce")

                # Drop invalid coordinates
                df = df.dropna(subset=["utm_easting", "utm_northing"])

                # Fix scaling (some values are 10× larger)
                mask_large = df["utm_easting"] > 1_000_000
                df.loc[mask_large, "utm_easting"] = df.loc[mask_large, "utm_easting"] / 10

                transformer = Transformer.from_crs("EPSG:32633", "EPSG:4326", always_xy=True)
                lon, lat = transformer.transform(df["utm_easting"].values, df["utm_northing"].values)
                df[constants.LONGITUDE] = lon
                df[constants.LATITUDE] = lat
            else:
                df[constants.LONGITUDE] = np.nan
                df[constants.LATITUDE] = np.nan

            # Add standardized metadata fields if missing
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

            df[constants.ALTITUDE] = None
            df[constants.AREA] = None
            df[constants.COUNTRY] = "Germany"
            df[constants.SOURCE] = "Wasserportal Berlin"

            # Clean and type-correct
            df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
            df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
            df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")

            # Drop duplicates
            df = df.drop_duplicates(subset=[constants.GAUGE_ID])

            logger.info(f"Fetched {len(df)} Berlin gauge metadata records.")
            return df.reset_index(drop=True)

        except Exception as e:
            logger.error(f"Failed to fetch metadata: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.STAGE_DAILY_MEAN,
            constants.DISCHARGE_DAILY_MEAN,
            constants.WATER_TEMPERATURE_DAILY_MEAN,
            constants.STAGE_INSTANT,
            constants.DISCHARGE_INSTANT,
        )

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Downloads CSV data for a gauge and variable."""
        thema_map = {
            # Daily
            constants.STAGE_DAILY_MEAN: ("ows", "tw"),
            constants.DISCHARGE_DAILY_MEAN: ("odf", "tw"),
            constants.WATER_TEMPERATURE_DAILY_MEAN: ("owt", "tw"),
            # Instantaneous
            constants.STAGE_INSTANT: ("ows", "ew"),
            constants.DISCHARGE_INSTANT: ("odf", "ew"),
        }

        if variable not in thema_map:
            raise ValueError(f"Unsupported variable: {variable}")

        thema, frequency = thema_map[variable]
        start_date_fmt = pd.to_datetime(start_date).strftime("%d.%m.%Y")
        url = self.BASE_URL.format(id=gauge_id, thema=thema, frequency=frequency, start_date=start_date_fmt)

        logger.info(f"Fetching {variable} for {gauge_id} from {url}")
        r = requests.get(url, timeout=20)
        r.raise_for_status()

        csv_text = r.text.strip()
        if not csv_text or "<html" in csv_text or "Fehler" in csv_text:
            logger.warning(f"No data returned for {gauge_id} ({variable})")
            return pd.DataFrame()

        try:
            df = pd.read_csv(StringIO(csv_text), sep=";", decimal=",", encoding="utf-8")
            return df
        except Exception as e:
            logger.error(f"Error parsing CSV for {gauge_id}: {e}")
            return pd.DataFrame()

    def _parse_data(self, gauge_id: str, raw_data: pd.DataFrame, variable: str) -> pd.DataFrame:
        """Parses Wasserportal CSV to standardized DataFrame."""
        if raw_data.empty:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        raw_data.columns = [c.strip().lower() for c in raw_data.columns]
        time_col = next((c for c in raw_data.columns if "datum" in c or "zeit" in c), raw_data.columns[0])
        val_col = next(
            (c for c in raw_data.columns if c not in [time_col] and raw_data[c].dtype != "O"), raw_data.columns[1]
        )

        raw_data[constants.TIME_INDEX] = pd.to_datetime(raw_data[time_col], dayfirst=True, errors="coerce")
        raw_data[variable] = pd.to_numeric(raw_data[val_col], errors="coerce")

        if variable == constants.STAGE_DAILY_MEAN:
            raw_data[variable] = raw_data[variable] / 100.0  # cm → m

        df = (
            raw_data[[constants.TIME_INDEX, variable]]
            .dropna()
            .sort_values(by=constants.TIME_INDEX)
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
