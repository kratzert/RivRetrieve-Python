"""Fetcher for Canadian river gauge data from HYDAT."""

import io
import logging
import os
import re
import shutil
import sqlite3
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from bs4 import BeautifulSoup

from . import base, utils

logger = logging.getLogger(__name__)


class CanadaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Canada's HYDAT database."""

    HYDAT_URL = "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/"
    DATA_DIR = Path(os.path.dirname(__file__)) / "data"
    HYDAT_PATH = DATA_DIR / "Hydat.sqlite3"

    @staticmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available Canadian gauge sites."""
        return utils.load_sites_csv("canada")

    def _find_latest_hydat_link(self) -> Optional[str]:
        s = utils.requests_retry_session()
        try:
            response = s.get(self.HYDAT_URL)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            links = soup.find_all('a')
            latest_date = None
            latest_link = None

            for link in links:
                href = link.get('href')
                if href:
                    match = re.match(r"Hydat_sqlite3_(\d{8})\.zip", href)
                    if match:
                        date_str = match.group(1)
                        try:
                            current_date = datetime.strptime(date_str, "%Y%m%d")
                            if latest_date is None or current_date > latest_date:
                                latest_date = current_date
                                latest_link = self.HYDAT_URL + href
                        except ValueError:
                            continue
            return latest_link
        except Exception as e:
            logger.error(f"Error finding latest HYDAT link: {e}")
            return None

    def _download_hydat(self) -> bool:
        """Downloads and extracts the latest HYDAT SQLite database."""
        logger.info("Checking for latest HYDAT database...")
        latest_link = self._find_latest_hydat_link()
        if not latest_link:
            logger.error("Could not find download link for HYDAT database.")
            return False

        zip_filename = latest_link.split("/")[-1]
        sqlite_filename = zip_filename.replace(".zip", ".sqlite3")
        self.HYDAT_PATH = self.DATA_DIR / sqlite_filename

        if self.HYDAT_PATH.exists():
            logger.info(f"Latest HYDAT database {sqlite_filename} already exists at {self.HYDAT_PATH}")
            return True

        logger.info(f"Downloading {zip_filename}...")
        s = utils.requests_retry_session()
        try:
            # HYDAT no longer requires license click-through on this new base URL
            response = s.get(latest_link, stream=True, timeout=300)
            response.raise_for_status()

            self.DATA_DIR.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                extracted = False
                for name in zf.namelist():
                    if name.endswith(".sqlite3"):
                        zf.extract(name, path=self.DATA_DIR)
                        # Rename to the expected versioned filename
                        shutil.move(self.DATA_DIR / name, self.HYDAT_PATH)
                        extracted = True
                        break
                if not extracted:
                    logger.error(f"No .sqlite3 file found in {zip_filename}.")
                    return False

            logger.info(f"Successfully downloaded and extracted HYDAT to {self.HYDAT_PATH}")
            return True

        except Exception as e:
            logger.error(f"Error downloading or extracting HYDAT: {e}")
            if self.HYDAT_PATH.exists():  # Clean up partial extraction
                os.remove(self.HYDAT_PATH)
            return False

    def _get_hydat_connection(self):
        if not self.HYDAT_PATH.exists():
            if not self._download_hydat():
                raise FileNotFoundError("Failed to download HYDAT database.")
        return sqlite3.connect(self.HYDAT_PATH)

    def get_data(self, variable: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches data from the local HYDAT SQLite database."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        col_name = utils.get_column_name(variable)

        if variable == "discharge":
            table = "DLY_FLOWS"
            value_prefix = "FLOW"
        elif variable == "stage":
            table = "DLY_LEVELS"
            value_prefix = "LEVEL"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            conn = self._get_hydat_connection()

            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)

            query = f"""
                SELECT *
                FROM {table}
                WHERE STATION_NUMBER = ?
                  AND YEAR BETWEEN ? AND ?
            """
            df = pd.read_sql_query(query, conn, params=(self.site_id, start_dt.year, end_dt.year))
            conn.close()

            if df.empty:
                return pd.DataFrame(columns=["Date", col_name])

            # Unpivot day columns
            day_cols = [f"{value_prefix}{i}" for i in range(1, 32)]
            id_vars = ['STATION_NUMBER', 'YEAR', 'MONTH']

            # Ensure all day columns exist, add if not
            for col in day_cols:
                if col not in df.columns:
                    df[col] = None

            df_long = pd.melt(df, id_vars=id_vars, value_vars=day_cols, var_name='Day_Col', value_name=col_name)
            df_long['DAY'] = df_long['Day_Col'].str.replace(value_prefix, "").astype(int)

            # Create Date column
            date_cols = ['YEAR', 'MONTH', 'DAY']
            df_long['Date'] = pd.to_datetime(df_long[date_cols], errors='coerce')
            df_long = df_long.dropna(subset=['Date'])

            # Filter by date range
            df_long = df_long[(df_long["Date"] >= start_dt) & (df_long["Date"] <= end_dt)]

            df_long[col_name] = pd.to_numeric(df_long[col_name], errors='coerce')
            return df_long[["Date", col_name]].dropna().sort_values(by="Date").reset_index(drop=True)

        except Exception as e:
            logger.error(f"Error querying or processing HYDAT for site {self.site_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=["Date", col_name])

    # These are not used for Canada as data is local
    def _download_data(self, variable: str, start_date: str, end_date: str) -> Any:
        return None

    def _parse_data(self, raw_data: Any, variable: str) -> pd.DataFrame:
        return pd.DataFrame()
