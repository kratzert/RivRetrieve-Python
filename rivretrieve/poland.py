"""Fetcher for Polish river gauge data from IMGW."""

import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime
from io import StringIO
from typing import List, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class PolandFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Poland's IMGW."""

    BASE_URL = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_hydrologiczne/"

    @staticmethod
    def get_gauge_ids() -> pd.DataFrame:
        """Retrieves a DataFrame of available Polish gauge IDs."""
        return utils.load_sites_csv("poland")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE, constants.STAGE, constants.WATER_TEMPERATURE)

    def _get_metadata_headers(self):
        """Fetches and cleans metadata headers."""
        try:
            address_meta1 = self.BASE_URL + "dobowe/codz_info.txt"
            response1 = utils.requests_retry_session().get(address_meta1)
            response1.raise_for_status()
            content1 = response1.content.decode("cp1250", errors="ignore")
            lines1 = content1.splitlines()[2:12]  # Daily data has 10 header lines
            cleaned1 = [
                re.sub(r"\s+", " ", re.sub(r"[?'^]", "", line)).strip()
                for line in lines1
            ]
            return cleaned1
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching metadata headers: {e}")
            raise

    def _download_data(
        self, gauge_id: str, variable: str, start_date: str, end_date: str
    ) -> List[pd.DataFrame]:
        """Downloads raw data from IMGW for the specified date range."""
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        s = utils.requests_retry_session()
        all_data = []
        meta_headers = self._get_metadata_headers()

        for year in range(start_dt.year, end_dt.year + 1):
            year_url = f"{self.BASE_URL}dobowe/{year}/"
            try:
                response = s.get(year_url)
                response.raise_for_status()
                html = response.text
                zip_files = re.findall(r'href="(codz_\d{4}_\d{2}\.zip)"', html)

                for fname in zip_files:
                    file_url = f"{year_url}{fname}"
                    resp = s.get(file_url)
                    resp.raise_for_status()

                    with tempfile.TemporaryDirectory() as tmpdir:
                        zip_path = os.path.join(tmpdir, fname)
                        with open(zip_path, "wb") as f:
                            f.write(resp.content)
                        with zipfile.ZipFile(zip_path, "r") as zf:
                            for member in zf.namelist():
                                with zf.open(member) as f:
                                    df = _imgw_read(f)
                                    if not df.empty:
                                        if df.shape[1] == len(meta_headers):
                                            df.columns = meta_headers
                                            all_data.append(df)
                                        elif (
                                            df.shape[1] == 9
                                        ):  # Special case for current year format
                                            df["flow"] = None
                                            df = df.iloc[:, list(range(7)) + [9, 7, 8]]
                                            df.columns = meta_headers
                                            all_data.append(df)
                                        else:
                                            logger.warning(
                                                f"Column mismatch in {fname} for {gauge_id}"
                                            )

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data for year {year}: {e}")
            except Exception as e:
                logger.error(f"Error processing data for year {year}: {e}")

        return all_data

    def _parse_data(
        self, gauge_id: str, raw_data_list: List[pd.DataFrame], variable: str
    ) -> pd.DataFrame:
        """Parses the raw dataframes into a standardized format."""
        if not raw_data_list:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            full_df = pd.concat(raw_data_list, ignore_index=True)
            if full_df.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            # Filter for the specific gauge ID
            full_df = full_df[full_df["Kod stacji"] == int(gauge_id)]
            if full_df.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            # Build Date column
            df_dates = full_df[
                ["Rok hydrologiczny", "Miesiąc kalendarzowy", "Dzień"]
            ].astype(int)
            df_dates.columns = ["hyy", "mm", "dd"]
            df_dates["yy"] = df_dates["hyy"] - (df_dates["mm"] >= 11).astype(int)
            full_df[constants.TIME_INDEX] = pd.to_datetime(
                dict(year=df_dates["yy"], month=df_dates["mm"], day=df_dates["dd"]),
                errors="coerce",
            )
            full_df = full_df.dropna(subset=[constants.TIME_INDEX])

            # Select variable
            if variable == constants.DISCHARGE:
                var_col = "Przepływ [m3/s]"
            elif variable == constants.STAGE:
                var_col = "Stan wody [cm]"
            elif variable == constants.WATER_TEMPERATURE:
                var_col = "Temperatura wody [st. C]"
            else:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            full_df[variable] = pd.to_numeric(full_df[var_col], errors="coerce")

            if variable == constants.STAGE:
                full_df[variable] = full_df[variable] / 100.0  # cm to m

            # Clean placeholder values
            full_df.replace(
                {9999: None, 99999.999: None, 99.9: None, 999: None}, inplace=True
            )

            result_df = (
                full_df[[constants.TIME_INDEX, variable]]
                .dropna()
                .sort_values(by=constants.TIME_INDEX)
                .reset_index(drop=True)
            )
            return result_df

        except Exception as e:
            logger.error(f"Error parsing data for site {gauge_id}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses Polish river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data_list = self._download_data(
                gauge_id, variable, start_date, end_date
            )
            df = self._parse_data(gauge_id, raw_data_list, variable)

            # Filter by date range
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[
                (df[constants.TIME_INDEX] >= start_date_dt)
                & (df[constants.TIME_INDEX] <= end_date_dt)
            ]
            return df
        except Exception as e:
            logger.error(
                f"Failed to get data for site {gauge_id}, variable {variable}: {e}"
            )
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])


def _imgw_read(fpath: str) -> pd.DataFrame:
    """Helper function to read IMGW CSV files with various encodings and separators."""
    try:
        data = pd.read_csv(fpath, header=None, sep=",", encoding="cp1250")
    except Exception:
        try:
            data = pd.read_csv(fpath, header=None, sep=";")
        except Exception:
            data = pd.DataFrame()

    if data.shape[1] == 1:
        try:
            data = pd.read_csv(fpath, header=None, sep=";", encoding="utf-8")
        except Exception:
            try:
                data = pd.read_csv(fpath, header=None, sep=";")
            except Exception:
                data = pd.DataFrame()

    if data.shape[1] == 1:
        try:
            data = pd.read_csv(fpath, header=None, sep=",", encoding="cp1250")
        except Exception:
            pass

    return data
