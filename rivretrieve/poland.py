"""Fetcher for Polish river gauge data from IMGW."""

import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime
from typing import List, Optional
from pathlib import Path

import pandas as pd
import requests
import xarray as xr

from . import base, constants, utils

logger = logging.getLogger(__name__)


class PolandFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Poland's IMGW."""

    BASE_URL = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_hydrologiczne/"
    CACHE_FILE = Path(os.path.dirname(__file__)) / "data" / "poland.zarr"

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

    def _download_all_data(self, start_year: int, end_year: int) -> List[pd.DataFrame]:
        """Downloads raw data from IMGW for the specified year range."""
        s = utils.requests_retry_session()
        all_data = []
        meta_headers = self._get_metadata_headers()

        for year in range(start_year, end_year + 1):
            year_url = f"{self.BASE_URL}dobowe/{year}/"
            try:
                response = s.get(year_url)
                response.raise_for_status()
                html = response.text
                zip_files = re.findall(r'href="(codz_\d{4}_\d{2}\.zip)"', html)
                logger.info(f"Found {len(zip_files)} zip files for year {year}")

                for i, fname in enumerate(zip_files):
                    logger.info(
                        f"Downloading and processing {fname} ({i + 1}/{len(zip_files)})"
                    )
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
                                                f"Column mismatch in {fname}"
                                            )

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data for year {year}: {e}")
            except Exception as e:
                logger.error(f"Error processing data for year {year}: {e}")

        return all_data

    def _parse_all_data(self, raw_data_list: List[pd.DataFrame]) -> pd.DataFrame:
        """Parses the raw dataframes into a single standardized format."""
        if not raw_data_list:
            return pd.DataFrame()

        try:
            full_df = pd.concat(raw_data_list, ignore_index=True)
            if full_df.empty:
                return pd.DataFrame()

            # Rename columns
            full_df = full_df.rename(
                columns={
                    "Kod stacji": constants.GAUGE_ID,
                    "Przepływ [m3/s]": constants.DISCHARGE,
                    "Stan wody [cm]": constants.STAGE,
                    "Temperatura wody [st. C]": constants.WATER_TEMPERATURE,
                }
            )

            # Build Date column
            date_cols = ["Rok hydrologiczny", "Miesiąc kalendarzowy", "Dzień"]
            full_df = full_df.dropna(subset=date_cols)
            df_dates = full_df[date_cols].astype(int)
            df_dates.columns = ["hyy", "mm", "dd"]
            df_dates["yy"] = df_dates["hyy"] - (df_dates["mm"] >= 11).astype(int)
            full_df[constants.TIME_INDEX] = pd.to_datetime(
                dict(year=df_dates["yy"], month=df_dates["mm"], day=df_dates["dd"]),
                errors="coerce",
            )
            full_df = full_df.dropna(subset=[constants.TIME_INDEX])
            full_df[constants.GAUGE_ID] = full_df[constants.GAUGE_ID].astype(str)

            # Select and convert variables
            var_cols = [
                constants.DISCHARGE,
                constants.STAGE,
                constants.WATER_TEMPERATURE,
            ]
            for var in var_cols:
                if var in full_df.columns:
                    full_df[var] = pd.to_numeric(full_df[var], errors="coerce")

            if constants.STAGE in full_df.columns:
                full_df[constants.STAGE] = full_df[constants.STAGE] / 100.0  # cm to m

            # Clean placeholder values
            full_df.replace(
                {9999: None, 99999.999: None, 99.9: None, 999: None}, inplace=True
            )

            result_df = full_df[
                [constants.GAUGE_ID, constants.TIME_INDEX] + var_cols
            ].dropna(how="all", subset=var_cols)
            return result_df

        except Exception as e:
            logger.error(f"Error parsing all data: {e}")
            return pd.DataFrame()

    def _create_cache(self):
        """Downloads all data, processes it, and saves it to a zarr cache."""
        logger.info(f"Creating cache file at {self.CACHE_FILE}")
        start_year = 1951
        end_year = datetime.now().year
        raw_data_list = self._download_all_data(start_year, end_year)
        if not raw_data_list:
            logger.error("No data downloaded, cache creation failed.")
            return

        df = self._parse_all_data(raw_data_list)
        if df.empty:
            logger.error("No data parsed, cache creation failed.")
            return

        # Convert to xarray Dataset
        df = df.set_index([constants.GAUGE_ID, constants.TIME_INDEX]).sort_index()
        ds = df.to_xarray()

        # Save to zarr
        try:
            self.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            ds.to_zarr(self.CACHE_FILE, mode="w")
            logger.info(f"Successfully created cache file at {self.CACHE_FILE}")
        except Exception as e:
            logger.error(f"Error saving cache to zarr: {e}")

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses Polish river gauge data from cache or source."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        if not self.CACHE_FILE.exists():
            self._create_cache()

        if not self.CACHE_FILE.exists():
            logger.error("Cache file not found after creation attempt.")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            ds = xr.open_zarr(self.CACHE_FILE)
            if variable not in ds:
                logger.warning(f"Variable {variable} not found in cache.")
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            data_array = ds[variable].sel(
                gauge_id=gauge_id, time=slice(start_date, end_date)
            )
            df = (
                data_array.to_pandas()
                .dropna()
                .reset_index()
                .rename(columns={variable: variable})
            )
            return df[[constants.TIME_INDEX, variable]]

        except KeyError:
            logger.info(
                f"No data found for gauge {gauge_id} in the selected date range."
            )
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            logger.error("Error reading from cache")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def _download_data(
        self, gauge_id: str, variable: str, start_date: str, end_date: str
    ) -> any:
        """Not used for PolandFetcher, cache is created from all data."""
        raise NotImplementedError("This method is not used in PolandFetcher.")

    def _parse_data(self, gauge_id: str, raw_data: any, variable: str) -> pd.DataFrame:
        """Not used for PolandFetcher, cache is created from all data."""
        raise NotImplementedError("This method is not used in PolandFetcher.")


def _imgw_read(fpath: str) -> pd.DataFrame:
    """Helper function to read IMGW CSV files with various encodings and separators."""
    try:
        data = pd.read_csv(
            fpath, header=None, sep=",", encoding="cp1250", low_memory=False
        )
    except Exception:
        try:
            data = pd.read_csv(fpath, header=None, sep=";", low_memory=False)
        except Exception:
            data = pd.DataFrame()

    if data.empty or data.shape[1] == 1:
        try:
            data = pd.read_csv(
                fpath, header=None, sep=";", encoding="utf-8", low_memory=False
            )
        except Exception:
            try:
                data = pd.read_csv(fpath, header=None, sep=";", low_memory=False)
            except Exception:
                data = pd.DataFrame()

    if data.empty or data.shape[1] == 1:
        try:
            data = pd.read_csv(
                fpath, header=None, sep=",", encoding="cp1250", low_memory=False
            )
        except Exception:
            pass

    return data
