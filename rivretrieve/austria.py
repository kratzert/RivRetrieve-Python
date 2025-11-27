"""Fetcher for Austrian river gauge data from eHYD."""

import io
import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
import xarray as xr
from pyproj import Transformer

from . import base, constants, utils

logger = logging.getLogger(__name__)


class AustriaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Hydrographische Archivdaten Österreichs (eHYD).

    Data source:
        https://ehyd.gv.at/

    Supported variables:
        - constants.DISCHARGE_DAILY_MEAN (m³/s)
        - constants.STAGE_DAILY_MEAN (m)
        - constants.DISCHARGE_MONTHLY_MAX (m³/s)
        - constants.DISCHARGE_MONTHLY_MIN (m³/s)
        - constants.STAGE_MONTHLY_MAX (m)
        - constants.STAGE_MONTHLY_MIN (m)
        - constants.WATER_TEMPERATURE_MONTHLY_MEAN (°C)

    Terms of use:
        - see impressum at: https://ehyd.gv.at/
    """
    
    BULK_URL = "https://ehyd.gv.at/eHYD/AreaSelection/download?cat=owf&reg=10"
    CACHE_FILE = Path(os.path.dirname(__file__)) / "data" / "austria.zarr"
    METADATA_CSV = Path(os.path.dirname(__file__)) / "cached_site_data" / "austria_sites.csv"

    VARIABLE_FOLDERS = {
        constants.DISCHARGE_DAILY_MEAN: "Q-Tagesmittel",
        constants.STAGE_DAILY_MEAN: "W-Tagesmittel",
        constants.DISCHARGE_MONTHLY_MAX: "Q-Monatsmaxima",
        constants.DISCHARGE_MONTHLY_MIN: "Q-Monatsminima",
        constants.STAGE_MONTHLY_MAX: "W-Monatsmaxima",
        constants.STAGE_MONTHLY_MIN: "W-Monatsminima",
        constants.WATER_TEMPERATURE_MONTHLY_MEAN: "WT-Monatsmittel",
    }

    VARIABLE_API_MAP = {
        constants.DISCHARGE_DAILY_MEAN: "streamflow_daily",
        constants.STAGE_DAILY_MEAN: "stage_daily",
        constants.DISCHARGE_MONTHLY_MAX: "streamflow_monthly_max",
        constants.DISCHARGE_MONTHLY_MIN: "streamflow_monthly_min",
        constants.STAGE_MONTHLY_MAX: "stage_monthly_max",
        constants.STAGE_MONTHLY_MIN: "stage_monthly_min",
        constants.WATER_TEMPERATURE_MONTHLY_MEAN: "temperature_monthly_mean",
    }

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(AustriaFetcher.VARIABLE_FOLDERS.keys())

    @staticmethod
    def _convert_coordinates(df: pd.DataFrame) -> pd.DataFrame:
        transformer = Transformer.from_crs("EPSG:31287", "EPSG:4326", always_xy=True)
        lon, lat = transformer.transform(df["xrkko08"].astype(float), df["yhkko09"].astype(float))
        df["latitude"], df["longitude"] = lat, lon
        return df

    @staticmethod
    def get_metadata(force_download: bool = False) -> pd.DataFrame:
        if AustriaFetcher.METADATA_CSV.exists() and not force_download:
            return pd.read_csv(AustriaFetcher.METADATA_CSV, index_col=constants.GAUGE_ID)

        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, "ehyd_all.zip")
            r = utils.requests_retry_session().get(AustriaFetcher.BULK_URL, timeout=180)
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                f.write(r.content)
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_name = [n for n in zf.namelist() if n.endswith("messstellen_owf.csv")][0]
                with zf.open(csv_name) as f:
                    df = pd.read_csv(f, sep=";", dtype=str, encoding="latin1")

        df = df.rename(
            columns={
                "hzbnr01": constants.GAUGE_ID,
                "mstnam02": constants.STATION_NAME,
                "gew03": constants.RIVER,
                "mpua04": constants.ALTITUDE,
                "egarea05": constants.AREA,
                "xrkko08": "xrkko08",
                "yhkko09": "yhkko09",
            }
        )
        for c in [constants.ALTITUDE, constants.AREA, "xrkko08", "yhkko09"]:
            df[c] = pd.to_numeric(df[c].str.replace(",", "."), errors="coerce")
        df = AustriaFetcher._convert_coordinates(df)
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str)
        cols = [
            constants.GAUGE_ID,
            constants.STATION_NAME,
            constants.RIVER,
            constants.ALTITUDE,
            constants.AREA,
            "xrkko08",
            "yhkko09",
            "latitude",
            "longitude",
        ]
        df = df[cols]
        AustriaFetcher.METADATA_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(AustriaFetcher.METADATA_CSV)
        return df.set_index(constants.GAUGE_ID)

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        return utils.load_cached_metadata_csv("austria")

    def _download_all_data(self) -> Path:
        tmpdir = tempfile.mkdtemp()
        zip_path = os.path.join(tmpdir, "ehyd_all.zip")
        r = utils.requests_retry_session().get(self.BULK_URL, timeout=180)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(r.content)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmpdir)
        return Path(tmpdir)

    def _parse_bulk_file(self, file_path: Path, variable: str) -> pd.DataFrame:
        text = file_path.read_text(encoding="latin1")
        lines = text.splitlines()
        data_start = next((i for i, l in enumerate(lines) if l.strip().startswith("Werte")), None)
        if data_start is None:
            return pd.DataFrame()
        cleaned = []
        for line in lines[data_start + 1 :]:
            parts = re.split(r"[;\t\s]+", line.strip(), maxsplit=2)
            if len(parts) >= 2:
                date_str, val_str = parts[0], parts[-1].replace(",", ".")
                cleaned.append((date_str, val_str))
        df = pd.DataFrame(cleaned, columns=["time", variable])
        df["time"] = pd.to_datetime(df["time"], errors="coerce", format="%d.%m.%Y")
        df[variable] = pd.to_numeric(df[variable], errors="coerce")
        df[constants.GAUGE_ID] = re.search(r"(\d+)\.csv$", file_path.name).group(1)
        return df.dropna()

    def _create_cache(self):
        extracted_path = self._download_all_data()
        dfs = []
        for var, folder in self.VARIABLE_FOLDERS.items():
            folder_path = extracted_path / folder
            if not folder_path.exists():
                continue
            for f in folder_path.glob("*.csv"):
                df = self._parse_bulk_file(f, var)
                if not df.empty:
                    dfs.append(df)
        if not dfs:
            return
        # Combine all variable DataFrames
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.rename(columns={"time": constants.TIME_INDEX})

        # Pivot to ensure each (gauge_id, time) is unique, with variables as columns
        full_df = full_df.pivot_table(
            index=[constants.GAUGE_ID, constants.TIME_INDEX],
            aggfunc="first"
        ).sort_index()

        # Convert to xarray Dataset
        ds = full_df.to_xarray()

        # Save to cache
        self.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        ds.to_zarr(self.CACHE_FILE, mode="w")


    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        variable = self.VARIABLE_API_MAP[variable].lower().strip()
        file_map = {
            "streamflow_daily": [4, 5],
            "stage_daily": [1, 2],
            "streamflow_monthly_max": [7],
            "streamflow_monthly_min": [6],
            "stage_monthly_max": [4],
            "stage_monthly_min": [3],
            "temperature_monthly_mean": [9, 2, 5, 8],
        }
        expected_signatures = {
            "streamflow_daily": "Q-Tagesmittel",
            "stage_daily": "W-Tagesmittel",
            "streamflow_monthly_max": "Q-Monatsmaxima",
            "streamflow_monthly_min": "Q-Monatsminima",
            "stage_monthly_max": "W-Monatsmaxima",
            "stage_monthly_min": "W-Monatsminima",
            "temperature_monthly_mean": "WT-Monatsmittel",
        }
        response = None
        for file_num in file_map[variable]:
            url = f"https://ehyd.gv.at/eHYD/MessstellenExtraData/owf?id={gauge_id}&file={file_num}"
            try:
                r = requests.get(url, timeout=30)
                if r.status_code != 200:
                    continue
                if expected_signatures[variable] in r.headers.get("Content-Disposition", ""):
                    response = r
                    break
            except Exception:
                continue
        if response is None:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
        text = response.text
        lines = text.splitlines()
        data_start = next((i for i, l in enumerate(lines) if l.strip().startswith("Werte")), None)
        if data_start is None:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
        cleaned = []
        for line in lines[data_start + 1 :]:
            parts = re.split(r"[;\t\s]+", line.strip(), maxsplit=2)
            if len(parts) >= 2:
                date_str = parts[0]
                time_str = parts[1] if re.match(r"^\d{2}:\d{2}:\d{2}$", parts[1]) else "00:00:00"
                val_str = parts[-1].replace(",", ".")
                cleaned.append((f"{date_str} {time_str}", val_str))
        df = pd.DataFrame(cleaned, columns=["time", variable])
        df["time"] = pd.to_datetime(df["time"], errors="coerce", format="%d.%m.%Y %H:%M:%S")
        df[variable] = pd.to_numeric(df[variable], errors="coerce")
        if start_date:
            df = df[df["time"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["time"] <= pd.to_datetime(end_date)]
        df = df.rename(columns={"time": constants.TIME_INDEX})
        return df.set_index(constants.TIME_INDEX)[[variable]]

    def _parse_data(self, gauge_id: str, raw_data: Optional[pd.DataFrame], variable: str) -> pd.DataFrame:
        return raw_data if raw_data is not None else pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch and parse eHYD time series data for Austria.

        Handles both:
          - Single-station direct download (via API)
          - Full bulk dataset from cache if gauge_id="all"
        """
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")
        
        if str(gauge_id).lower() == "all":
            if not self.CACHE_FILE.exists():
                logger.info("Austria cache not found — downloading full eHYD dataset...")
                self._create_cache()

            try:
                ds = xr.open_zarr(self.CACHE_FILE)
                if variable not in ds:
                    logger.warning(f"Variable {variable} not found in Austria cache.")
                    return pd.DataFrame()

                data_array = ds[variable].sel(time=slice(start_date, end_date))
                df = data_array.to_pandas()

                # Fix orientation: make time the index, gauge_id a column
                if df.index.name == constants.GAUGE_ID and isinstance(df.columns, pd.DatetimeIndex):
                    df = df.T  # transpose to get time as index
                    df.index.name = constants.TIME_INDEX
                    df.columns.name = constants.GAUGE_ID

                # Convert from wide to long format
                df = df.stack().reset_index()
                df.columns = [constants.TIME_INDEX, constants.GAUGE_ID, variable]

                # Filter and clean
                df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], errors="coerce")
                df = df.dropna(subset=[constants.TIME_INDEX, variable])
                df = df.sort_values([constants.GAUGE_ID, constants.TIME_INDEX])

                return df.set_index(constants.TIME_INDEX)[[constants.GAUGE_ID, variable]]

            except Exception as e:
                logger.error(f"Error reading Austria cache: {e}")
                return pd.DataFrame(columns=[constants.TIME_INDEX, constants.GAUGE_ID, variable])

        # Case 2: single station (live API download)
        raw = self._download_data(gauge_id, variable, start_date, end_date)
        df = self._parse_data(gauge_id, raw, variable)

        # The downloaded data uses internal variable names (e.g. streamflow_daily)
        # Rename to the RivRetrieve standardized variable (e.g. discharge_daily_mean)
        internal_var = self.VARIABLE_API_MAP[variable]
        if internal_var in df.columns and internal_var != variable:
            df = df.rename(columns={internal_var: variable})

        # Case: single station — return only time and variable (no gauge_id)
        if df.empty:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = df.reset_index()[[constants.TIME_INDEX, variable]].set_index(constants.TIME_INDEX)
        return df


