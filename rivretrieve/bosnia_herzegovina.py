"""Fetcher for Bosnia and Herzegovina river gauge data from vodostaji.voda.ba."""

import logging
from io import BytesIO
from typing import Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class BosniaHerzegovinaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the Federal Hydrometeorological Institute portal.

    Data source:
        - https://vodostaji.voda.ba/

    Supported variables:
        - constants.DISCHARGE_DAILY_MEAN (m³/s)
        - constants.DISCHARGE_INSTANT (m³/s)
        - constants.STAGE_DAILY_MEAN (m)
        - constants.STAGE_INSTANT (m)
        - constants.WATER_TEMPERATURE_DAILY_MEAN (°C)
        - constants.WATER_TEMPERATURE_INSTANT (°C)

    Data description and API:
        - live station metadata snapshot: https://vodostaji.voda.ba/data/internet/layers/20/index.json
        - annual station workbooks:
          https://vodostaji.voda.ba/data/internet/stations/<group>/<gauge_id>/<parameter>/<file>

    Terms of use:
        - see https://vodostaji.voda.ba/
    """

    METADATA_URL = "https://vodostaji.voda.ba/data/internet/layers/20/index.json"
    STATION_GROUPS = tuple(range(1, 11))
    SOURCE = "vodostaji.voda.ba"
    COUNTRY = "Bosnia and Herzegovina"
    VARIABLE_MAP = {
        constants.DISCHARGE_INSTANT: {"code": "Q", "file": "Q_1Y.xlsx", "column": constants.DISCHARGE_INSTANT},
        constants.DISCHARGE_DAILY_MEAN: {
            "code": "Q",
            "file": "Q_1Y.xlsx",
            "column": constants.DISCHARGE_DAILY_MEAN,
        },
        constants.STAGE_INSTANT: {"code": "H", "file": "H_1Y.xlsx", "column": constants.STAGE_INSTANT},
        constants.STAGE_DAILY_MEAN: {"code": "H", "file": "H_1Y.xlsx", "column": constants.STAGE_DAILY_MEAN},
        constants.WATER_TEMPERATURE_INSTANT: {
            "code": "WT",
            "file": "Tvode_1Y.xlsx",
            "column": constants.WATER_TEMPERATURE_INSTANT,
        },
        constants.WATER_TEMPERATURE_DAILY_MEAN: {
            "code": "WT",
            "file": "Tvode_1Y.xlsx",
            "column": constants.WATER_TEMPERATURE_DAILY_MEAN,
        },
    }

    @staticmethod
    def _empty_result(variable: str) -> pd.DataFrame:
        """Returns an empty standardized RivRetrieve time series frame."""
        return pd.DataFrame(columns=[constants.TIME_INDEX, variable]).set_index(constants.TIME_INDEX)

    @staticmethod
    def _empty_metadata() -> pd.DataFrame:
        """Returns an empty metadata frame indexed by gauge ID."""
        return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Bosnia and Herzegovina gauge metadata."""
        return utils.load_cached_metadata_csv("bosnia_herzegovina")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(BosniaHerzegovinaFetcher.VARIABLE_MAP.keys())

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and normalizes station metadata from the live JSON snapshot.

        Keeps provider-specific metadata columns, standardizes the key RivRetrieve
        metadata fields, and returns a DataFrame indexed by ``constants.GAUGE_ID``.
        """
        session = utils.requests_retry_session()

        try:
            response = session.get(self.METADATA_URL, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Bosnia and Herzegovina metadata: {exc}")
            raise
        except ValueError as exc:
            logger.error(f"Failed to decode Bosnia and Herzegovina metadata: {exc}")
            raise

        if not isinstance(data, list) or not data:
            return self._empty_metadata()

        df = pd.json_normalize(data)
        rename_map = {
            "metadata_station_no": constants.GAUGE_ID,
            "metadata_station_name": constants.STATION_NAME,
            "metadata_river_name": constants.RIVER,
            "metadata_catchment_name": "catchment",
            "metadata_station_latitude": constants.LATITUDE,
            "metadata_station_longitude": constants.LONGITUDE,
            "metadata_station_elevation": constants.ALTITUDE,
            "metadata_CATCHMENT_SIZE": constants.AREA,
        }
        df = df.rename(columns=rename_map)

        numeric_cols = [
            constants.LATITUDE,
            constants.LONGITUDE,
            constants.ALTITUDE,
            "metadata_station_carteasting",
            "metadata_station_cartnorthing",
            "metadata_station_local_x",
            "metadata_station_local_y",
        ]
        for column in numeric_cols:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        if constants.AREA in df.columns:
            df["catchment_area_km2"] = (
                df[constants.AREA].astype(str).str.replace("km²", "", regex=False).str.strip().replace({"": np.nan})
            )
            df["catchment_area_km2"] = pd.to_numeric(df["catchment_area_km2"], errors="coerce")
            df[constants.AREA] = df["catchment_area_km2"]
        else:
            df[constants.AREA] = np.nan

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

        return df.reset_index(drop=True).set_index(constants.GAUGE_ID)

    def _download_data(
        self, gauge_id: str, variable: str, start_date: str, end_date: str
    ) -> tuple[Optional[bytes], Optional[int]]:
        """Downloads raw Excel bytes from the endpoint download URLs."""
        del start_date, end_date

        config = self.VARIABLE_MAP[variable]
        session = utils.requests_retry_session()

        for group in self.STATION_GROUPS:
            url = (
                f"https://vodostaji.voda.ba/data/internet/stations/{group}/{gauge_id}/{config['code']}/{config['file']}"
            )
            try:
                response = session.get(url, timeout=20)
                if response.status_code == 200 and len(response.content) > 0:
                    return response.content, group
            except requests.exceptions.RequestException:
                continue

        return None, None

    def _parse_data(
        self, gauge_id: str, raw_data: tuple[Optional[bytes], Optional[int]], variable: str
    ) -> pd.DataFrame:
        """Parses the Excel bytes into the standard RivRetrieve data frame layout."""
        content, station_group = raw_data
        if not content:
            return self._empty_result(variable)

        try:
            df = pd.read_excel(
                BytesIO(content),
                skiprows=8,
                header=None,
                names=[constants.TIME_INDEX, variable],
            )
        except Exception as exc:
            logger.error(f"Failed to parse Bosnia and Herzegovina data for {gauge_id}: {exc}")
            return self._empty_result(variable)

        if df.empty:
            return self._empty_result(variable)

        df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], dayfirst=True, errors="coerce")
        df[variable] = pd.to_numeric(df[variable], errors="coerce")
        df = df.dropna(subset=[constants.TIME_INDEX, variable])

        if variable in {constants.STAGE_DAILY_MEAN, constants.STAGE_INSTANT}:
            df[variable] = df[variable] / 100.0

        if variable in {
            constants.DISCHARGE_DAILY_MEAN,
            constants.STAGE_DAILY_MEAN,
            constants.WATER_TEMPERATURE_DAILY_MEAN,
        }:
            df = df.set_index(constants.TIME_INDEX).resample("D").mean().dropna().reset_index()

        df = df.drop_duplicates(subset=constants.TIME_INDEX, keep="first").sort_values(constants.TIME_INDEX)
        df = df.set_index(constants.TIME_INDEX)
        df.attrs["station_group"] = station_group
        df.attrs["station_id"] = gauge_id
        df.attrs["variable"] = variable
        return df

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable.

        This method retrieves the requested data from the provider's workbook endpoint,
        parses it, and returns it in a standardized pandas DataFrame format.

        Args:
            gauge_id: The site-specific identifier for the gauge.
            variable: The variable to fetch. Must be one of the strings listed
                in the fetcher's ``get_available_variables()`` output.
                These are typically defined in ``rivretrieve.constants``.
            start_date: Optional start date for the data retrieval in 'YYYY-MM-DD' format.
                If None, data is fetched from the earliest available date in the workbook.
            end_date: Optional end date for the data retrieval in 'YYYY-MM-DD' format.
                If None, data is fetched up to the latest available date in the workbook.

        Returns:
            pd.DataFrame: A pandas DataFrame indexed by datetime objects (``constants.TIME_INDEX``)
            with a single column named after the requested ``variable``. The DataFrame
            will be empty if no data is found for the given parameters.

        Raises:
            ValueError: If the requested ``variable`` is not supported by this fetcher.
        """
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)
        except Exception as exc:
            logger.error(f"Failed to fetch Bosnia and Herzegovina data for {gauge_id} ({variable}): {exc}")
            return self._empty_result(variable)

        if df.empty:
            return self._empty_result(variable)

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1)
        return df[(df.index >= start_dt) & (df.index < end_dt)]
