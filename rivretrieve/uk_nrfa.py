"""Fetcher for UK National River Flow Archive (NRFA) data."""

import logging
from typing import Any, Dict, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class UKNRFAFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the UK National River Flow Archive."""

    BASE_URL = "https://nrfaapps.ceh.ac.uk/nrfa/ws"
    GAUGE_ID_COL = "id"

    @staticmethod
    def get_gauge_ids() -> pd.DataFrame:
        """Retrieves a DataFrame of available NRFA gauge IDs from the cached CSV."""
        return utils.load_sites_csv("uk_nrfa")

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata from the NRFA API."""
        query_params = {"station": "*", "format": "json-object", "fields": "all"}
        try:
            s = utils.requests_retry_session()
            response = s.get(f"{UKNRFAFetcher.BASE_URL}/station-info", params=query_params)
            response.raise_for_status()  # raises an error for non-200 responses
            data = response.json()
            df = pd.DataFrame(data["data"])
            # Rename id column to the standard GAUGE_ID
            df = df.rename(columns={UKNRFAFetcher.GAUGE_ID_COL: constants.GAUGE_ID})
            df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str)
            return df.set_index(constants.GAUGE_ID)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching NRFA catalogue: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing NRFA catalogue: {e}")
            raise

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        # Based on common NRFA data types, can be expanded
        return (constants.DISCHARGE,)

    def _get_nrfa_data_type(self, variable: str) -> str:
        if variable == constants.DISCHARGE:
            return "gdf"  # Mean daily flow
        # TODO: Map other variables like STAGE if available
        else:
            raise ValueError(f"Unsupported variable: {variable} for NRFA")

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
        """Downloads the raw time series data from the NRFA API."""
        data_type = self._get_nrfa_data_type(variable)
        query_params = {
            "station": str(gauge_id),
            "data-type": data_type,
            "format": "json-object",
            "start-date": f"{start_date}T00:00:00Z",
            "end-date": f"{end_date}T23:59:59Z",
        }
        s = utils.requests_retry_session()
        try:
            response = s.get(f"{self.BASE_URL}/time-series", params=query_params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching NRFA time series for {gauge_id} ({data_type}): {e}")
            return None

    def _parse_data(self, gauge_id: str, raw_data: Optional[Dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses the raw JSON time series data."""
        if not raw_data or "data-stream" not in raw_data or not raw_data["data-stream"]:
            logger.warning(f"No data stream found for {gauge_id}, variable {variable}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            dates = raw_data["data-stream"][0::2]
            values = raw_data["data-stream"][1::2]
            df = pd.DataFrame.from_dict({"time": dates, variable: values})
            df[constants.TIME_INDEX] = pd.to_datetime(df["time"], format="ISO8601").dt.date
            df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX])
            df[variable] = pd.to_numeric(df[variable], errors="coerce")
            return df[[constants.TIME_INDEX, variable]].dropna().reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error parsing NRFA data for {gauge_id}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses UK NRFA river gauge data."""
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)

            # Filter by date range
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[(df[constants.TIME_INDEX] >= start_date_dt) & (df[constants.TIME_INDEX] <= end_date_dt)]
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
