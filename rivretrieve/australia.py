"""Fetcher for Australian river gauge data from BoM."""

import json
import logging
from io import StringIO
from typing import Any, Dict, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class AustraliaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Australia's BoM."""

    BOM_URL = "http://www.bom.gov.au/waterdata/services"

    @staticmethod
    def get_gauge_ids() -> pd.DataFrame:
        """Retrieves a DataFrame of available Australian gauge IDs."""
        return utils.load_sites_csv("australia")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE, constants.STAGE)

    def _make_bom_request(self, params: Dict[str, Any]) -> Any:
        """Helper function to make requests to the BoM API."""
        base_params = {
            "service": "kisters",
            "type": "QueryServices",
        }
        all_params = {**base_params, **params}
        s = utils.requests_retry_session()
        try:
            response = s.get(self.BOM_URL, params=all_params)
            response.raise_for_status()
            if params.get("format") == "csv":
                return response.text
            # Default format is json
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"BoM API request failed for params {params}: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"BoM API JSON decode failed for params {params}: {e}\nResponse: {response.text}")
            raise

    def _get_timeseries_id(self, gauge_id: str, variable: str) -> Optional[str]:
        """Retrieves the timeseries ID for the given site and variable."""
        if variable == constants.STAGE:
            bom_variable = "Water Course Level"
            # ts_name = "H.Merged.DailyMean"
        elif variable == constants.DISCHARGE:
            bom_variable = "Water Course Discharge"
            # ts_name = "Q.Merged.DailyMean"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

        ts_name = "DMQaQc.Merged.DailyMean.24HR"  # Daily Mean Quality Controlled

        params = {
            "request": "getTimeseriesList",
            "parametertype_name": bom_variable,
            "ts_name": ts_name,
            "station_no": gauge_id,
            "format": "json",
        }
        try:
            json_data = self._make_bom_request(params)
            if isinstance(json_data, list) and len(json_data) > 1:
                # Response is a list of lists, header is index 0
                header = json_data[0]
                data = json_data[1:]
                df = pd.DataFrame(data, columns=header)
                if not df.empty and "ts_id" in df.columns:
                    return df["ts_id"].iloc[0]
                else:
                    logger.warning(f"No ts_id found for site {gauge_id}, variable {variable}")
                    return None
            elif isinstance(json_data, list) and len(json_data) == 1 and json_data[0] == "No matches.":
                logger.warning(f"No matches for site {gauge_id}, variable {variable} in getTimeseriesList")
                return None
            else:
                logger.warning(f"Unexpected response from getTimeseriesList for site {gauge_id}: {json_data}")
                return None
        except Exception as e:
            logger.error(f"Error getting timeseries ID for site {gauge_id}: {e}")
            return None

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> Optional[str]:
        """Downloads the raw CSV data."""
        ts_id = self._get_timeseries_id(gauge_id, variable)
        if not ts_id:
            return None

        params = {
            "request": "getTimeseriesValues",
            "datasource": "0",
            "format": "csv",
            "ts_id": ts_id,
            "from": f"{start_date}T00:00:00.000",
            "to": f"{end_date}T00:00:00.000",
            "returnfields": "Timestamp,Value,Quality Code,Interpolation Type",
            "metadata": "true",
        }
        try:
            csv_data = self._make_bom_request(params)
            return csv_data
        except Exception as e:
            logger.error(f"Error downloading data for ts_id {ts_id}: {e}")
            return None

    def _parse_data(self, gauge_id: str, raw_data: Optional[str], variable: str) -> pd.DataFrame:
        """Parses the raw CSV data."""
        if not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            lines = raw_data.strip().split("\n")
            header_line_index = -1
            header_line = ""
            for i, line in enumerate(lines):
                if line.startswith("#Timestamp;Value;Quality Code"):
                    header_line_index = i
                    header_line = line.lstrip("#")
                    break

            if header_line_index == -1:
                logger.warning(f"Could not find data header in CSV for site {gauge_id}")
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            # Join lines from the header row onwards
            csv_content = "\n".join(lines[header_line_index:])
            # Replace the commented header with the uncommented version
            csv_content = csv_content.replace(lines[header_line_index], header_line)

            csv_io = StringIO(csv_content)
            df = pd.read_csv(csv_io, sep=";")

            if df.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            df[constants.TIME_INDEX] = pd.to_datetime(df["Timestamp"]).dt.date
            df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
            df = df.rename(columns={"Value": variable})
            df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX])
            return df[[constants.TIME_INDEX, variable]].dropna()
        except Exception as e:
            logger.error(f"Error parsing CSV data for site {gauge_id}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses Australian river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
