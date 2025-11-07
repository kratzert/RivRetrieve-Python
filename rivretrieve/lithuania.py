"""Fetcher for Lithuanian river gauge data from Meteo.lt."""

import logging
import time
from collections import deque
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class LithuaniaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Lithuania's meteorological service (Meteo.lt).

    Data Source: Meteo.lt API https://api.meteo.lt/v1/

    Supported Variables:
        - constants.DISCHARGE_DAILY_MEAN (m³/s)
        - constants.STAGE_DAILY_MEAN (m)

    Terms of Use:
        - For the license of the data and the terms of use, see https://api.meteo.lt/
    """

    METADATA_URL = "https://api.meteo.lt/v1/hydro-stations"
    DATA_URL_TEMPLATE = "https://api.meteo.lt/v1/hydro-stations/{}/observations/historical/{}"

    # To comply with the 180 requests/minute limit
    request_times: deque[float] = deque(maxlen=180)

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached metadata (if available)."""
        return utils.load_cached_metadata_csv("lithuania")

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and parses site metadata from Meteo.lt."""
        headers = {
            "Accept": "application/json",
        }
        s = utils.requests_retry_session()
        try:
            logger.info(f"Fetching Lithuania metadata from {self.METADATA_URL}")
            resp = s.get(self.METADATA_URL, headers=headers, timeout=20)
            resp.raise_for_status()
            stations = resp.json()

            df = pd.json_normalize(stations)

            rename_map = {
                "code": constants.GAUGE_ID,
                "name": constants.STATION_NAME,
                "waterBody": constants.RIVER,
                "coordinates.latitude": constants.LATITUDE,
                "coordinates.longitude": constants.LONGITUDE,
            }
            df = df.rename(columns=rename_map)

            df[constants.ALTITUDE] = None
            df[constants.AREA] = None
            df[constants.COUNTRY] = "Lithuania"
            df[constants.SOURCE] = "Meteo.lt"

            df = df[
                [
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
            ]

            df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
            df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
            df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")

            logger.info(f"Fetched {len(df)} Lithuania gauge metadata records.")
            return df.set_index(constants.GAUGE_ID)

        except Exception as e:
            logger.error(f"Failed to fetch Meteo.lt metadata: {e}")
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE_DAILY_MEAN, constants.STAGE_DAILY_MEAN)

    def _get_api_variable(self, variable: str) -> str:
        if variable == constants.DISCHARGE_DAILY_MEAN:
            return "waterDischarge"
        elif variable == constants.STAGE_DAILY_MEAN:
            return "waterLevel"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _throttle_requests(self):
        """Ensures API rate limit is not exceeded."""
        now = time.time()
        while self.request_times and now - self.request_times[0] > 60:
            self.request_times.popleft()

        if len(self.request_times) >= 180:
            wait_time = 60 - (now - self.request_times[0]) + 0.1  # Add a small buffer
            logger.info(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time)
            # Clear outdated timestamps after sleep
            now = time.time()
            while self.request_times and now - self.request_times[0] > 60:
                self.request_times.popleft()
        self.request_times.append(time.time())

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """Downloads raw data in monthly chunks from the Meteo.lt API."""
        s = utils.requests_retry_session()
        headers = {
            "Accept": "application/json",
        }
        all_data = []

        date_range = pd.date_range(start=start_date, end=end_date, freq="MS")

        for dt in date_range:
            date_str = dt.strftime("%Y-%m")
            url = self.DATA_URL_TEMPLATE.format(gauge_id, date_str)

            self._throttle_requests()

            logger.debug(f"Fetching {gauge_id} ({variable}) - {date_str} from {url}")
            try:
                r = s.get(url, headers=headers, timeout=20)

                if r.status_code == 404:
                    logger.debug(f"No data for {gauge_id} for {date_str}")
                    continue
                r.raise_for_status()

                js = r.json()
                obs = js.get("observations", [])
                if obs:
                    all_data.extend(obs)
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download {gauge_id} for {date_str}: {e}")
            except Exception as e:
                logger.error(f"Error processing response for {gauge_id} {date_str}: {e}")

        return all_data

    def _parse_data(self, gauge_id: str, raw_data: List[Dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses the raw JSON data."""
        if not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        api_variable = self._get_api_variable(variable)
        try:
            df = pd.DataFrame(raw_data)
            if df.empty or "observationDateUtc" not in df.columns or api_variable not in df.columns:
                logger.warning(f"Missing expected columns for site {gauge_id}")
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            df[constants.TIME_INDEX] = pd.to_datetime(df["observationDateUtc"], errors="coerce").dt.tz_localize("UTC")
            df[variable] = pd.to_numeric(df[api_variable], errors="coerce")

            # Unit conversion
            if variable == constants.STAGE_DAILY_MEAN:
                df[variable] = df[variable] / 100.0  # cm to m

            df = df.dropna(subset=[constants.TIME_INDEX, variable])

            if df.empty:
                logger.warning(f"DataFrame empty after dropna for {gauge_id} {variable}")
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            return (
                df[[constants.TIME_INDEX, variable]]
                .sort_values(by=constants.TIME_INDEX)
                .set_index(constants.TIME_INDEX)
            )
        except Exception as e:
            logger.error(f"Error parsing JSON data for site {gauge_id}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

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

            if df.empty:
                logger.debug(f"Parsed DataFrame is empty for {gauge_id} {variable}")
                return df

            # Filter to the exact date range
            start_date_dt = pd.to_datetime(start_date).tz_localize("UTC")
            end_date_dt = pd.to_datetime(end_date).tz_localize("UTC") + pd.Timedelta(days=1)

            df_filtered = df[(df.index >= start_date_dt) & (df.index < end_date_dt)]
            return df_filtered
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
