"""Fetcher for UK river gauge data."""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class UKEAFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the UK Environment Agency (EA).

    Data Source: Environment Agency Hydrology API (https://environment.data.gov.uk/hydrology/)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_INSTANT`` (m)
    """

    BASE_URL = "http://environment.data.gov.uk"

    METADATA_TRANSLATION_MAPPING = {
        "notation": constants.GAUGE_ID,
        "stationReference": "stationReference",
        "label": constants.STATION_NAME,
        "lat": constants.LATITUDE,
        "long": constants.LONGITUDE,
        "riverName": constants.RIVER,
        "catchmentArea": constants.AREA,
    }

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available UK Environment Agency gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("uk_ea")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE_DAILY_MEAN, constants.STAGE_INSTANT)

    def get_metadata(self) -> pd.DataFrame:
        """Fetches site metadata for all stations from the EA API.

        Data is fetched from:
        ``http://environment.data.gov.uk/hydrology/id/stations.json``

        Returns:
            A pandas DataFrame indexed by gauge_id, containing site metadata.
        """
        params = {"_limit": 10000}
        url = f"{self.BASE_URL}/hydrology/id/stations.json"
        s = utils.requests_retry_session()
        try:
            response = s.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data.get("items", []))

            if df.empty:
                return pd.DataFrame().set_index(constants.GAUGE_ID)

            df = df.rename(columns=self.METADATA_TRANSLATION_MAPPING)

            return df.set_index(constants.GAUGE_ID)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching EA stations list: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing EA stations list: {e}")
            raise

    def _get_measure_notation(self, variable: str) -> str:
        """Gets the notation for the given variable."""
        if variable == constants.STAGE_INSTANT:
            return "level-i-900-m-qualified"
        elif variable == constants.DISCHARGE_DAILY_MEAN:
            return "flow-m-86400-m3s-qualified"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Downloads the raw data from the UK Environment Agency API."""
        notation = self._get_measure_notation(variable)

        # Check if the station has data for the given variable
        measure_url = f"{self.BASE_URL}/hydrology/id/measures?station={gauge_id}"
        try:
            r = utils.requests_retry_session().get(measure_url)
            r.raise_for_status()
            measures = r.json()["items"]
            ix = next(
                (i for i, item in enumerate(measures) if notation in item["notation"]),
                None,
            )
            if ix is None:
                raise ValueError(f"Site {gauge_id} does not have {variable} data ({notation})")
            target_notation = measures[ix]["notation"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching measures for site {gauge_id}: {e}")
            raise

        all_items = []
        current_start_date = start_date
        limit = 2000000  # API limit

        while True:
            api_url = (
                f"{self.BASE_URL}/hydrology/id/measures/{target_notation}/readings"
                f"?mineq-date={current_start_date}&maxeq-date={end_date}&_limit={limit}"
            )
            try:
                r = utils.requests_retry_session().get(api_url)
                r.raise_for_status()
                data = r.json()
                items = data.get("items", [])
                all_items.extend(items)

                if len(items) < limit:
                    break
                else:
                    # Prepare for the next chunk
                    last_datetime_str = items[-1]["dateTime"]
                    last_date = datetime.fromisoformat(last_datetime_str.replace("Z", "+00:00")).date()
                    current_start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                    if current_start_date > end_date:
                        break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from {api_url}: {e}")
                raise
            except ValueError as e:
                logger.error(f"Error decoding JSON from {api_url}: {e}")
                raise

        return all_items

    def _parse_data(self, raw_data: List[Dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses the raw JSON data into a pandas DataFrame."""
        if not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = pd.DataFrame(raw_data)
        df[constants.TIME_INDEX] = pd.to_datetime(df["dateTime"]).dt.date
        df["Value"] = pd.to_numeric(df["value"], errors="coerce")

        df = df[[constants.TIME_INDEX, "Value"]]

        df = df.rename(columns={"Value": variable})
        df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX])

        # Ensure complete time series within the data range
        if not df.empty:
            date_range = pd.date_range(
                start=df[constants.TIME_INDEX].min(),
                end=df[constants.TIME_INDEX].max(),
                freq="D",
            )
            complete_ts = pd.DataFrame(date_range, columns=[constants.TIME_INDEX])
            df = pd.merge(complete_ts, df, on=constants.TIME_INDEX, how="left")

        return df.set_index(constants.TIME_INDEX)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable.

        This method retrieves the requested data from the provider's API or data source,
        parses it, and returns it in a standardized pandas DataFrame format.

        Args:
            gauge_id: The site-specific identifier for the gauge.
            variable: The variable to fetch. Must be one of the strings listed
                in the fetcher's ``get_available_variables()`` output.
                These are typically defined in ``rivretrieve.constants``.
            start_date: Optional start date for the data retrieval in 'YYYY-MM-DD' format.
                If None, data is fetched from the earliest available date.
            end_date: Optional end date for the data retrieval in 'YYYY-MM-DD' format.
                If None, data is fetched up to the latest available date.

        Returns:
            pd.DataFrame: A pandas DataFrame indexed by datetime objects (``constants.TIME_INDEX``)
            with a single column named after the requested ``variable``. The DataFrame
            will be empty if no data is found for the given parameters.

        Raises:
            ValueError: If the requested ``variable`` is not supported by this fetcher.
            requests.exceptions.RequestException: If a network error occurs during data download.
            Exception: For other unexpected errors during data fetching or parsing.
        """
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(raw_data, variable)

            # Filter by exact start and end date after processing
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
            return df

        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
