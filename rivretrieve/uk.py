"""Fetcher for UK river gauge data."""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class UKFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the UK Environment Agency."""

    BASE_URL = "http://environment.data.gov.uk"

    @staticmethod
    def get_gauge_ids() -> pd.DataFrame:
        """Retrieves a DataFrame of available UK gauge IDs."""
        return utils.load_sites_csv("uk")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE, constants.STAGE)

    def _get_measure_notation(self, variable: str) -> str:
        """Gets the notation for the given variable."""
        if variable == constants.STAGE:
            return "level-i-900-m-qualified"
        elif variable == constants.DISCHARGE:
            return "flow-m-86400-m3s-qualified"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Downloads the raw data from the UK Environment Agency API."""
        notation = self._get_measure_notation(variable)
        site = gauge_id.split("/")[-1]

        # Check if the station has data for the given variable
        measure_url = f"{self.BASE_URL}/hydrology/id/measures?station={site}"
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

    def _parse_data(self, gauge_id: str, raw_data: List[Dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses the raw JSON data into a pandas DataFrame."""
        if not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = pd.DataFrame(raw_data)
        df[constants.TIME_INDEX] = pd.to_datetime(df["dateTime"]).dt.date
        df["Value"] = pd.to_numeric(df["value"], errors="coerce")

        if variable == constants.STAGE:
            # UK stage data is 15-min, average to daily
            # A full day has 24 * 4 = 96 readings. We accept days with at least 90 readings.
            min_readings = 90
            df_daily = (
                df.groupby(constants.TIME_INDEX).agg(Value=("Value", "mean"), Count=("Value", "size")).reset_index()
            )
            df_daily = df_daily[df_daily["Count"] >= min_readings]
            df_daily = df_daily[[constants.TIME_INDEX, "Value"]]
        else:  # discharge is already daily
            df_daily = df[[constants.TIME_INDEX, "Value"]]

        df_daily = df_daily.rename(columns={"Value": variable})
        df_daily[constants.TIME_INDEX] = pd.to_datetime(df_daily[constants.TIME_INDEX])

        # Ensure complete time series within the data range
        if not df_daily.empty:
            date_range = pd.date_range(
                start=df_daily[constants.TIME_INDEX].min(),
                end=df_daily[constants.TIME_INDEX].max(),
                freq="D",
            )
            complete_ts = pd.DataFrame(date_range, columns=[constants.TIME_INDEX])
            df_daily = pd.merge(complete_ts, df_daily, on=constants.TIME_INDEX, how="left")

        return df_daily.set_index(constants.TIME_INDEX)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses UK river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)

            # Filter by exact start and end date after processing
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
            return df

        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
