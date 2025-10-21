"Fetcher for Japanese river gauge data."

import io
import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

from . import base, constants, utils

logger = logging.getLogger(__name__)


class JapanFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Japan's Ministry of Land, Infrastructure, Transport and Tourism (MLIT).

    Data Source: Water Information System (http://www1.river.go.jp/)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
    """

    BASE_URL = "http://www1.river.go.jp/cgi-bin/DspWaterData.exe"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available Japanese gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("japan")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE_DAILY_MEAN, constants.STAGE_DAILY_MEAN)

    def _get_kind(self, variable: str) -> int:
        if variable == constants.STAGE_DAILY_MEAN:
            return 2
        elif variable == constants.DISCHARGE_DAILY_MEAN:
            return 6
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> List[pd.DataFrame]:
        """Downloads raw data month by month."""
        kind = self._get_kind(variable)

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        current_dt = start_dt.replace(day=1)
        monthly_data = []
        s = utils.requests_retry_session()

        while current_dt <= end_dt:
            month_start_str = current_dt.strftime("%Y%m%d")
            # End date for the request can be a bit beyond the current month
            request_end_dt = current_dt + relativedelta(months=1, days=-1)
            if request_end_dt > end_dt:
                request_end_dt = end_dt
            request_end_str = request_end_dt.strftime("%Y%m%d")

            params = {
                "KIND": kind,
                "ID": gauge_id,
                "BGNDATE": month_start_str,
                "ENDDATE": request_end_str,
            }

            try:
                response = s.get(self.BASE_URL, params=params)
                response.raise_for_status()
                response.encoding = "shift_jis"  # Japanese encoding
                soup = BeautifulSoup(response.text, "lxml")
                tables = soup.find_all("table")
                if len(tables) > 1:
                    table = tables[1]  # Second table has the data
                else:
                    table = None

                if table:
                    df = pd.read_html(io.StringIO(str(table)), header=None)[0]
                    monthly_data.append(df)
                else:
                    logger.warning(f"No table found for site {gauge_id} for {current_dt.strftime('%Y-%m')}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data for site {gauge_id} for {current_dt.strftime('%Y-%m')}: {e}")
            except Exception as e:
                logger.error(f"Error processing data for site {gauge_id} for {current_dt.strftime('%Y-%m')}: {e}")

            current_dt += relativedelta(months=1)

        return monthly_data

    def _parse_data(
        self,
        gauge_id: str,
        raw_data_list: List[pd.DataFrame],
        variable: str,
    ) -> pd.DataFrame:
        """Parses the list of monthly DataFrames."""
        if not raw_data_list:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        all_dfs = []
        for df in raw_data_list:
            if df.empty or len(df) < 5:
                continue

            # Skip header rows of the second table, data starts around row 2
            data_df = df.iloc[2:].copy()
            if data_df.empty:
                continue

            # Columns: Date, 0h, 1h, ..., 12h, ..., 23h
            # We need Date (index 0) and the value at 12h (index 12)
            if data_df.shape[1] < 13:
                logger.warning(f"Unexpected table structure for site {gauge_id}, skipping month.")
                continue

            data_df = data_df.iloc[:, [0, 12]]
            data_df.columns = [constants.TIME_INDEX, "Value"]

            try:
                data_df[constants.TIME_INDEX] = pd.to_datetime(
                    data_df[constants.TIME_INDEX], format="%Y/%m/%d", errors="coerce"
                )
                data_df = data_df.dropna(subset=[constants.TIME_INDEX])

                data_df["Value"] = pd.to_numeric(data_df["Value"], errors="coerce")
                all_dfs.append(data_df.dropna())
            except Exception as e:
                logger.error(f"Error parsing DataFrame: {e}\n{df.head()}")
                continue

        if not all_dfs:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.rename(columns={"Value": variable})
        final_df = final_df.sort_values(by=constants.TIME_INDEX)

        return final_df.set_index(constants.TIME_INDEX)

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
            raw_data_list = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data_list, variable)

            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
            return df

        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
