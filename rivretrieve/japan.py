"""Fetcher for Japanese river gauge data."""

import io
import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

from . import base, utils

logger = logging.getLogger(__name__)


class JapanFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Japan's MLIT."""

    BASE_URL = "http://www1.river.go.jp/cgi-bin/DspWaterData.exe"

    @staticmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available Japanese gauge sites."""
        return utils.load_sites_csv("japan")

    def _get_kind(self, variable: str) -> int:
        if variable == "stage":
            return 2
        elif variable == "discharge":
            return 6
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(self, variable: str, start_date: str, end_date: str) -> List[pd.DataFrame]:
        """Downloads raw data month by month."""
        kind = self._get_kind(variable)
        site_id = self.site_id

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
                request_end_dt = end_date
            request_end_str = request_end_dt.strftime("%Y%m%d")

            params = {
                "KIND": kind,
                "ID": site_id,
                "BGNDATE": month_start_str,
                "ENDDATE": request_end_str,
            }

            try:
                response = s.get(self.BASE_URL, params=params)
                response.raise_for_status()
                response.encoding = 'shift_jis'  # Japanese encoding
                soup = BeautifulSoup(response.text, 'lxml')
                tables = soup.find_all('table')
                if len(tables) > 1:
                    table = tables[1]  # Second table has the data
                else:
                    table = None

                if table:
                    df = pd.read_html(io.StringIO(str(table)), header=None)[0]
                    monthly_data.append(df)
                else:
                    logger.warning(f"No table found for site {site_id} for {current_dt.strftime('%Y-%m')}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data for site {site_id} for {current_dt.strftime('%Y-%m')}: {e}")
            except Exception as e:
                logger.error(f"Error processing data for site {site_id} for {current_dt.strftime('%Y-%m')}: {e}")

            current_dt += relativedelta(months=1)

        return monthly_data

    def _parse_data(self, raw_data_list: List[pd.DataFrame], variable: str) -> pd.DataFrame:
        """Parses the list of monthly DataFrames."""
        if not raw_data_list:
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])

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
                logger.warning(f"Unexpected table structure for site {self.site_id}, skipping month.")
                continue

            data_df = data_df.iloc[:, [0, 12]]
            data_df.columns = ['Date', 'Value']

            try:
                data_df['Date'] = pd.to_datetime(data_df['Date'], format='%Y/%m/%d', errors='coerce')
                data_df = data_df.dropna(subset=['Date'])

                data_df['Value'] = pd.to_numeric(data_df['Value'], errors='coerce')
                all_dfs.append(data_df.dropna())
            except Exception as e:
                logger.error(f"Error parsing DataFrame: {e}\n{df.head()}")
                continue

        if not all_dfs:
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.rename(columns={"Value": utils.get_column_name(variable)})
        final_df = final_df.sort_values(by="Date").reset_index(drop=True)

        return final_df

    def get_data(self, variable: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches and parses Japanese river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        utils.get_column_name(variable)  # Validate variable

        try:
            raw_data_list = self._download_data(variable, start_date, end_date)
            df = self._parse_data(raw_data_list, variable)

            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[(df["Date"] >= start_date_dt) & (df["Date"] <= end_date_dt)]
            return df

        except Exception as e:
            logger.error(f"Failed to get data for site {self.site_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])