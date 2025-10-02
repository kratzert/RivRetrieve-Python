"""Fetcher for USA river gauge data from USGS NWIS."""

import logging
from typing import Optional

import pandas as pd
from dataretrieval import nwis

from . import base, utils

logger = logging.getLogger(__name__)


class USAFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from USGS NWIS."""

    @staticmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available USA gauge sites."""
        return utils.load_sites_csv("usa")

    def _get_param_code(self, variable: str) -> str:
        if variable == "stage":
            return "00065"
        elif variable == "discharge":
            return "00060"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(self, variable: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Downloads data using the dataretrieval package."""
        param_code = self._get_param_code(variable)
        try:
            df, meta = nwis.get_dv(sites=[self.site_id], startDT=start_date, endDT=end_date, parameterCd=[param_code])
            return df
        except Exception as e:
            logger.error(f"Error fetching NWIS data for site {self.site_id}, param {param_code}: {e}")
            return pd.DataFrame()

    def _parse_data(self, raw_data: pd.DataFrame, variable: str) -> pd.DataFrame:
        """Parses the DataFrame from dataretrieval."""
        if raw_data.empty:
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])

        logger.info(f"DataFrame columns for site {self.site_id}: {raw_data.columns.tolist()}")

        col_name = utils.get_column_name(variable)
        param_code = self._get_param_code(variable)

        value_col = None
        for col in raw_data.columns:
            if col.startswith(param_code) and ("_Mean" in col or "_00003" in col):
                value_col = col
                break

        if value_col is None:
            logger.warning(f"Could not find value column for param {param_code} in data for site {self.site_id}")
            return pd.DataFrame(columns=["Date", col_name])

        df = raw_data[[value_col]].copy()
        df.index.name = "Date"
        df = df.reset_index()
        df["Date"] = pd.to_datetime(df["Date"].dt.date)

        # Unit conversion
        if variable == "stage":  # Feet to meters
            mult = 0.3048
        elif variable == "discharge":  # cfs to m3/s
            mult = 0.0283168466
        df[col_name] = pd.to_numeric(df[value_col], errors='coerce') * mult

        return df[["Date", col_name]].dropna()

    def get_data(self, variable: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches and parses USA river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        utils.get_column_name(variable)  # Validate variable

        try:
            raw_data = self._download_data(variable, start_date, end_date)
            df = self._parse_data(raw_data, variable)
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {self.site_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])
