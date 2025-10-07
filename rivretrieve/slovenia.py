"""Fetcher for Slovenian river gauge data from ARSO."""

import logging
from datetime import datetime
from io import StringIO
from typing import Optional

import pandas as pd
import requests

from . import base, utils

logger = logging.getLogger(__name__)


class SloveniaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Slovenia's ARSO API."""

    BASE_URL = "https://vode.arso.gov.si/hidarhiv/pov_arhiv_tab.php"

    @staticmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available Slovenian gauge sites."""
        return utils.load_sites_csv("slovenia")

    def _download_data(
        self, variable: str, start_date: str, end_date: str
    ) -> Optional[str]:
        """Downloads the raw CSV data from the ARSO API."""
        start_year = datetime.strptime(start_date, "%Y-%m-%d").year
        end_year = datetime.strptime(end_date, "%Y-%m-%d").year

        query = (
            f"?p_postaja={self.site_id}"
            f"&p_od_leto={start_year}"
            f"&p_do_leto={end_year}"
            "&b_oddo_CSV=Izvoz+dnevnih+vrednosti+v+CSV"
        )
        url = self.BASE_URL + query
        s = utils.requests_retry_session()
        try:
            response = s.get(url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for site {self.site_id}: {e}")
            return None

    def _parse_data(self, raw_data: Optional[str], variable: str) -> pd.DataFrame:
        """Parses the raw CSV data."""
        col_name = utils.get_column_name(variable)
        if not raw_data:
            return pd.DataFrame(columns=["Date", col_name])

        try:
            df = pd.read_csv(StringIO(raw_data), sep=";", encoding="utf-8")

            if df.empty:
                return pd.DataFrame(columns=["Date", col_name])

            df = df.rename(columns={"Datum": "Date"})
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce")
            df = df.dropna(subset=["Date"])

            if variable == "stage":
                raw_col = "vodostaj (cm)"
                if raw_col in df.columns:
                    df[col_name] = (
                        pd.to_numeric(df[raw_col], errors="coerce") / 100.0
                    )  # cm to m
                else:
                    logger.warning(
                        f"Column {raw_col} not found for site {self.site_id}"
                    )
                    return pd.DataFrame(columns=["Date", col_name])
            elif variable == "discharge":
                raw_col = "pretok (m3/s)"
                if raw_col in df.columns:
                    df[col_name] = pd.to_numeric(df[raw_col], errors="coerce")
                else:
                    logger.warning(
                        f"Column {raw_col} not found for site {self.site_id}"
                    )
                    return pd.DataFrame(columns=["Date", col_name])
            else:
                # Should not happen due to check in get_data
                return pd.DataFrame(columns=["Date", col_name])

            return (
                df[["Date", col_name]]
                .dropna()
                .sort_values(by="Date")
                .reset_index(drop=True)
            )

        except Exception as e:
            logger.error(f"Error parsing data for site {self.site_id}: {e}")
            return pd.DataFrame(columns=["Date", col_name])

    def get_data(
        self,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses Slovenian river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        utils.get_column_name(variable)  # Validate variable

        if variable not in ["stage", "discharge"]:
            logger.warning(f"Unsupported variable: {variable} for SloveniaFetcher")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])

        try:
            raw_data = self._download_data(variable, start_date, end_date)
            df = self._parse_data(raw_data, variable)

            # Filter by date range
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[(df["Date"] >= start_date_dt) & (df["Date"] <= end_date_dt)]
            return df
        except Exception as e:
            logger.error(
                f"Failed to get data for site {self.site_id}, variable {variable}: {e}"
            )
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])
