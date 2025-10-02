"""Fetcher for Chilean river gauge data."""

import io
import logging
import re
import time
from typing import Optional

import pandas as pd
import requests

from . import base, utils

logger = logging.getLogger(__name__)


class ChileFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Chile's CR2 explorador."""

    @staticmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available Chilean gauge sites."""
        return utils.load_sites_csv("chile")

    def _download_data(self, variable: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Downloads the raw CSV data by first finding the download link."""
        if variable != "discharge":
            logger.warning("ChileFetcher only supports variable='discharge'")
            return None

        # This long URL was extracted from the R code
        original = "https://explorador.cr2.cl/request.php?options={%22variable%22:{%22id%22:%22qflxDaily%22,%22var%22:%22caudal%22,%22intv%22:%22daily%22,%22season%22:%22year%22,%22stat%22:%22mean%22,%22minFrac%22:80},%22time%22:{%22start%22:-946771200,%22end%22:1727827200,%22months%22:%22A%C3%B1o%20completo%22},%22anomaly%22:{%22enabled%22:false,%22type%22:%22dif%22,%22rank%22:%22no%22,%22start_year%22:1980,%22end_year%22:2010,%22minFrac%22:70},%22map%22:{%22stat%22:%22mean%22,%22minFrac%22:10,%22borderColor%22:%227F7F7F%22,%22colorRamp%22:%22Jet%22,%22showNaN%22:false,%22limits%22:{%22range%22:[5,95],%22size%22:[4,12],%22type%22:%22prc%22}},%22series%22:{%22sites%22:[%22"
        ending = "%22],%22start%22:null,%22end%22:null},%22export%22:{%22map%22:%22Shapefile%22,%22series%22:%22CSV%22,%22view%22:{%22frame%22:%22Vista%20Actual%22,%22map%22:%22roadmap%22,%22clat%22:-18.0036,%22clon%22:-69.6331,%22zoom%22:5,%22width%22:461,%22height%22:2207}},%22action%22:[%22export_series%22]}"
        request_url = f"{original}{self.site_id}{ending}"

        s = utils.requests_retry_session()
        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            time.sleep(0.3)  # Be nice to the server
            response = s.get(request_url, headers=headers)
            response.raise_for_status()

            # The response body contains the URL to the CSV file
            match = re.search(r'https://www\.explorador\.cr2\.cl/tmp/[^/]+/[^\"]+\.csv', response.text)
            if not match:
                logger.error(f"Could not find download link in response for site {self.site_id}")
                return None

            csv_url = match.group(0)
            logger.info(f"Found CSV URL: {csv_url}")

            time.sleep(0.3)
            csv_response = s.get(csv_url, headers=headers)
            csv_response.raise_for_status()

            df = pd.read_csv(io.StringIO(csv_response.text))
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for site {self.site_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing data for site {self.site_id}: {e}")
            return None

    def _parse_data(self, raw_df: Optional[pd.DataFrame], variable: str) -> pd.DataFrame:
        """Parses the raw DataFrame."""
        col_name = utils.get_column_name(variable)
        if raw_df is None or raw_df.empty:
            return pd.DataFrame(columns=["Date", col_name])

        try:
            # Clean column names (remove leading/trailing spaces)
            raw_df.columns = raw_df.columns.str.strip()

            if not all(col in raw_df.columns for col in ['agno', 'mes', 'dia', 'valor']):
                logger.warning(f"Missing expected columns for site {self.site_id}")
                return pd.DataFrame(columns=["Date", col_name])

            df = raw_df.copy()
            df['Date'] = pd.to_datetime(df[['agno', 'mes', 'dia']].rename(columns={
                'agno': 'year',
                'mes': 'month',
                'dia': 'day'
            }),
                                        errors='coerce')
            df = df.dropna(subset=['Date'])
            df[col_name] = pd.to_numeric(df['valor'], errors='coerce')
            # Unit is already m3/s according to CR2 metadata

            return df[["Date", col_name]].dropna().sort_values(by="Date").reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error parsing data for site {self.site_id}: {e}")
            return pd.DataFrame(columns=["Date", col_name])

    def get_data(self, variable: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches and parses Chilean river gauge data."""
        if variable != "discharge":
            logger.warning("ChileFetcher only supports variable='discharge'")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])

        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        try:
            raw_data = self._download_data(variable, start_date, end_date)
            df = self._parse_data(raw_data, variable)

            # Filter by date range
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[(df["Date"] >= start_date_dt) & (df["Date"] <= end_date_dt)]
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {self.site_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])
