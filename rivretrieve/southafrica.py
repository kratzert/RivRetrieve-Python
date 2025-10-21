"Fetcher for South African river gauge data."

import logging
import re
from datetime import date, datetime
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

from . import base, constants, utils

logger = logging.getLogger(__name__)


class SouthAfricaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from South Africa's Department of Water and Sanitation (DWS).

    Data Source: DWS Hydrology Services (https://www.dws.gov.za/Hydrology/Verified/HyData.aspx)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_INSTANT`` (m)
    """

    BASE_URL = "https://www.dws.gov.za/Hydrology/Verified/HyData.aspx"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available South African gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("southAfrican")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE_DAILY_MEAN, constants.DISCHARGE_INSTANT, constants.STAGE_INSTANT)

    def _construct_endpoint(
        self,
        gauge_id: str,
        data_type: str,
        chunk_start_date: date,
        chunk_end_date: date,
    ) -> str:
        start_str = chunk_start_date.strftime("%Y-%m-%d")
        end_str = chunk_end_date.strftime("%Y-%m-%d")
        endpoint = (
            f"{self.BASE_URL}?Station={gauge_id}100.00"
            f"&DataType={data_type}&StartDT={start_str}&EndDT={end_str}&SiteType=RIV"
        )
        return endpoint

    def _get_variable_name(self, variable: str) -> str:
        if variable == constants.DISCHARGE_DAILY_MEAN:
            return "D_AVG_FR"
        elif variable == constants.DISCHARGE_INSTANT:
            return "COR_FLOW"
        elif variable == constants.STAGE_INSTANT:
            return "COR_LEVEL"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> List[pd.DataFrame]:
        """Downloads raw data in chunks."""
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        s = utils.requests_retry_session()
        headers = {"User-Agent": "Mozilla/5.0"}
        data_list = []

        if variable in (constants.STAGE_INSTANT, constants.DISCHARGE_INSTANT):
            data_type = "Point"
            chunk_years = 1
            header = [
                "DATE",
                "TIME",
                "COR_LEVEL",  # Stage instant.
                "COR_LEVEL_QUAL",
                "COR_FLOW",  # Flow instant.
                "COR_FLOW_QUAL",
            ]
        elif variable == constants.DISCHARGE_DAILY_MEAN:  # discharge
            data_type = "Daily"
            chunk_years = 20
            header = ["DATE", "D_AVG_FR", "QUAL"]
        else:
            raise ValueError(f"Unsupported variable: {variable}")

        current_start_dt = start_dt
        while current_start_dt <= end_dt:
            chunk_end_dt = current_start_dt + relativedelta(years=chunk_years, days=-1)
            if chunk_end_dt > end_dt:
                chunk_end_dt = end_dt

            endpoint = self._construct_endpoint(gauge_id, data_type, current_start_dt, chunk_end_dt)
            logger.info(f"Fetching {variable} for site {gauge_id} from {current_start_dt} to {chunk_end_dt}")

            try:
                response = s.get(endpoint, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "lxml")
                pre_tag = soup.find("pre")

                if pre_tag:
                    data_text = pre_tag.text
                    if "No data for this period" in data_text or not data_text.strip():
                        logger.info("No data found for this chunk.")
                    else:
                        # Clean up lines and split by spaces
                        lines = data_text.strip().split("\n")
                        data_rows = []
                        header_found = False
                        for line in lines:
                            if line.startswith("DATE"):
                                header_found = True
                                continue
                            if header_found and re.match(r"^[0-9]{8}", line):
                                data_rows.append(re.split(r"\s+", line.strip()))

                        if data_rows:
                            df = pd.DataFrame(data_rows)
                            # Pad columns if necessary
                            if df.shape[1] < len(header):
                                for i in range(len(header) - df.shape[1]):
                                    df[df.shape[1] + i] = None
                            df = df.iloc[:, : len(header)]
                            df.columns = header
                            data_list.append(df)
                else:
                    logger.warning(f"No <pre> tag found for site {gauge_id} at {endpoint}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data for site {gauge_id}: {e}")
            except Exception as e:
                logger.error(f"Error processing data for site {gauge_id}: {e}")

            current_start_dt = chunk_end_dt + relativedelta(days=1)

        return data_list

    def _parse_data(
        self,
        gauge_id: str,
        raw_data_list: List[pd.DataFrame],
        variable: str,
    ) -> pd.DataFrame:
        """Parses the list of DataFrames."""
        if not raw_data_list:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            df = pd.concat(raw_data_list, ignore_index=True)
            if df.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            df[constants.TIME_INDEX] = pd.to_datetime(df["DATE"], format="%Y%m%d", errors="coerce")
            df = (
                df.dropna(subset=[constants.TIME_INDEX])
                .sort_values(by=constants.TIME_INDEX)
                .set_index(constants.TIME_INDEX)
            )

            column = self._get_variable_name(variable)
            df[column] = pd.to_numeric(df[column], errors="coerce")

            return df.rename(columns={column: variable})

        except Exception as e:
            logger.error(f"Error parsing data for site {gauge_id}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

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
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
