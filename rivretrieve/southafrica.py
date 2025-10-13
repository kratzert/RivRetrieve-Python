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
    """Fetches river gauge data from South Africa's DWS."""

    BASE_URL = "https://www.dws.gov.za/Hydrology/Verified/HyData.aspx"

    @staticmethod
    def get_gauge_ids() -> pd.DataFrame:
        """Retrieves a DataFrame of available South African gauge IDs."""
        return utils.load_sites_csv("southAfrican")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE, constants.STAGE)

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

        if variable == constants.STAGE:
            data_type = "Point"
            chunk_years = 1
            header = [
                "DATE",
                "TIME",
                "COR_LEVEL",
                "COR_LEVEL_QUAL",
                "COR_FLOW",
                "COR_FLOW_QUAL",
            ]
        elif variable == constants.DISCHARGE:  # discharge
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

            endpoint = self._construct_endpoint(
                gauge_id, data_type, current_start_dt, chunk_end_dt
            )
            logger.info(
                f"Fetching {variable} for site {gauge_id} from {current_start_dt} to {chunk_end_dt}"
            )

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
                    logger.warning(
                        f"No <pre> tag found for site {gauge_id} at {endpoint}"
                    )

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
            full_df = pd.concat(raw_data_list, ignore_index=True)
            if full_df.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            full_df[constants.TIME_INDEX] = pd.to_datetime(
                full_df["DATE"], format="%Y%m%d", errors="coerce"
            )
            full_df = full_df.dropna(subset=[constants.TIME_INDEX])

            if variable == constants.STAGE:
                full_df["COR_LEVEL"] = pd.to_numeric(
                    full_df["COR_LEVEL"], errors="coerce"
                )
                # Average stage if multiple readings per day
                daily_df = (
                    full_df.groupby(constants.TIME_INDEX)
                    .agg(Value=("COR_LEVEL", "mean"))
                    .reset_index()
                )
            else:  # discharge
                full_df["D_AVG_FR"] = pd.to_numeric(
                    full_df["D_AVG_FR"], errors="coerce"
                )
                daily_df = full_df[[constants.TIME_INDEX, "D_AVG_FR"]].rename(
                    columns={"D_AVG_FR": "Value"}
                )

            daily_df = daily_df.rename(columns={"Value": variable})
            return (
                daily_df.dropna()
                .sort_values(by=constants.TIME_INDEX)
                .reset_index(drop=True)
            )

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
        """Fetches and parses South African river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data_list = self._download_data(
                gauge_id, variable, start_date, end_date
            )
            df = self._parse_data(gauge_id, raw_data_list, variable)
            return df
        except Exception as e:
            logger.error(
                f"Failed to get data for site {gauge_id}, variable {variable}: {e}"
            )
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
