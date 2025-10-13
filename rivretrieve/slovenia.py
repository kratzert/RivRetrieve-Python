"""Fetcher for Slovenian river gauge data from ARSO."""

import logging
from datetime import datetime
from io import StringIO
from typing import Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class SloveniaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Slovenia's ARSO API."""

    BASE_URL = "https://vode.arso.gov.si/hidarhiv/pov_arhiv_tab.php"

    @staticmethod
    def get_gauge_ids() -> pd.DataFrame:
        """Retrieves a DataFrame of available Slovenian gauge IDs."""
        return utils.load_sites_csv("slovenia")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE, constants.STAGE)

    def _download_data(
        self, gauge_id: str, variable: str, start_date: str, end_date: str
    ) -> Optional[str]:
        """Downloads the raw CSV data from the ARSO API."""
        start_year = datetime.strptime(start_date, "%Y-%m-%d").year
        end_year = datetime.strptime(end_date, "%Y-%m-%d").year

        query = (
            f"?p_postaja={gauge_id}"
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
            logger.error(f"Error fetching data for site {gauge_id}: {e}")
            return None

    def _parse_data(
        self, gauge_id: str, raw_data: Optional[str], variable: str
    ) -> pd.DataFrame:
        """Parses the raw CSV data."""
        if not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            df = pd.read_csv(StringIO(raw_data), sep=";", encoding="utf-8")

            if df.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            df = df.rename(columns={"Datum": constants.TIME_INDEX})
            df[constants.TIME_INDEX] = pd.to_datetime(
                df[constants.TIME_INDEX], format="%d.%m.%Y", errors="coerce"
            )
            df = df.dropna(subset=[constants.TIME_INDEX])

            if variable == constants.STAGE:
                raw_col = "vodostaj (cm)"
                if raw_col in df.columns:
                    df[variable] = (
                        pd.to_numeric(df[raw_col], errors="coerce") / 100.0
                    )  # cm to m
                else:
                    logger.warning(f"Column {raw_col} not found for site {gauge_id}")
                    return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
            elif variable == constants.DISCHARGE:
                raw_col = "pretok (m3/s)"
                if raw_col in df.columns:
                    df[variable] = pd.to_numeric(df[raw_col], errors="coerce")
                else:
                    logger.warning(f"Column {raw_col} not found for site {gauge_id}")
                    return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
            else:
                # Should not happen due to check in get_data
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            return (
                df[[constants.TIME_INDEX, variable]]
                .dropna()
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
        """Fetches and parses Slovenian river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)

            # Filter by date range
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[
                (df[constants.TIME_INDEX] >= start_date_dt)
                & (df[constants.TIME_INDEX] <= end_date_dt)
            ]
            return df
        except Exception as e:
            logger.error(
                f"Failed to get data for site {gauge_id}, variable {variable}: {e}"
            )
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
