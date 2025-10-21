"""Fetcher for Czech river gauge data from CHMI."""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class CzechFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the Czech Hydrometeorological Institute (CHMI).

    Data Source: CHMI Open Data Portal (https://opendata.chmi.cz/)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)
        - ``constants.DISCHARGE_INSTANT`` (m³/s, hourly)
        - ``constants.STAGE_INSTANT`` (m, hourly)

    """

    METADATA_URL = "https://opendata.chmi.cz/hydrology/historical/metadata/meta1.json"
    DAILY_BASE_URL = "https://opendata.chmi.cz/hydrology/historical/data/daily/H_{id}_DQ_{year}.json"
    HOURLY_BASE_URL = "https://opendata.chmi.cz/hydrology/historical/data/hourly/H_{id}_HQ_{year}.json"
    TEMP_BASE_URL = "https://opendata.chmi.cz/hydrology/historical/data/measured_temperature/H_{id}_OT_{year}.json"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available Czech gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("czech")

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and returns CHMI hydrological station metadata.

        Data is fetched from:
        ``https://opendata.chmi.cz/hydrology/historical/metadata/meta1.json``

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        logger.info(f"Fetching metadata from {self.METADATA_URL}")
        s = utils.requests_retry_session()
        try:
            response = s.get(self.METADATA_URL)
            response.raise_for_status()
            data = response.json()

            header = data["data"]["data"]["header"].split(",")
            values = data["data"]["data"]["values"]
            df = pd.DataFrame(values, columns=header)

            rename_map = {
                "objID": constants.GAUGE_ID,
                "STATION_NAME": constants.STATION_NAME,
                "STREAM_NAME": constants.RIVER,
                "GEOGR1": constants.LATITUDE,
                "GEOGR2": constants.LONGITUDE,
                "PLO_STA": constants.AREA,
            }

            df = df[list(rename_map.keys())].rename(columns=rename_map)

            df[constants.ALTITUDE] = None  # Altitude not available
            df[constants.SOURCE] = "CHMI Open Data"
            df[constants.COUNTRY] = "Czech Republic"

            for col in [constants.LATITUDE, constants.LONGITUDE, constants.AREA]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            return df.set_index(constants.GAUGE_ID)

        except requests.RequestException as e:
            logger.error(f"Failed to fetch metadata: {e}")
            raise
        except KeyError as e:
            logger.error(f"Unexpected JSON structure: missing {e}")
            raise

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.DISCHARGE_DAILY_MEAN,
            constants.STAGE_DAILY_MEAN,
            constants.WATER_TEMPERATURE_DAILY_MEAN,
            constants.DISCHARGE_INSTANT,
            constants.STAGE_INSTANT,
        )

    def _get_url_and_ts_con_id(self, variable: str):
        if variable == constants.DISCHARGE_DAILY_MEAN:
            return self.DAILY_BASE_URL, "QD"
        elif variable == constants.STAGE_DAILY_MEAN:
            return self.DAILY_BASE_URL, "HD"
        elif variable == constants.WATER_TEMPERATURE_DAILY_MEAN:
            return self.DAILY_BASE_URL, "TD"
        elif variable == constants.DISCHARGE_INSTANT:
            return self.HOURLY_BASE_URL, "QH"
        elif variable == constants.STAGE_INSTANT:
            return self.HOURLY_BASE_URL, "HH"
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """Downloads raw data year by year."""
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        years = range(start_dt.year, end_dt.year + 1)

        base_url_template, ts_target = self._get_url_and_ts_con_id(variable)

        all_data = []
        s = utils.requests_retry_session()

        for year in years:
            url = base_url_template.format(id=gauge_id, year=year)
            logger.info(f"Fetching {variable} for {gauge_id} for year {year} from {url}")
            try:
                r = s.get(url)
                if r.status_code == 404:
                    logger.warning(f"Data not found for year {year}: {url}")
                    continue
                r.raise_for_status()
                js = r.json()

                tslist = js.get("tsList", [])
                if not tslist:
                    logger.warning(f"No tsList found for year {year}")
                    continue

                ts_entries = [ts for ts in tslist if ts.get("tsConID", "").upper() == ts_target.upper()]
                if not ts_entries:
                    available = [ts.get("tsConID") for ts in tslist]
                    logger.warning(f"tsConID {ts_target} not found for year {year}. Available: {available}")
                    continue

                for ts in ts_entries:
                    data_block = ts.get("tsData", {}).get("data", {})
                    if not data_block:
                        logger.warning(f"No data block in tsData for {ts.get('tsConID')} in year {year}")
                        continue

                    header = data_block.get("header", "").split(",")
                    values = data_block.get("values", [])
                    if not header or not values:
                        logger.warning(f"Empty header or values for {ts.get('tsConID')} in year {year}")
                        continue

                    df = pd.DataFrame(values, columns=header)
                    all_data.append(df)

            except requests.RequestException as e:
                logger.error(f"Error fetching data for year {year}: {e}")
            except Exception as e:
                logger.error(f"Error processing data for year {year}: {e}")

        return all_data

    def _parse_data(self, gauge_id: str, raw_data_list: List[pd.DataFrame], variable: str) -> pd.DataFrame:
        """Parses the raw dataframes."""
        if not raw_data_list:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            df_all = pd.concat(raw_data_list, ignore_index=True)
            if df_all.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            df_all["DT"] = pd.to_datetime(df_all["DT"].str.replace("Z", "+00:00"), utc=True).dt.tz_localize(None)
            df_all["VAL"] = pd.to_numeric(df_all["VAL"], errors="coerce")

            df_all = df_all.rename(columns={"DT": constants.TIME_INDEX, "VAL": variable})

            # Unit conversion
            if variable == constants.STAGE_DAILY_MEAN or variable == constants.STAGE_INSTANT:
                df_all[variable] = df_all[variable] / 100.0  # cm to m

            return (
                df_all[[constants.TIME_INDEX, variable]]
                .dropna()
                .sort_values(by=constants.TIME_INDEX)
                .set_index(constants.TIME_INDEX)
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
