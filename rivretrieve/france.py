"Fetcher for French river gauge data from Hubeau."

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class FranceFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from France's Hubeau API.

    Data Source: Hubeau (https://hubeau.eaufrance.fr/)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MAX`` (m)
    """

    BASE_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available French gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("french")  # Note: CSV file name is french_sites.csv

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE_DAILY_MEAN, constants.STAGE_DAILY_MAX)

    def _get_variable_code(self, variable: str) -> str:
        if variable == constants.DISCHARGE_DAILY_MEAN:
            return "QmnJ"
        elif variable == constants.STAGE_DAILY_MAX:
            return "HIXnJ"
        else:
            logger.warning(f"Unsupported variable: {variable}")

    def _conversion_factor(self, variable: str) -> float:
        if variable.startswith(constants.DISCHARGE):
            return 1000  # l/s for flow rates (divide by 1000 to convert to m3/s).
        elif variable.startswith(constants.STAGE):
            return 1000  # mm for water heights (divide by 1000 to convert to meters);

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """Downloads raw data from the Hubeau API."""
        grandeur = self._get_variable_code(variable)

        params = {
            "code_entite": gauge_id,
            "date_debut_obs": start_date,
            "date_fin_obs": end_date,
            "grandeur_hydro": grandeur,
            "size": 20000,  # Max page size
        }
        s = utils.requests_retry_session()
        headers = {"User-Agent": "Mozilla/5.0"}
        all_data = []
        next_uri = self.BASE_URL

        while next_uri:
            try:
                logger.info(f"Fetching {next_uri}")
                response = s.get(
                    next_uri,
                    params=params if next_uri == self.BASE_URL else None,
                    headers=headers,
                )
                response.raise_for_status()
                data = response.json()
                all_data.extend(data.get("data", []))
                next_uri = data.get("next")
                if not next_uri:
                    break
                # Subsequent requests use the full URL from "next"
                params = None
            except requests.exceptions.RequestException as e:
                logger.error(f"Hubeau API request failed for site {gauge_id}: {e}")
                raise
            except ValueError as e:
                logger.error(f"Hubeau API JSON decode failed for site {gauge_id}: {e}\nResponse: {response.text}")
                raise
        return all_data

    def _parse_data(
        self,
        gauge_id: str,
        raw_data: List[Dict[str, Any]],
        variable: str,
    ) -> pd.DataFrame:
        """Parses the raw JSON data."""
        if not raw_data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        try:
            df = pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            grandeur_code = self._get_variable_code(variable)
            df = df[df["grandeur_hydro_elab"] == grandeur_code]

            if df.empty or "date_obs_elab" not in df.columns or "resultat_obs_elab" not in df.columns:
                logger.warning(f"Missing expected columns for site {gauge_id}")
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            df[constants.TIME_INDEX] = pd.to_datetime(df["date_obs_elab"]).dt.date
            df[variable] = pd.to_numeric(df["resultat_obs_elab"], errors="coerce") / self._conversion_factor(variable)
            df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX])
            return (
                df[[constants.TIME_INDEX, variable]]
                .dropna()
                .sort_values(by=constants.TIME_INDEX)
                .set_index(constants.TIME_INDEX)
            )
        except Exception as e:
            logger.error(f"Error parsing JSON data for site {gauge_id}: {e}")
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
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
