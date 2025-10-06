"""Fetcher for French river gauge data from Hubeau."""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from . import base, utils

logger = logging.getLogger(__name__)


class FranceFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from France's Hubeau API."""

    BASE_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"

    @staticmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available French gauge sites."""
        return utils.load_sites_csv("french")  # Note: CSV file name is french_sites.csv

    def _download_data(self, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Downloads raw data from the Hubeau API."""
        if variable == "discharge":
            grandeur = "QmnJ"
        elif variable == "stage":
            grandeur = "HnJ"  # Assuming daily mean stage, though doc mentions HIXnJ
            logger.warning("Using grandeur_hydro='HnJ' for stage, this might not be daily mean.")
        else:
            logger.warning(f"Unsupported variable: {variable}")
            return []
        params = {
            "code_entite": self.site_id,
            "date_debut_obs": start_date,
            "date_fin_obs": end_date,
            "grandeur_hydro": grandeur,
            "size": 20000  # Max page size
        }
        s = utils.requests_retry_session()
        headers = {'User-Agent': 'Mozilla/5.0'}
        all_data = []
        next_uri = self.BASE_URL

        while next_uri:
            try:
                logger.info(f"Fetching {next_uri}")
                response = s.get(next_uri, params=params if next_uri == self.BASE_URL else None, headers=headers)
                response.raise_for_status()
                data = response.json()
                all_data.extend(data.get("data", []))
                next_uri = data.get("next")
                if not next_uri:
                    break
                # Subsequent requests use the full URL from "next"
                params = None
            except requests.exceptions.RequestException as e:
                logger.error(f"Hubeau API request failed for site {self.site_id}: {e}")
                raise
            except ValueError as e:
                logger.error(f"Hubeau API JSON decode failed for site {self.site_id}: {e}\nResponse: {response.text}")
                raise
        return all_data

    def _parse_data(self, raw_data: List[Dict[str, Any]], variable: str) -> pd.DataFrame:
        """Parses the raw JSON data."""
        col_name = utils.get_column_name(variable)
        if not raw_data:
            return pd.DataFrame(columns=["Date", col_name])

        try:
            df = pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=["Date", col_name])

            grandeur_code = "QmnJ" if variable == "discharge" else "HnJ"
            df = df[df['grandeur_hydro_elab'] == grandeur_code]

            if df.empty or "date_obs_elab" not in df.columns or "resultat_obs_elab" not in df.columns:
                logger.warning(f"Missing expected columns for site {self.site_id}")
                return pd.DataFrame(columns=["Date", col_name])

            df["Date"] = pd.to_datetime(df["date_obs_elab"]).dt.date
            # Convert L/s to m3/s
            df[col_name] = pd.to_numeric(df["resultat_obs_elab"], errors='coerce') / 1000.0
            df["Date"] = pd.to_datetime(df["Date"])
            return df[["Date", col_name]].dropna().sort_values(by="Date").reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error parsing JSON data for site {self.site_id}: {e}")
            return pd.DataFrame(columns=["Date", col_name])

    def get_data(self, variable: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches and parses French river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        utils.get_column_name(variable)  # Validate variable

        if variable not in ["discharge", "stage"]:
            logger.warning("FranceFetcher only supports variable='discharge'")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])

        try:
            raw_data = self._download_data(variable, start_date, end_date)
            df = self._parse_data(raw_data, variable)
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {self.site_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])
