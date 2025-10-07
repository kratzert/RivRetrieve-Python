"""Fetcher for Brazilian river gauge data from ANA Hidroweb."""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import os

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from . import base, utils

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
USERNAME = os.environ.get("ANA_USERNAME")
PASSWORD = os.environ.get("ANA_PASSWORD")

class BrazilFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Brazil's ANA Hidroweb API v2."""

    BASE_URL = "https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas"
    AUTH_URL = f"{BASE_URL}/OAUth/v1"
    DATA_URL = f"{BASE_URL}/HidroinfoanaSerieTelemetricaAdotada/v1"

    def __init__(self, site_id: str, username: Optional[str] = None, password: Optional[str] = None):
        super().__init__(site_id)
        self.username = username or USERNAME
        self.password = password or PASSWORD
        self._token = None
        self._token_expiry = 0

        if not self.username or not self.password:
            logger.error("ANA Username or Password not provided. Please set ANA_USERNAME and ANA_PASSWORD in your .env file or pass them to the constructor.")

    @staticmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available Brazilian gauge sites."""
        return utils.load_sites_csv("brazil")

    def _get_token(self) -> Optional[str]:
        """Gets and caches the authentication token."""
        if not self.username or not self.password:
            return None

        if self._token and time.time() < self._token_expiry:
            return self._token

        logger.info("Fetching new authentication token for Brazil...")
        headers = {
            "Identificador": self.username,
            "Senha": self.password
        }
        s = utils.requests_retry_session()
        try:
            response = s.get(self.AUTH_URL, headers=headers)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "OK" and data.get("items", {}).get("sucesso"):
                self._token = data["items"]["tokenautenticacao"]
                # Set expiry to 14 minutes (840 seconds) to be safe
                self._token_expiry = time.time() + 840
                logger.info("Successfully obtained new token.")
                return self._token
            else:
                logger.error(f"Authentication failed: {data}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching token: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing token response: {e}")
            return None

    def _download_data(self, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Downloads raw data in 30-day chunks."""
        if not self.username or not self.password:
            return []

        all_data = []
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        s = utils.requests_retry_session()

        while current_date <= end_dt:
            token = self._get_token()
            if not token:
                logger.error("Cannot download data without a token.")
                break

            chunk_end_date = current_date + timedelta(days=29)
            if chunk_end_date > end_dt:
                chunk_end_date = end_dt

            params = {
                "CodigoDaEstacao": self.site_id,
                "DataDeBusca": current_date.strftime("%Y-%m-%d"),
                "RangeIntervaloDeBusca": "DIAS_30",
                "TipoFiltroData": "DATA_LEITURA"
            }
            headers = {
                "Authorization": f"Bearer {token}"
            }

            logger.info(f"Fetching Brazil data for site {self.site_id} from {current_date.strftime('%Y-%m-%d')}")
            try:
                response = s.get(self.DATA_URL, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                if data.get("status") == "OK" and data.get("items"):
                    all_data.extend(data["items"])
                elif data.get("status") == "OK":
                    logger.info(f"No items returned for {self.site_id} for period starting {current_date.strftime('%Y-%m-%d')}")
                else:
                    logger.warning(f"API returned non-OK status: {data}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error downloading data chunk for {self.site_id}: {e}")
            except Exception as e:
                logger.error(f"Error processing data chunk for {self.site_id}: {e}")

            current_date = chunk_end_date + timedelta(days=1)
            time.sleep(0.2) # Be nice to the API

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

            df["Date"] = pd.to_datetime(df["Data_Hora_Medicao"]).dt.date
            df["Date"] = pd.to_datetime(df["Date"])

            if variable == "discharge":
                val_col = "Vazao_Adotada"
                df[col_name] = pd.to_numeric(df[val_col], errors='coerce')
            elif variable == "stage":
                val_col = "Cota_Adotada"
                # Convert cm to m
                df[col_name] = pd.to_numeric(df[val_col], errors='coerce') / 100.0
            else:
                return pd.DataFrame(columns=["Date", col_name])

            df = df[["Date", col_name]].dropna()
            df = df.groupby("Date").mean().reset_index() # Ensure daily average
            return df.sort_values(by="Date").reset_index(drop=True)

        except Exception as e:
            logger.error(f"Error parsing data for site {self.site_id}: {e}")
            return pd.DataFrame(columns=["Date", col_name])

    def get_data(self, variable: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches and parses Brazilian river gauge data."""
        if not self.username or not self.password:
            logger.error("ANA Username or Password not provided. Check your .env file or constructor arguments.")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])

        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        utils.get_column_name(variable)  # Validate variable

        try:
            raw_data = self._download_data(variable, start_date, end_date)
            df = self._parse_data(raw_data, variable)

            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            df = df[(df["Date"] >= start_date_dt) & (df["Date"] <= end_date_dt)]
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {self.site_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=["Date", utils.get_column_name(variable)])