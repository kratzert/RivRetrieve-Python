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

    BASE_URL = "https://www.ana.gov.br/hidrowebservice" #EstacoesTelemetricas"
    AUTH_URL = f"{BASE_URL}/EstacoesTelemetricas/OAUth/v1"
    # DATA_URL = f"{BASE_URL}/HidroinfoanaSerieTelemetricaAdotada/v1"

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
            'accept': '*/*',
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

    def _get_station_metadata(self): 
        token = self._get_token()
        headers = {
            'accept': '*/*',
            "Authorization": f"Bearer {token}"
        }
        metadata_url = f"{self.BASE_URL}/EstacoesTelemetricas/HidroInventarioEstacoes/v1"
        params = {
            "Código da Estação": self.site_id
        }
        s = utils.requests_retry_session()
        response = s.get(metadata_url, params=params, headers=headers)
        response.raise_for_status()
        metadata = pd.DataFrame(response.json()['items'])
        return metadata

    def _parse_data(self, raw_data: List[Dict[str, Any]], variable: str) -> pd.DataFrame:
        df = pd.DataFrame(raw_data)
        df.columns = df.columns.str.upper()
        if variable == 'stage': 
            prefix = 'COTA'
            # Handle inconsistency in data returned by API
            df = df.rename({'NIVELCONSISTENCIA': 'NIVEL_CONSISTENCIA'}, axis=1) 
        elif variable == 'discharge':
            prefix = 'VAZAO'

        columns = [
            "CODIGOESTACAO", "DATA_HORA_DADO",
            "DATA_ULTIMA_ALTERACAO", "DIA_MAXIMA",
            "DIA_MINIMA", "MAXIMA", "MINIMA",
            "MEDIADIARIA", "MEDIA", "MEDIA_ANUAL",
            "NIVEL_CONSISTENCIA"
        ]
        columns += [f"{prefix}_{i:02d}" for i in range(1, 32)]

        # Select columns and convert time to datetime
        if not all(c in df.columns for c in columns):
            return None

        df = df[columns]
        df["DATA_HORA_DADO"] = pd.to_datetime(df["DATA_HORA_DADO"])
        values = df.melt(
            id_vars=['DATA_HORA_DADO', 'NIVEL_CONSISTENCIA'],
            value_vars=[col for col in df.columns if col.startswith(prefix) and not col.endswith('_STATUS')],
            var_name='DAY',
            value_name=prefix
        )
        values['DAY'] = values['DAY'].str.extract(rf'{prefix}_(\d{{2}})')

        status = df.melt(
            id_vars=['DATA_HORA_DADO', 'NIVEL_CONSISTENCIA'],
            value_vars=[col for col in df.columns if col.startswith(prefix) and col.endswith('_STATUS')],
            var_name='DAY',
            value_name=f'{prefix}_STATUS'
        )

        if status.shape[0] == values.shape[0]:
            status['DAY'] = status['DAY'].str.extract(rf'{prefix}_(\d{{2}})_STATUS')
            df = pd.merge(values, status, on=['DATA_HORA_DADO', 'NIVEL_CONSISTENCIA', 'DAY'])
        else: 
            df = values 
            df[f'{prefix}_STATUS'] = pd.NA 

        df['YEAR'] = pd.to_datetime(df['DATA_HORA_DADO']).dt.year
        df['MONTH'] = pd.to_datetime(df['DATA_HORA_DADO']).dt.month
        df['DAY'] = df['DAY'].astype(int)

        # Create actual dates safely
        df['DATA_HORA_DADO'] = pd.to_datetime(
            df[['YEAR', 'MONTH', 'DAY']],
            errors='coerce'  # invalid days become NaT
        )

        # Step 6: Drop invalid dates (e.g. Feb 30)
        df = df.dropna(subset=['DATA_HORA_DADO'])
        df = df[['DATA_HORA_DADO', 'NIVEL_CONSISTENCIA', f'{prefix}', f'{prefix}_STATUS']]
        df['Date'] = df['DATA_HORA_DADO']

        col_name = utils.get_column_name(variable)
        if variable == "discharge":
            df[col_name] = pd.to_numeric(df[prefix], errors='coerce')
        elif variable == "stage":
            # Convert cm to m
            df[col_name] = pd.to_numeric(df[prefix], errors='coerce') / 100.0

        return df[['Date', col_name]].sort_values(by="Date").reset_index(drop=True)

    def _download_data(self, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]: 
        if not self.username or not self.password:
            return []

        if end_date is None: # I.e. get the most recent
            end_date = datetime.strftime(datetime.today(), "%Y-%m-%d")

        if start_date is None: 
            start_date = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=30)
            start_date = datetime.strftime(start_date, "%Y-%m-%d")

        # Convert to datetime
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Now we check whether the station has telemetry 
        metadata = self._get_station_metadata()
        metadata.columns = metadata.columns.str.upper()
        has_discharge = bool(metadata['TIPO_ESTACAO_DESC_LIQUIDA'].iloc[0])
        has_stage = bool(metadata['TIPO_ESTACAO_ESCALA'].iloc[0])
        if (variable == 'discharge'): 
            if not has_discharge: 
                return []
            else:
                column_varname = 'DESC_LIQUIDA'
        
        if (variable == 'stage') and not has_stage: 
            if not has_stage: 
                return []
            else:
                column_varname = 'ESCALA'

        record_start_date = pd.to_datetime(metadata[f'DATA_PERIODO_{column_varname}_INICIO'].iloc[0])
        record_end_date = pd.to_datetime(metadata[f'DATA_PERIODO_{column_varname}_FIM'].iloc[0])
        if record_end_date is None: 
            record_end_date = datetime.today()

        if start_date < record_start_date: 
            start_date = record_start_date
        if end_date > record_end_date: 
            end_date = record_end_date
        elif end_date < start_date:
            return []
        
        # Set up the requests session
        s = utils.requests_retry_session()
        # Get the endpoint for the given variable
        # NOTE these services allow up to one year of data to be retrieved
        if variable == "discharge": 
            endpoint = "EstacoesTelemetricas/HidroSerieVazao/v1" 
        elif variable == "stage": 
            endpoint = "EstacoesTelemetricas/HidroSerieCotas/v1"
        elif variable == "precipitation":
            endpoint = "EstacoesTelemetricas/HidroSerieChuva/v1"
        data_url = f"{self.BASE_URL}/{endpoint}"
        current_date = end_date
        all_data = []
        while current_date >= start_date:
            token = self._get_token()
            headers = {
                'accept': '*/*',
                "Authorization": f"Bearer {token}"
            }
            if not token:
                logger.error("Cannot download data without a token.")
                break

            chunk_start_date = current_date - relativedelta(years=1) + timedelta(days=1)
            if chunk_start_date < start_date:
                chunk_start_date = start_date

            params = {
                "Código da Estação": self.site_id,
                "Tipo Filtro Data": "DATA_LEITURA",
                "Data Inicial (yyyy-MM-dd)": chunk_start_date,
                "Data Final (yyyy-MM-dd)": current_date
            }
            logger.info(f"Fetching Brazil data for site {self.site_id} from {current_date.strftime('%Y-%m-%d')}")
            try:
                response = s.get(data_url, params=params, headers=headers)
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

            current_date = chunk_start_date - timedelta(days=1)
            time.sleep(0.2) # Be nice to the API

        return all_data

    def _download_nrt_data(self, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Downloads raw data from telemetered stations in 30-day chunks."""
        if not self.username or not self.password:
            return []

        if end_date is None: # I.e. get the most recent
            end_date = datetime.strftime(datetime.today(), "%Y-%m-%d")

        if start_date is None: 
            start_date = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=30)
            start_date = datetime.strftime(start_date, "%Y-%m-%d")

        # Convert to datetime
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Now we check whether the station has telemetry 
        metadata = self._get_station_metadata()
        metadata.columns = metadata.columns.str.upper()
        has_telemetry = bool(metadata['TIPO_ESTACAO_TELEMETRICA'].iloc[0])
        if not has_telemetry: 
            return []

        # Otherwise we work out the start and end dates for the telemetry data 
        telemetry_start_date = pd.to_datetime(metadata['DATA_PERIODO_TELEMETRICA_INICIO'].iloc[0])
        telemetry_end_date = pd.to_datetime(metadata['DATA_PERIODO_TELEMETRICA_FIM'].iloc[0])
        if telemetry_end_date is None: 
            telemetry_end_date = datetime.today() 

        if start_date < telemetry_start_date: 
            start_date = telemetry_start_date

        if end_date > telemetry_end_date: 
            end_date = telemetry_end_date
        elif end_date < start_date:
            return []

        # Set up requests session
        s = utils.requests_retry_session()
        endpoint = "EstacoesTelemetricas/HidroinfoanaSerieTelemetricaAdotada/v1"
        data_url = f"{self.BASE_URL}/{endpoint}" 

        # This API returns data for timesteps *prior* to the given start date, so it makes sense to work back in time
        current_date = end_date 
        all_data = []
        while current_date >= start_date:
            token = self._get_token()
            headers = {
                'accept': '*/*',
                "Authorization": f"Bearer {token}"
            }
            if not token:
                logger.error("Cannot download data without a token.")
                break

            chunk_start_date = current_date - timedelta(days=29)
            if chunk_start_date < start_date:
                chunk_start_date = start_date

            params = {
                "Código da Estação": self.site_id,
                "Tipo Filtro Data": "DATA_LEITURA",
                "Data de Busca": current_date,
                "Range Intervalo de busca": "DIAS_30",
            }

            logger.info(f"Fetching Brazil data for site {self.site_id} from {current_date.strftime('%Y-%m-%d')}")
            try:
                response = s.get(data_url, params=params, headers=headers)
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

            current_date = chunk_start_date - timedelta(days=1)
            time.sleep(0.2) # Be nice to the API

        return all_data

    def _parse_nrt_data(self, raw_data: List[Dict[str, Any]], variable: str) -> pd.DataFrame:
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