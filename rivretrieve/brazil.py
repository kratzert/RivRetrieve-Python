"""Fetcher for Brazilian river gauge data from ANA Hidroweb."""

import io
import logging
import re
import zipfile
from typing import Optional

import pandas as pd
import requests

from . import base, utils

logger = logging.getLogger(__name__)


class BrazilFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Brazil's ANA Hidroweb."""

    BASE_URL = "https://www.snirh.gov.br/hidroweb/rest/api/documento/convencionais"

    @staticmethod
    def get_sites() -> pd.DataFrame:
        """Retrieves a DataFrame of available Brazilian gauge sites."""
        return utils.load_sites_csv("brazil")

    def _download_data(self, variable: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Downloads and extracts the data file."""
        params = {"tipo": 3, "documentos": self.site_id}
        s = utils.requests_retry_session()

        try:
            response = s.get(self.BASE_URL, params=params, headers=utils.DEFAULT_HEADERS)
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                # Find the correct inner zip file (vazoes for discharge, cotas for stage)
                inner_zip_name = None
                pattern = f"^{self.site_id}/(vazoes|cotas)_{self.site_id}.zip$"
                for name in zf.namelist():
                    if re.match(pattern, name):
                        if (variable == "discharge" and "vazoes" in name) or \
                           (variable == "stage" and "cotas" in name):
                            inner_zip_name = name
                            break

                if not inner_zip_name:
                    logger.warning(f"Could not find inner zip for site {self.site_id}, variable {variable}")
                    return None

                with zf.open(inner_zip_name) as inner_zf_file:
                    with zipfile.ZipFile(io.BytesIO(inner_zf_file.read())) as inner_zf:
                        data_file_name = inner_zf.namelist()[0]
                        with inner_zf.open(data_file_name) as data_file:
                            # The files are ISO-8859-1 encoded
                            raw_df = pd.read_csv(data_file,
                                                 sep=";",
                                                 decimal=",",
                                                 skiprows=13,
                                                 encoding='ISO-8859-1',
                                                 engine='python')
                            return raw_df

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading from Hidroweb for site {self.site_id}: {e}")
            return None
        except zipfile.BadZipFile:
            logger.error(f"Bad zip file for site {self.site_id}")
            return None
        except Exception as e:
            logger.error(f"Error processing file for site {self.site_id}: {e}")
            return None

    def _parse_data(self, raw_df: Optional[pd.DataFrame], variable: str) -> pd.DataFrame:
        """Parses the raw DataFrame."""
        col_name = utils.get_column_name(variable)
        if raw_df is None or raw_df.empty:
            return pd.DataFrame(columns=["Date", col_name])

        try:
            id_cols = ["EstacaoCodigo", "NivelConsistencia", "Data", "Hora"]
            if variable == "discharge":
                prefix = "Vazao"
                id_cols.append("MetodoObtencaoVazoes")
            else:  # stage
                prefix = "Cota"

            # Select relevant columns
            value_cols = [col for col in raw_df.columns if col.startswith(prefix)]
            status_cols = [col for col in value_cols if col.endswith("Status")]
            value_cols = [col for col in value_cols if not col.endswith("Status")]

            df = raw_df[id_cols + value_cols + status_cols].copy()

            # Rename status columns for pivot
            rename_map = {sc: sc.replace("Status", "_Status") for sc in status_cols}
            df = df.rename(columns=rename_map)

            # Rename value columns for pivot
            rename_map = {vc: f"{vc}_Value" for vc in value_cols}
            df = df.rename(columns=rename_map)

            # Pivot longer
            df_long = pd.melt(df, id_vars=id_cols, var_name="day_type", value_name="Value")
            df_long[['day', 'Type']] = df_long['day_type'].str.split('_', expand=True)
            df_long['day'] = df_long['day'].str.replace(prefix, "").astype(int)

            # Separate Value and Status
            df_values = df_long[df_long['Type'] == 'Value'].copy()
            df_status = df_long[df_long['Type'] == 'Status'].copy()
            df_status = df_status.rename(columns={"Value": "Status"})

            # Merge back Value and Status
            id_cols_melt = id_cols + ['day']
            df_merged = pd.merge(df_values[id_cols_melt + ['Value']],
                                 df_status[id_cols_melt + ['Status']],
                                 on=id_cols_melt,
                                 how='left')

            # Create Date column
            df_merged['Data'] = pd.to_datetime(df_merged['Data'], format='%d/%m/%Y')
            df_merged['year'] = df_merged['Data'].dt.year
            df_merged['month'] = df_merged['Data'].dt.month

            # Handle potential errors in make_date by coercing
            date_df = df_merged[['year', 'month', 'day']].copy()
            df_merged['Date'] = pd.to_datetime(date_df, errors='coerce')
            df_merged = df_merged.dropna(subset=['Date'])

            df_merged['Value'] = pd.to_numeric(df_merged['Value'], errors='coerce')
            if variable == "stage":  # cm to m
                df_merged['Value'] = df_merged['Value'] / 100.0

            df_final = df_merged[["Date", "Value"]].rename(columns={"Value": col_name})
            return df_final.dropna().sort_values(by="Date").reset_index(drop=True)

        except Exception as e:
            logger.error(f"Error parsing data for site {self.site_id}: {e}")
            return pd.DataFrame(columns=["Date", col_name])

    def get_data(self, variable: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches and parses Brazilian river gauge data."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        utils.get_column_name(variable)  # Validate variable

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
