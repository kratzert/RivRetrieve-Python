"""Fetcher for Spanish river gauge data from ROAN."""

import io
import logging
import zipfile
from typing import Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class SpainFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Spain's National Hydrological Data System (ROAN).

    Data Source: MITECO (https://www.miteco.gob.es/es/agua/temas/evaluacion-de-los-recursos-hidricos/sistema-informacion-anuario-aforos/default.aspx)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN (m³/s)``
    """

    METADATA_ZIP_URL = "https://www.miteco.gob.es/content/dam/miteco/es/agua/temas/evaluacion-de-los-recursos-hidricos/sistema-informacion-anuario-aforos/listado-estaciones-aforo.zip"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available Spanish gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("spain")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (constants.DISCHARGE_DAILY_MEAN,)

    def get_metadata(self) -> pd.DataFrame:
        """Downloads and returns ROAN station metadata from MITECO.

        Columns are translated from Spanish to English based on the mapping
        provided in issue #28.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        logger.info(f"Downloading stations metadata from {self.METADATA_ZIP_URL}")
        try:
            resp = utils.requests_retry_session().get(self.METADATA_ZIP_URL)
            resp.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                target_file = [f for f in z.namelist() if "Situac" in f and "Rio" in f]
                if not target_file:
                    raise FileNotFoundError("Could not find 'Situac...Rio.csv' in ZIP.")
                csv_name = target_file[0]
                logger.info(f"Found metadata file in ZIP: {csv_name}")

                with z.open(csv_name) as f:
                    df = pd.read_csv(f, encoding="latin1", sep=";", low_memory=False)

            df.columns = [c.strip() for c in df.columns]

            rename_map = {
                "COD_HIDRO": constants.GAUGE_ID,
                "NOM_ANUARIO": constants.STATION_NAME,
                "RIO": constants.RIVER,
                "COTA_Z": constants.ALTITUDE,
                "CUENCA_TOTAL": constants.AREA,
            }
            df = df.rename(columns=rename_map)
            df[constants.COUNTRY] = "Spain"
            df[constants.SOURCE] = "ROAN"

            # The metadata contains a few unnamed and unused columns. We drop them.
            drop_cols = [x for x in df.columns if x.startswith("Unnamed: ")]
            df = df.drop(columns=drop_cols)

            if constants.GAUGE_ID in df.columns:
                df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str)
                df = df.set_index(constants.GAUGE_ID)
            else:
                logger.error("GAUGE_ID column not found after renaming.")
                return pd.DataFrame()

            # Convert types
            for col in [constants.ALTITUDE, constants.AREA]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            logger.info(f"Loaded metadata with {len(df)} stations.")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading metadata ZIP: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing metadata: {e}")
            raise

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Downloads daily streamflow data from the ROAN web service."""
        if variable != constants.DISCHARGE_DAILY_MEAN:
            logger.error(f"Unsupported variable: {variable} for SpainFetcher")
            return None

        start_year = pd.to_datetime(start_date).year
        end_year = pd.to_datetime(end_date).year
        origin = 1008  # fixed origin code for ROAN service

        url = (
            "https://sig.mapama.gob.es/WebServices/clientews/redes-seguimiento/"
            f"default.aspx?nombre=ROAN_RIOS_DIARIO_CAUDAL&claves=INDROEA|ANO_INI|ANO_FIN&valores={gauge_id}|{start_year}|{end_year}&origen={origin}"
        )

        logger.info(f"Fetching data from: {url}")
        try:
            s = utils.requests_retry_session()
            resp = s.get(url)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding
            soup = __import__("bs4").BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table")

            if table is None:
                logger.warning(f"No data table found for gauge {gauge_id} between {start_year} and {end_year}")
                return None

            df = pd.read_html(io.StringIO(str(table)))[0]
            return df
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading data for gauge {gauge_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing HTML table for gauge {gauge_id}: {e}")
            return None

    def _parse_data(self, gauge_id: str, raw_data: Optional[pd.DataFrame], variable: str) -> pd.DataFrame:
        """Parses the raw DataFrame from _download_data."""
        if raw_data is None or raw_data.empty:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = raw_data.copy()

        try:
            # Clean column names
            df.columns = [c.strip() for c in df.columns]

            # Identify numeric columns (from Oct onward)
            num_cols = df.columns[3:]

            # Convert numeric values (e.g., 074 -> 0.74)
            df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce") / 100

            # Reshape: one row per date
            month_map = {
                "Oct": 10,
                "Nov": 11,
                "Dic": 12,
                "Ene": 1,
                "Feb": 2,
                "Mar": 3,
                "Abr": 4,
                "May": 5,
                "Jun": 6,
                "Jul": 7,
                "Ago": 8,
                "Sep": 9,
            }

            df_long = df.melt(
                id_vars=["Estación", "Año", "Día"], var_name="Mes", value_name=constants.DISCHARGE_DAILY_MEAN
            )

            # Handle hydrological year (October to September)
            df_long[["Año_ini", "Año_fin"]] = df_long["Año"].str.split("-", expand=True).astype(int)
            df_long["Mes_num"] = df_long["Mes"].map(month_map)

            def get_calendar_year(row):
                if row["Mes_num"] >= 10:  # Oct, Nov, Dec
                    return row["Año_ini"]
                else:  # Jan to Sep
                    return row["Año_fin"]

            df_long["Año_real"] = df_long.apply(get_calendar_year, axis=1)

            # Build full date column
            df_long[constants.TIME_INDEX] = pd.to_datetime(
                dict(year=df_long["Año_real"], month=df_long["Mes_num"], day=df_long["Día"]), errors="coerce"
            )

            # Keep valid data only
            df_long = df_long.dropna(subset=[constants.TIME_INDEX, constants.DISCHARGE_DAILY_MEAN])

            # Final clean DataFrame
            df_final = (
                df_long[[constants.TIME_INDEX, constants.DISCHARGE_DAILY_MEAN]]
                .sort_values(constants.TIME_INDEX)
                .set_index(constants.TIME_INDEX)
            )

            return df_final

        except Exception as e:
            logger.error(f"Error parsing data for gauge {gauge_id}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable.

        Args:
            gauge_id: The site-specific identifier for the gauge.
            variable: The variable to fetch.
            start_date: Optional start date in 'YYYY-MM-DD' format.
            end_date: Optional end date in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: A pandas DataFrame indexed by time.
        """
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)

            if not df.empty:
                start_date_dt = pd.to_datetime(start_date)
                end_date_dt = pd.to_datetime(end_date)
                df = df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
            return df
        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
