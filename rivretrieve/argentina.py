"""Fetcher for Argentina river data from INA Alerta."""

import logging
from typing import Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class ArgentinaFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the Argentine national hydrological system.

        Data source:
            Portal de datos del Sistema de Información Hidrológica de la Cuenca del Plata – DSIyAH INA
            https://alerta.ina.gob.ar/pub/gui

        Supported variables:
            - ``constants.STAGE_INSTANT`` (m)
            - ``constants.STAGE_DAILY_MEAN`` (m)
            - ``constants.STAGE_HOURLY_MEAN`` (m)
            - ``constants.DISCHARGE_INSTANT`` (m³/s)
            - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
            - ``constants.DISCHARGE_DAILY_MAX`` (m³/s)
            - ``constants.DISCHARGE_DAILY_MIN`` (m³/s)
            - ``constants.DISCHARGE_HOURLY_MEAN`` (m³/s)
            - ``constants.WATER_TEMPERATURE_INSTANT`` (°C)
            - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)
            - ``constants.WATER_TEMPERATURE_HOURLY_MEAN`` (°C)

        API description:
            https://alerta.ina.gob.ar/pub/gui/apibase

        Metadata endpoints:
            - https://alerta.ina.gob.ar/pub/datos/estaciones&&type=H&format=json
            - https://alerta.ina.gob.ar/pub/datos/estaciones&&type=A&format=json

        Terms of use:
            Data provided openly by DSIyAH INA. Check provider website for applicable conditions.
    """

    METADATA_URL_TEMPLATE = (
        "https://alerta.ina.gob.ar/pub/datos/estaciones&&type={stype}&format=json"
    )

    DATA_URL = "https://alerta.ina.gob.ar/pub/datos/datos"

    # Variables map
    VAR_ID_MAP = {
        constants.DISCHARGE_INSTANT: 4,
        constants.DISCHARGE_DAILY_MEAN: 40,
        constants.DISCHARGE_DAILY_MAX: 68,
        constants.DISCHARGE_DAILY_MIN: 69,
        constants.DISCHARGE_HOURLY_MEAN: 87,

        constants.STAGE_INSTANT: 2,
        constants.STAGE_DAILY_MEAN: 39,
        constants.STAGE_HOURLY_MEAN: 85,

        constants.WATER_TEMPERATURE_INSTANT: 73,
        constants.WATER_TEMPERATURE_DAILY_MEAN: 73,
        constants.WATER_TEMPERATURE_HOURLY_MEAN: 73,
    }

    SUPPORTED_VARIABLES = tuple(VAR_ID_MAP.keys())

    #  Cached metadata
    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        return utils.load_cached_metadata_csv("argentina")

    # Live metadata download
    def get_metadata(self) -> pd.DataFrame:
        """Fetches metadata for both H and A station types, merges them,
        and standardizes to rivretrieve's global metadata format."""
        station_types = ["H", "A"]
        frames = []

        for stype in station_types:
            url = self.METADATA_URL_TEMPLATE.format(stype=stype)
            logger.info(f"Fetching metadata from: {url}")

            try:
                r = requests.get(url, timeout=25)
                r.raise_for_status()
                data = r.json().get("data", [])

                if not isinstance(data, list):
                    logger.warning(f"Unexpected format for type={stype}, skipping.")
                    continue

                df = pd.DataFrame(data)
                if df.empty:
                    continue

                # Standard columns
                df = df.rename(columns={
                    "sitecode": constants.GAUGE_ID,
                    "nombre": constants.STATION_NAME,
                    "rio": constants.RIVER,
                    "lat": constants.LATITUDE,
                    "lon": constants.LONGITUDE,
                })

                df[constants.LATITUDE] = pd.to_numeric(df[constants.LATITUDE], errors="coerce")
                df[constants.LONGITUDE] = pd.to_numeric(df[constants.LONGITUDE], errors="coerce")

                df[constants.COUNTRY] = "Argentina"
                df[constants.SOURCE] = "INA Alerta"
                df["station_type"] = stype

                # Add missing global metadata fields
                for col in [
                    constants.ALTITUDE,
                    constants.AREA,
                ]:
                    if col not in df.columns:
                        df[col] = np.nan

                frames.append(df)

            except Exception as e:
                logger.error(f"Failed to fetch metadata type={stype}: {e}")

        if not frames:
            logger.error("No metadata returned.")
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).reset_index(drop=True)

        logger.info(f"Fetched {len(df)} gauge metadata records.")
        return df

    # Supported variables
    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return ArgentinaFetcher.SUPPORTED_VARIABLES

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str):
        if variable not in self.VAR_ID_MAP:
            raise ValueError(f"Variable not supported: {variable}")

        var_id = self.VAR_ID_MAP[variable]

        url = (
            f"{self.DATA_URL}"
            f"&timeStart={start_date}"
            f"&timeEnd={end_date}"
            f"&siteCode={gauge_id}"
            f"&varId={var_id}"
            f"&format=json"
        )

        logger.info(f"Fetching INA data: {url}")

        try:
            r = requests.get(url, timeout=25)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"Failed to download INA data for {gauge_id}/{variable}: {e}")
            return {"data": []}

    def _parse_data(self, gauge_id: str, raw_json: dict, variable: str) -> pd.DataFrame:
        data = raw_json.get("data", [])
        if not data:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = pd.DataFrame(data)

        if "timestart" not in df or "valor" not in df:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        df = df.rename(columns={"timestart": constants.TIME_INDEX, "valor": variable})
        df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], errors="coerce")
        df[variable] = pd.to_numeric(df[variable], errors="coerce")

        df = (
            df[[constants.TIME_INDEX, variable]]
            .dropna()
            .drop_duplicates(subset=[constants.TIME_INDEX])
            .sort_values(constants.TIME_INDEX)
            .set_index(constants.TIME_INDEX)
        )

        return df

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:

        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_json = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_json, variable)
            return df.loc[(df.index >= start_date) & (df.index <= end_date)]
        except Exception as e:
            logger.error(f"Failed to parse INA data for {gauge_id}/{variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])
