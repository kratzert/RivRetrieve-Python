"""Fetcher for Italy-Toscany river gauge data from the Toscana SIR services."""

import logging
import re
from io import StringIO
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests
from pyproj import Transformer

from . import base, constants, utils

logger = logging.getLogger(__name__)


class ItalyToscanyFetcher(base.RiverDataFetcher):
    """Fetches daily river gauge data for Toscana from the public SIR services.

    Data source:
        - monitoring website: https://www.sir.toscana.it/monitoraggio/stazioni.php?type=idro
        - historical archive portal: https://www.sir.toscana.it/consistenza-rete

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)

    Data description and API:
        - archive data description: https://www.sir.toscana.it/consistenza-rete
        - GIS layers overview for idrometers: https://www.sir.toscana.it/strati-gis

    Terms of use:
        - data usage notes for archived data: https://www.sir.toscana.it/consistenza-rete
    """

    METADATA_URL = (
        "https://geo.sir.toscana.it/geoserver/geo/ows"
        "?service=WFS&version=1.0.0&request=GetFeature"
        "&typeName=geo:cf_idrometri&outputFormat=application/json"
    )
    STATION_TABLE_URL = "https://www.sir.toscana.it/monitoraggio/stazioni.php?type=idro"
    ARCHIVE_URL = "https://www.sir.toscana.it/archivio/download.php"
    SOURCE = "Settore Idrologico Regione Toscana - SIR"
    COUNTRY = "Italy"
    REQUEST_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0 Safari/537.36"
        )
    }
    VARIABLE_MAP = {
        constants.DISCHARGE_DAILY_MEAN: {
            "archive_id": "idro_p",
        },
        constants.STAGE_DAILY_MEAN: {
            "archive_id": "idro_l",
        },
    }
    TABLE_PATTERN = re.compile(r"\w+\[\d+\]\s*=\s*new Array\((.*?)\);")

    def __init__(self):
        self._transformer = Transformer.from_crs("EPSG:3003", "EPSG:4326", always_xy=True)

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Italy-Toscany gauge metadata."""
        return utils.load_cached_metadata_csv("italy_toscany")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(ItalyToscanyFetcher.VARIABLE_MAP.keys())

    @staticmethod
    def _empty_data_frame(variable: str) -> pd.DataFrame:
        return pd.DataFrame(columns=[constants.TIME_INDEX, variable]).set_index(constants.TIME_INDEX)

    @staticmethod
    def _empty_metadata_frame() -> pd.DataFrame:
        columns = [
            constants.GAUGE_ID,
            constants.STATION_NAME,
            constants.RIVER,
            constants.LATITUDE,
            constants.LONGITUDE,
            constants.ALTITUDE,
            constants.AREA,
            constants.COUNTRY,
            constants.SOURCE,
            "municipality",
            "province",
            "basin",
            "hydro_zone",
            "zero_idrometrico",
        ]
        return pd.DataFrame(columns=columns).set_index(constants.GAUGE_ID)

    @staticmethod
    def _clean_text(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None

        text = str(value).strip()
        if not text or text == "-":
            return None
        return text

    @staticmethod
    def _clean_station_name(value: Any) -> Optional[str]:
        text = ItalyToscanyFetcher._clean_text(value)
        if text is None:
            return None
        return re.sub(r"\s+\([^)]*\)\s*$", "", text).strip() or None

    def _request_json(self, session: requests.Session, url: str) -> Any:
        response = session.get(url, headers=self.REQUEST_HEADERS, timeout=60)
        response.raise_for_status()
        return response.json()

    def _request_text(self, session: requests.Session, url: str, params: Optional[dict[str, str]] = None) -> str:
        response = session.get(url, params=params, headers=self.REQUEST_HEADERS, timeout=60)
        response.raise_for_status()
        return response.text

    def _parse_metadata_geojson(self, payload: Any) -> pd.DataFrame:
        features = payload.get("features", []) if isinstance(payload, dict) else []
        rows = []

        for feature in features:
            props = feature.get("properties", {}) if isinstance(feature, dict) else {}
            coords = (feature.get("geometry") or {}).get("coordinates", []) if isinstance(feature, dict) else []
            longitude = np.nan
            latitude = np.nan

            if isinstance(coords, list) and len(coords) >= 2:
                try:
                    longitude, latitude = self._transformer.transform(float(coords[0]), float(coords[1]))
                except (TypeError, ValueError):
                    longitude = np.nan
                    latitude = np.nan

            rows.append(
                {
                    constants.GAUGE_ID: self._clean_text(props.get("id_stazione")),
                    constants.STATION_NAME: self._clean_station_name(props.get("nome")),
                    constants.RIVER: None,
                    constants.LATITUDE: pd.to_numeric(latitude, errors="coerce"),
                    constants.LONGITUDE: pd.to_numeric(longitude, errors="coerce"),
                    constants.ALTITUDE: pd.to_numeric(props.get("quota"), errors="coerce"),
                    constants.AREA: np.nan,
                    constants.COUNTRY: self.COUNTRY,
                    constants.SOURCE: self.SOURCE,
                    "municipality": self._clean_text(props.get("comune")),
                    "province": self._clean_text(props.get("provincia")),
                    "basin": None,
                    "hydro_zone": self._clean_text(props.get("zona")),
                    "zero_idrometrico": pd.to_numeric(props.get("zero_idrometrico"), errors="coerce"),
                }
            )

        if not rows:
            return self._empty_metadata_frame()

        df = pd.DataFrame(rows)
        df = df.dropna(subset=[constants.GAUGE_ID])
        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return df.set_index(constants.GAUGE_ID)

    @classmethod
    def _parse_station_table(cls, text: str) -> pd.DataFrame:
        rows = []

        for match in cls.TABLE_PATTERN.finditer(text):
            values = re.findall(r'"(.*?)"', match.group(1))
            if len(values) < 6 or not values[0].startswith("TOS"):
                continue

            rows.append(
                {
                    constants.GAUGE_ID: values[0].strip(),
                    constants.RIVER: cls._clean_text(values[1]),
                    constants.STATION_NAME: cls._clean_station_name(values[2]),
                    "province": cls._clean_text(values[3]),
                    "basin": cls._clean_text(values[4]),
                    "hydro_zone": cls._clean_text(values[5]),
                }
            )

        if not rows:
            return pd.DataFrame(
                columns=[
                    constants.GAUGE_ID,
                    constants.RIVER,
                    constants.STATION_NAME,
                    "province",
                    "basin",
                    "hydro_zone",
                ]
            ).set_index(constants.GAUGE_ID)

        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return df.set_index(constants.GAUGE_ID)

    def get_metadata(self) -> pd.DataFrame:
        """Fetches live metadata for Italy-Toscany stations.

        Merges the live GIS layer with the public monitoring table and returns
        a DataFrame indexed by ``constants.GAUGE_ID``.
        """
        session = utils.requests_retry_session(
            retries=6,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
        )

        try:
            metadata_payload = self._request_json(session, self.METADATA_URL)
            station_table_text = self._request_text(session, self.STATION_TABLE_URL)
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch Italy-Toscany metadata: {exc}")
            raise
        except ValueError as exc:
            logger.error(f"Failed to decode Italy-Toscany metadata: {exc}")
            raise

        metadata_df = self._parse_metadata_geojson(metadata_payload)
        station_df = self._parse_station_table(station_table_text)

        if metadata_df.empty and station_df.empty:
            return self._empty_metadata_frame()

        merged = metadata_df.join(station_df, how="outer", rsuffix="_station")
        merged[constants.STATION_NAME] = merged[f"{constants.STATION_NAME}_station"].combine_first(
            merged[constants.STATION_NAME]
        )
        merged[constants.RIVER] = merged[f"{constants.RIVER}_station"].combine_first(merged[constants.RIVER])
        merged["province"] = merged["province_station"].combine_first(merged["province"])
        merged["basin"] = merged["basin"].combine_first(merged["basin_station"])
        merged["hydro_zone"] = merged["hydro_zone_station"].combine_first(merged["hydro_zone"])

        merged = merged.drop(
            columns=[
                f"{constants.STATION_NAME}_station",
                f"{constants.RIVER}_station",
                "province_station",
                "basin_station",
                "hydro_zone_station",
            ],
            errors="ignore",
        )
        merged[constants.COUNTRY] = self.COUNTRY
        merged[constants.SOURCE] = self.SOURCE

        merged = merged.dropna(subset=[constants.LATITUDE, constants.LONGITUDE])
        merged = merged.sort_index()
        return merged

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> str:
        session = utils.requests_retry_session(
            retries=6,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
        )
        params = {
            "IDST": self.VARIABLE_MAP[variable]["archive_id"],
            "IDS": str(gauge_id),
        }
        return self._request_text(session, self.ARCHIVE_URL, params=params)

    @staticmethod
    def _parse_archive_csv(raw_data: str, variable: str) -> pd.DataFrame:
        if not raw_data.strip():
            return ItalyToscanyFetcher._empty_data_frame(variable)

        lines = raw_data.splitlines()
        start_idx = next((idx for idx, line in enumerate(lines) if "gg/mm/aaaa" in line), None)
        if start_idx is None:
            return ItalyToscanyFetcher._empty_data_frame(variable)

        data_lines = [line for line in lines[start_idx:] if line.strip() and not line.strip().startswith(";;;")]
        if len(data_lines) <= 1:
            return ItalyToscanyFetcher._empty_data_frame(variable)

        try:
            raw_df = pd.read_csv(
                StringIO("\n".join(data_lines)),
                sep=";",
                decimal=",",
                quotechar='"',
                engine="python",
                dtype=str,
                on_bad_lines="skip",
            )
        except Exception as exc:
            logger.error(f"Failed to parse Italy-Toscany archive payload: {exc}")
            return ItalyToscanyFetcher._empty_data_frame(variable)

        if raw_df.empty or raw_df.shape[1] < 2:
            return ItalyToscanyFetcher._empty_data_frame(variable)

        columns = ["date", "value", "quality_flag"]
        raw_df = raw_df.iloc[:, : min(raw_df.shape[1], len(columns))]
        raw_df.columns = columns[: raw_df.shape[1]]

        parsed = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(raw_df["date"], format="%d/%m/%Y", errors="coerce"),
                variable: pd.to_numeric(
                    raw_df["value"]
                    .astype(str)
                    .str.extract(r"([-+]?\d+(?:[.,]\d+)?)", expand=False)
                    .str.replace(",", ".", regex=False),
                    errors="coerce",
                ),
            }
        ).dropna(subset=[constants.TIME_INDEX, variable])

        if parsed.empty:
            return ItalyToscanyFetcher._empty_data_frame(variable)

        parsed = parsed.drop_duplicates(subset=[constants.TIME_INDEX]).sort_values(constants.TIME_INDEX)
        return parsed.set_index(constants.TIME_INDEX)

    def _parse_data(self, gauge_id: str, raw_data: str, variable: str) -> pd.DataFrame:
        return self._parse_archive_csv(raw_data, variable)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable.

        This method retrieves the requested data from the provider's archive service,
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
            Exception: For unexpected download or parsing errors.
        """
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(str(gauge_id), variable, start_date, end_date)
            df = self._parse_data(str(gauge_id), raw_data, variable)
        except Exception as exc:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {exc}")
            return self._empty_data_frame(variable)

        if df.empty:
            return df

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        return df[(df.index >= start_dt) & (df.index <= end_dt)]
