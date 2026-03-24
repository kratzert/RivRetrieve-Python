"""Fetcher for Netherlands river gauge data from Rijkswaterstaat WaterWebservices."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests
from pyproj import Transformer

from . import base, constants, utils

logger = logging.getLogger(__name__)


class NetherlandsFetcher(base.RiverDataFetcher):
    """Fetches Dutch river gauge data from Rijkswaterstaat WaterWebservices.

    Data source:
        - website: https://rijkswaterstaatdata.nl/waterdata/

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.STAGE_INSTANT`` (m)
        - ``constants.WATER_TEMPERATURE_DAILY_MEAN`` (°C)
        - ``constants.WATER_TEMPERATURE_INSTANT`` (°C)

    Data description and API:
        - current API docs: https://ddapi20-waterwebservices.rijkswaterstaat.nl/swagger-ui/index.html

    Terms of use:
        - see https://rijkswaterstaatdata.nl/waterdata/

    Notes:
        - The Rijkswaterstaat catalog exposes discharge (``Q``), water level
          (``WATHTE``), and water temperature (``T``) for surface water, which are
          mapped to the corresponding RivRetrieve daily and instantaneous variables.
        - Station metadata is filtered to stations that advertise at least one
          supported surface-water variable.
        - Rijkswaterstaat often serves observations at 10-minute resolution. This
          fetcher retrieves raw observations in monthly windows and aggregates them to
          daily means for daily products.
        - Stage values are converted from centimeters to meters.
        - Provider sentinel missing values are filtered before aggregation.
    """

    SOURCE = "Rijkswaterstaat WaterWebservices"
    COUNTRY = "Netherlands"
    LOCAL_TIMEZONE = "Europe/Amsterdam"
    CATALOG_ENDPOINTS = (
        "https://ddapi20-waterwebservices.rijkswaterstaat.nl/METADATASERVICES/OphalenCatalogus",
        "https://waterwebservices.rijkswaterstaat.nl/METADATASERVICES_DBO/OphalenCatalogus",
    )
    DATA_ENDPOINTS = (
        "https://ddapi20-waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen",
        "https://waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES_DBO/OphalenWaarnemingen",
    )
    VARIABLE_MAP = {
        constants.DISCHARGE_DAILY_MEAN: {
            "aggregate_daily": True,
            "variants": (
                {
                    "grootheid": "Q",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "m3/s",
                    "hoedanigheid": "NVT",
                    "unit_factor": 1.0,
                },
                {
                    "grootheid": "Q",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "m3/d",
                    "hoedanigheid": "NVT",
                    "unit_factor": 1.0 / 86400.0,
                },
            ),
        },
        constants.DISCHARGE_INSTANT: {
            "aggregate_daily": False,
            "variants": (
                {
                    "grootheid": "Q",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "m3/s",
                    "hoedanigheid": "NVT",
                    "unit_factor": 1.0,
                },
                {
                    "grootheid": "Q",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "m3/d",
                    "hoedanigheid": "NVT",
                    "unit_factor": 1.0 / 86400.0,
                },
            ),
        },
        constants.STAGE_DAILY_MEAN: {
            "aggregate_daily": True,
            "variants": (
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "NAP",
                    "unit_factor": 0.01,
                },
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "MSL",
                    "unit_factor": 0.01,
                },
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "TAW",
                    "unit_factor": 0.01,
                },
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "PLAATSLR",
                    "unit_factor": 0.01,
                },
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "NVT",
                    "unit_factor": 0.01,
                },
            ),
        },
        constants.STAGE_INSTANT: {
            "aggregate_daily": False,
            "variants": (
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "NAP",
                    "unit_factor": 0.01,
                },
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "MSL",
                    "unit_factor": 0.01,
                },
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "TAW",
                    "unit_factor": 0.01,
                },
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "PLAATSLR",
                    "unit_factor": 0.01,
                },
                {
                    "grootheid": "WATHTE",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "cm",
                    "hoedanigheid": "NVT",
                    "unit_factor": 0.01,
                },
            ),
        },
        constants.WATER_TEMPERATURE_DAILY_MEAN: {
            "aggregate_daily": True,
            "variants": (
                {
                    "grootheid": "T",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "oC",
                    "hoedanigheid": "NVT",
                    "unit_factor": 1.0,
                },
            ),
        },
        constants.WATER_TEMPERATURE_INSTANT: {
            "aggregate_daily": False,
            "variants": (
                {
                    "grootheid": "T",
                    "compartiment_code": "OW",
                    "compartment_names": {"Oppervlaktewater"},
                    "eenheid": "oC",
                    "hoedanigheid": "NVT",
                    "unit_factor": 1.0,
                },
            ),
        },
    }
    CATALOG_BODY = {
        "CatalogusFilter": {
            "Compartimenten": True,
            "Grootheden": True,
            "Parameters": True,
            "Eenheden": True,
            "Hoedanigheden": True,
            "Groeperingen": False,
        }
    }

    def __init__(self):
        self._catalog_cache: Optional[dict[str, Any]] = None

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Netherlands gauge metadata."""
        return utils.load_cached_metadata_csv("netherlands")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(NetherlandsFetcher.VARIABLE_MAP.keys())

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
        ]
        return pd.DataFrame(columns=columns).set_index(constants.GAUGE_ID)

    @staticmethod
    def _empty_data_frame(variable: str) -> pd.DataFrame:
        return pd.DataFrame(columns=[constants.TIME_INDEX, variable]).set_index(constants.TIME_INDEX)

    @staticmethod
    def _clean_provider_value(value: Any) -> float:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return np.nan
        if abs(float(numeric)) >= 9.9e11:
            return np.nan
        return float(numeric)

    def _request_json(self, endpoints: Iterable[str], body: dict[str, Any]) -> dict[str, Any]:
        session = utils.requests_retry_session(
            retries=6,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
        )
        last_error: Optional[Exception] = None

        for endpoint in endpoints:
            try:
                response = session.post(endpoint, json=body, timeout=60)
                response.raise_for_status()
                return response.json()
            except (requests.exceptions.RequestException, ValueError) as exc:
                last_error = exc
                logger.warning(f"Netherlands request failed for {endpoint}: {exc}")

        if last_error is not None:
            raise last_error
        raise RuntimeError("No endpoints were configured for NetherlandsFetcher.")

    def _get_catalog(self) -> dict[str, Any]:
        if self._catalog_cache is None:
            self._catalog_cache = self._request_json(self.CATALOG_ENDPOINTS, self.CATALOG_BODY)
        return self._catalog_cache

    @staticmethod
    def _as_frame(payload: Any) -> pd.DataFrame:
        if isinstance(payload, list) and payload:
            return pd.DataFrame(payload)
        if isinstance(payload, list):
            return pd.DataFrame()
        if isinstance(payload, dict):
            return pd.DataFrame([payload])
        return pd.DataFrame()

    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> Optional[str]:
        for candidate in candidates:
            if candidate in df.columns:
                return candidate
        return None

    @staticmethod
    def _extract_nested_value(value: Any, *path: str) -> Any:
        current = value
        for key in path:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current

    @classmethod
    def _series_nested_value(cls, series: pd.Series, *path: str) -> pd.Series:
        return series.apply(lambda value: cls._extract_nested_value(value, *path))

    @staticmethod
    def _aligned_numeric_series(df: pd.DataFrame, columns: tuple[str, ...], default: float = np.nan) -> pd.Series:
        for column in columns:
            if column in df.columns:
                return pd.to_numeric(df[column], errors="coerce")
        return pd.Series(default, index=df.index, dtype=float)

    @classmethod
    def _extract_epsg_series(cls, df: pd.DataFrame) -> pd.Series:
        if "Coordinatenstelsel" not in df.columns:
            return pd.Series(25831, index=df.index, dtype="Int64")

        raw = df["Coordinatenstelsel"]
        if raw.apply(lambda value: isinstance(value, dict)).any():
            raw = raw.apply(
                lambda value: (
                    cls._extract_nested_value(value, "EPSG")
                    or cls._extract_nested_value(value, "Code")
                    or cls._extract_nested_value(value, "Waarde")
                    or cls._extract_nested_value(value, "Id")
                    if isinstance(value, dict)
                    else value
                )
            )

        return pd.to_numeric(raw, errors="coerce").fillna(25831).astype("Int64")

    @staticmethod
    def _build_transformers(epsg_codes: Iterable[int]) -> dict[int, Transformer]:
        transformers: dict[int, Transformer] = {}
        for epsg in epsg_codes:
            try:
                transformers[epsg] = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)
            except Exception:
                logger.warning(f"Skipping unsupported CRS EPSG:{epsg} in Netherlands metadata.")
        return transformers

    @classmethod
    def _transform_coordinates(cls, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        if df.empty:
            return pd.Series(dtype=float), pd.Series(dtype=float)

        direct_lat = cls._aligned_numeric_series(df, ("Lat", "lat", "Latitude", "latitude"))
        direct_lon = cls._aligned_numeric_series(df, ("Lon", "lon", "Longitude", "longitude"))
        if direct_lat.notna().any() and direct_lon.notna().any():
            return direct_lon, direct_lat

        x = cls._aligned_numeric_series(df, ("X", "x"))
        y = cls._aligned_numeric_series(df, ("Y", "y"))
        epsg = cls._extract_epsg_series(df)

        lon = pd.Series(np.nan, index=df.index, dtype=float)
        lat = pd.Series(np.nan, index=df.index, dtype=float)
        valid = x.notna() & y.notna() & epsg.notna()
        transformers = cls._build_transformers(sorted(set(epsg[valid].astype(int).tolist())))

        for code, transformer in transformers.items():
            mask = valid & (epsg.astype("Int64") == code)
            if not mask.any():
                continue
            transformed_lon, transformed_lat = transformer.transform(x[mask].to_numpy(), y[mask].to_numpy())
            lon.loc[mask] = transformed_lon
            lat.loc[mask] = transformed_lat

        return lon, lat

    def _get_supported_station_codes(self, variable: str) -> set[str]:
        config = self.VARIABLE_MAP[variable]
        catalog = self._get_catalog()
        meta = self._as_frame(catalog.get("AquoMetadataLijst"))
        locs = self._as_frame(catalog.get("LocatieLijst"))
        xref = self._as_frame(catalog.get("AquoMetadataLocatieLijst"))
        if meta.empty or locs.empty or xref.empty:
            return set()

        meta_key = self._pick_column(
            meta, ("AquoMetadata_MessageID", "AquoMetadataMessageID", "AquoMetadataId", "AquoMetadata_ID", "MessageID")
        )
        xref_meta_key = self._pick_column(
            xref, ("AquoMetaData_MessageID", "AquoMetadata_MessageID", "AquoMetadataMessageID", "AquoMetadataId")
        )
        xref_loc_key = self._pick_column(
            xref, ("Locatie_MessageID", "LocatieMessageID", "LocatieId", "Locatie_ID", "MessageID_Locatie")
        )
        loc_key = self._pick_column(
            locs,
            ("MessageID", "MessageId", "Locatie_MessageID", "LocatieMessageID", "LocatieId"),
        )
        loc_code = self._pick_column(locs, ("Code", "LocatieCode", "StationCode"))
        if not all([meta_key, xref_meta_key, xref_loc_key, loc_key, loc_code]):
            return set()

        meta = meta.copy()
        meta["grootheid_code"] = (
            self._series_nested_value(meta["Grootheid"], "Code") if "Grootheid" in meta.columns else None
        )
        meta["compartment_code"] = (
            self._series_nested_value(meta["Compartiment"], "Code") if "Compartiment" in meta.columns else None
        )
        meta["compartment_name"] = (
            self._series_nested_value(meta["Compartiment"], "Omschrijving") if "Compartiment" in meta.columns else None
        )

        allowed = pd.Series(False, index=meta.index)
        for variant in config["variants"]:
            variant_mask = meta["grootheid_code"].eq(variant["grootheid"]) & (
                meta["compartment_code"].eq(variant["compartiment_code"])
                | meta["compartment_name"].isin(variant["compartment_names"])
            )
            if variant.get("eenheid") is not None and "Eenheid" in meta.columns:
                eenheid_code = self._series_nested_value(meta["Eenheid"], "Code")
                variant_mask = variant_mask & eenheid_code.eq(variant["eenheid"])
            if variant.get("hoedanigheid") is not None and "Hoedanigheid" in meta.columns:
                hoedanigheid_code = self._series_nested_value(meta["Hoedanigheid"], "Code")
                variant_mask = variant_mask & hoedanigheid_code.eq(variant["hoedanigheid"])
            allowed = allowed | variant_mask
        matching_meta = meta.loc[allowed, [meta_key]]
        if matching_meta.empty:
            return set()

        matched_xref = xref[xref[xref_meta_key].isin(matching_meta[meta_key])]
        if matched_xref.empty:
            return set()

        matched_locs = locs[locs[loc_key].isin(matched_xref[xref_loc_key])]
        return set(matched_locs[loc_code].dropna().astype(str).str.strip())

    def get_metadata(self) -> pd.DataFrame:
        """Fetches live metadata for Dutch surface-water stations.

        Returns a DataFrame indexed by ``constants.GAUGE_ID`` and filtered to
        stations that advertise at least one supported RivRetrieve variable.
        """
        catalog = self._get_catalog()
        locs = self._as_frame(catalog.get("LocatieLijst"))
        if locs.empty:
            return self._empty_metadata_frame()

        loc_code = self._pick_column(locs, ("Code", "LocatieCode", "StationCode"))
        name_col = self._pick_column(locs, ("Naam", "LocatieNaam", "StationNaam"))
        if loc_code is None:
            return self._empty_metadata_frame()

        supported_codes: set[str] = set()
        for variable in self.get_available_variables():
            supported_codes.update(self._get_supported_station_codes(variable))
        if supported_codes:
            locs = locs[locs[loc_code].astype(str).str.strip().isin(supported_codes)]
        if locs.empty:
            return self._empty_metadata_frame()

        river_series = pd.Series(np.nan, index=locs.index, dtype=object)
        for column_name in ("Water", "Waternaam", "Omschrijving"):
            if column_name in locs.columns:
                river_series = locs[column_name]
                break

        lon, lat = self._transform_coordinates(locs)
        df = pd.DataFrame(
            {
                constants.GAUGE_ID: locs[loc_code].astype(str).str.strip(),
                constants.STATION_NAME: locs[name_col].astype(str).str.strip() if name_col else np.nan,
                constants.RIVER: river_series,
                constants.LATITUDE: lat,
                constants.LONGITUDE: lon,
                constants.ALTITUDE: np.nan,
                constants.AREA: np.nan,
                constants.COUNTRY: self.COUNTRY,
                constants.SOURCE: self.SOURCE,
            }
        )
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return df.set_index(constants.GAUGE_ID)

    @staticmethod
    def _monthly_windows(start_date: str, end_date: str) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        if pd.isna(start) or pd.isna(end) or start > end:
            return []

        windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        current = start
        while current <= end:
            next_month = (current + pd.offsets.MonthBegin(1)).normalize()
            window_end = min(end, next_month - pd.Timedelta(days=1))
            windows.append((current, window_end))
            current = window_end + pd.Timedelta(days=1)
        return windows

    def _get_location_payload(self, gauge_id: str) -> dict[str, Any]:
        catalog = self._get_catalog()
        locs = self._as_frame(catalog.get("LocatieLijst"))
        if locs.empty:
            return {"Code": gauge_id}

        loc_code = self._pick_column(locs, ("Code", "LocatieCode", "StationCode"))
        if loc_code is None:
            return {"Code": gauge_id}

        matches = locs[locs[loc_code].astype(str).str.strip() == str(gauge_id)]
        if matches.empty:
            return {"Code": gauge_id}

        row = matches.iloc[0]
        location = {"Code": str(gauge_id)}
        x = pd.to_numeric(pd.Series([row.get("X")]), errors="coerce").iloc[0]
        y = pd.to_numeric(pd.Series([row.get("Y")]), errors="coerce").iloc[0]
        if pd.notna(x) and pd.notna(y):
            location["X"] = float(x)
            location["Y"] = float(y)
        return location

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        config = self.VARIABLE_MAP[variable]
        location = self._get_location_payload(str(gauge_id))
        payloads: list[dict[str, Any]] = []

        for window_start, window_end in self._monthly_windows(start_date, end_date):
            for rank, variant in enumerate(config["variants"]):
                aquo_metadata: dict[str, Any] = {
                    "Compartiment": {"Code": variant["compartiment_code"]},
                    "Grootheid": {"Code": variant["grootheid"]},
                }
                if variant.get("eenheid") is not None:
                    aquo_metadata["Eenheid"] = {"Code": variant["eenheid"]}
                if variant.get("hoedanigheid") is not None:
                    aquo_metadata["Hoedanigheid"] = {"Code": variant["hoedanigheid"]}

                body = {
                    "Locatie": location,
                    "AquoPlusWaarnemingMetadata": {"AquoMetadata": aquo_metadata},
                    "Periode": {
                        "Begindatumtijd": window_start.strftime("%Y-%m-%dT00:00:00.000+00:00"),
                        "Einddatumtijd": window_end.strftime("%Y-%m-%dT23:59:59.999+00:00"),
                    },
                }
                payload = self._request_json(self.DATA_ENDPOINTS, body)
                if payload:
                    payloads.append(
                        {
                            "payload": payload,
                            "rank": rank,
                            "unit_factor": variant["unit_factor"],
                        }
                    )

        return payloads

    @staticmethod
    def _flatten_measurement_lists(payload: dict[str, Any]) -> list[dict[str, Any]]:
        observation_list = payload.get("WaarnemingenLijst")
        if observation_list is None:
            return []

        containers: list[Any]
        if isinstance(observation_list, list):
            containers = observation_list
        else:
            containers = [observation_list]

        measurements: list[dict[str, Any]] = []
        for container in containers:
            if isinstance(container, dict):
                value = container.get("MetingenLijst")
                if isinstance(value, list):
                    measurements.extend([item for item in value if isinstance(item, dict)])
                elif isinstance(value, dict):
                    measurements.append(value)
        return measurements

    @classmethod
    def _parse_data(
        cls,
        gauge_id: str,
        raw_data: list[dict[str, Any]],
        variable: str,
    ) -> pd.DataFrame:
        """Parses Rijkswaterstaat payloads into the standard RivRetrieve layout."""
        if not raw_data:
            return cls._empty_data_frame(variable)

        rows: list[dict[str, Any]] = []
        config = cls.VARIABLE_MAP[variable]
        for payload_info in raw_data:
            payload = payload_info["payload"]
            for measurement in cls._flatten_measurement_lists(payload):
                timestamp = (
                    measurement.get("Tijdstip") or measurement.get("Tijdstempel") or measurement.get("Datumtijd")
                )
                numeric_value = cls._extract_nested_value(measurement.get("Meetwaarde"), "Waarde_Numeriek")
                rows.append(
                    {
                        constants.TIME_INDEX: pd.to_datetime(timestamp, utc=True, errors="coerce"),
                        variable: cls._clean_provider_value(numeric_value) * payload_info["unit_factor"],
                        "_rank": payload_info["rank"],
                    }
                )

        if not rows:
            return cls._empty_data_frame(variable)

        df = pd.DataFrame(rows).dropna(subset=[constants.TIME_INDEX, variable])
        if df.empty:
            return cls._empty_data_frame(variable)

        df[constants.TIME_INDEX] = df[constants.TIME_INDEX].dt.tz_convert(cls.LOCAL_TIMEZONE).dt.tz_localize(None)
        df = df.sort_values([constants.TIME_INDEX, "_rank"])
        df = df.drop_duplicates(subset=[constants.TIME_INDEX], keep="first")

        if config["aggregate_daily"]:
            df[constants.TIME_INDEX] = df[constants.TIME_INDEX].dt.floor("D")
            df = df.groupby(constants.TIME_INDEX, as_index=False)[variable].mean()
        else:
            df = df[[constants.TIME_INDEX, variable]]

        df = df.drop_duplicates(subset=[constants.TIME_INDEX]).sort_values(constants.TIME_INDEX)
        return df.set_index(constants.TIME_INDEX)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses Dutch time series data for a specific gauge and variable.

        This method retrieves the requested data from Rijkswaterstaat's observation API,
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
        if "instantaneous" in variable:
            end_dt = end_dt + pd.Timedelta(days=1)
            return df[(df.index >= start_dt) & (df.index < end_dt)]

        return df[(df.index >= start_dt) & (df.index <= end_dt)]
