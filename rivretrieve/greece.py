"""Fetcher for Greece river gauge data from the OpenHI / Enhydris public API."""

from __future__ import annotations

import logging
import re
from io import StringIO
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)


class GreeceFetcher(base.RiverDataFetcher):
    """Fetches Greek river gauge data from the public OpenHI / Enhydris API.

    Data source:
        - website: https://openhi.net/en/
        - API root: https://system.openhi.net/api/
        - station search endpoint: https://system.openhi.net/api/stations/

    Supported variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.STAGE_INSTANT`` (m)

    Data description and API:
        - OpenHI public API browser: https://system.openhi.net/api/
        - Enhydris webservice docs: https://enhydris.readthedocs.io/en/latest/dev/webservice-api.html
        - station time series groups: ``/api/stations/{station_id}/timeseriesgroups/``
        - group time series listing: ``/api/stations/{station_id}/timeseriesgroups/{group_id}/timeseries/``
        - historical data download:
          ``/api/stations/{station_id}/timeseriesgroups/{group_id}/timeseries/{timeseries_id}/data/?fmt=csv``

    Terms of use:
        - OpenHI data licence: https://openhi.net/licence/

    Notes:
        - The upstream platform is OpenHI / Enhydris, an integrated national portal
          that aggregates stations from multiple owners. This fetcher keeps
          ``source`` fixed to OpenHI / Enhydris and exposes owner metadata
          separately when available.
        - Station discovery uses the union of the public search queries
          ``ts_only:+variable:stage``, ``ts_only:+variable:level``, and
          ``ts_only:+variable:discharge``. This avoids over-matching broader search
          terms such as ``water`` while still capturing stations published as
          either ``Stage`` or ``Water Level``.
        - The upstream hydrometric metadata are not fully uniform. This fetcher
          normalizes provider variables ``Stage`` and ``Water Level`` into
          RivRetrieve ``stage`` variables, while ``Discharge`` maps to RivRetrieve
          discharge variables.
        - Stage values are normalized to meters. Provider series advertised in
          centimeters are converted to meters before return or daily aggregation.
        - Provider sentinel missing values such as ``-999``, ``-9999``, and
          ``-6999`` are converted to missing values before aggregation.
        - Series selection is deterministic but provider-aware. For each station,
          the fetcher ranks compatible time series groups, prefers checked/public
          series where available, and uses lower-priority groups only to backfill
          timestamps or days that are missing from higher-priority groups.
        - Daily products are returned as daily means. When a provider daily-mean
          series is not available, this fetcher aggregates the selected subdaily
          series to daily means.
        - OpenHI metadata and bundled provider-derived fixtures remain subject to
          the OpenHI data licence (CC BY-SA 4.0). The repository MIT licence
          applies only to RivRetrieve code.
    """

    BASE_URL = "https://system.openhi.net/api"
    SOURCE = "OpenHI / Enhydris public API"
    COUNTRY = "Greece"
    STATION_SEARCH_QUERIES = (
        "ts_only: variable:stage",
        "ts_only: variable:level",
        "ts_only: variable:discharge",
    )
    SENTINEL_VALUES = {-999.0, -9999.0, -6999.0}
    TYPE_PRIORITY = {
        "Checked": 0,
        "Initial": 1,
        "Aggregated": 2,
    }
    VARIABLE_MAP = {
        constants.DISCHARGE_DAILY_MEAN: {
            "kind": constants.DISCHARGE,
            "aggregate_daily": True,
            "provider_variable_ids": {2},
        },
        constants.DISCHARGE_INSTANT: {
            "kind": constants.DISCHARGE,
            "aggregate_daily": False,
            "provider_variable_ids": {2},
        },
        constants.STAGE_DAILY_MEAN: {
            "kind": constants.STAGE,
            "aggregate_daily": True,
            "provider_variable_ids": {14, 5710},
        },
        constants.STAGE_INSTANT: {
            "kind": constants.STAGE,
            "aggregate_daily": False,
            "provider_variable_ids": {14, 5710},
        },
    }

    def __init__(self):
        self._session = utils.requests_retry_session(
            retries=6,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
        )
        self._station_query_cache: dict[str, list[dict[str, Any]]] = {}
        self._timeseries_group_cache: dict[str, list[dict[str, Any]]] = {}
        self._timeseries_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._variable_cache: dict[int, str] = {}
        self._unit_cache: dict[int, str] = {}
        self._organization_cache: dict[int, Optional[str]] = {}

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves cached Greece gauge metadata."""
        return utils.load_cached_metadata_csv("greece")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return tuple(GreeceFetcher.VARIABLE_MAP.keys())

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
            "station_code",
            "display_timezone",
            "owner_id",
            "owner_name",
            "provider_start_date",
            "provider_end_date",
        ]
        return pd.DataFrame(columns=columns).set_index(constants.GAUGE_ID)

    @classmethod
    def _absolute_url(cls, path_or_url: str) -> str:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            return path_or_url
        return f"{cls.BASE_URL}/{path_or_url.lstrip('/')}"

    def _request_json(self, path_or_url: str, params: Optional[dict[str, Any]] = None) -> Any:
        url = self._absolute_url(path_or_url)
        response = self._session.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    def _request_text(self, path_or_url: str, params: Optional[dict[str, Any]] = None) -> str:
        url = self._absolute_url(path_or_url)
        response = self._session.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.text

    def _fetch_paginated(self, path_or_url: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        url = self._absolute_url(path_or_url)
        current_params = params.copy() if params else None
        results: list[dict[str, Any]] = []

        while url:
            payload = self._request_json(url, params=current_params)
            if isinstance(payload, dict):
                page_results = payload.get("results", [])
                if isinstance(page_results, list):
                    results.extend([record for record in page_results if isinstance(record, dict)])
                url = payload.get("next")
                current_params = None
                continue
            break

        return results

    @staticmethod
    def _parse_geom(geom: Any) -> tuple[float, float]:
        if geom is None:
            return np.nan, np.nan

        match = re.search(r"POINT\s*\(([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)\)", str(geom))
        if not match:
            return np.nan, np.nan

        longitude = pd.to_numeric(match.group(1), errors="coerce")
        latitude = pd.to_numeric(match.group(2), errors="coerce")
        return float(latitude), float(longitude)

    @staticmethod
    def _normalized_text(value: Any) -> str:
        return str(value or "").strip().casefold()

    @staticmethod
    def _timestamp_sort_value(value: Any) -> int:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return 0
        return -int(parsed.value)

    def _search_stations(self, query: str) -> list[dict[str, Any]]:
        if query not in self._station_query_cache:
            self._station_query_cache[query] = self._fetch_paginated("/stations/", params={"q": query})
        return [record.copy() for record in self._station_query_cache[query]]

    def _get_timeseries_groups(self, station_id: str) -> list[dict[str, Any]]:
        station_id = str(station_id).strip()
        if station_id not in self._timeseries_group_cache:
            self._timeseries_group_cache[station_id] = self._fetch_paginated(
                f"/stations/{station_id}/timeseriesgroups/"
            )
        return [record.copy() for record in self._timeseries_group_cache[station_id]]

    def _get_timeseries(self, station_id: str, group_id: str) -> list[dict[str, Any]]:
        key = (str(station_id).strip(), str(group_id).strip())
        if key not in self._timeseries_cache:
            self._timeseries_cache[key] = self._fetch_paginated(
                f"/stations/{key[0]}/timeseriesgroups/{key[1]}/timeseries/"
            )
        return [record.copy() for record in self._timeseries_cache[key]]

    def _get_variable_name(self, variable_id: Any) -> str:
        numeric_id = pd.to_numeric(variable_id, errors="coerce")
        if pd.isna(numeric_id):
            return ""

        cache_key = int(numeric_id)
        if cache_key not in self._variable_cache:
            payload = self._request_json(f"/variables/{cache_key}/")
            translations = payload.get("translations", {}) if isinstance(payload, dict) else {}
            english = translations.get("en", {}) if isinstance(translations, dict) else {}
            greek = translations.get("el", {}) if isinstance(translations, dict) else {}
            description = english.get("descr") or greek.get("descr") or ""
            self._variable_cache[cache_key] = str(description).strip()

        return self._variable_cache[cache_key]

    def _get_unit_symbol(self, unit_id: Any) -> str:
        numeric_id = pd.to_numeric(unit_id, errors="coerce")
        if pd.isna(numeric_id):
            return ""

        cache_key = int(numeric_id)
        if cache_key not in self._unit_cache:
            payload = self._request_json(f"/units/{cache_key}/")
            symbol = payload.get("symbol", "") if isinstance(payload, dict) else ""
            self._unit_cache[cache_key] = str(symbol).strip()

        return self._unit_cache[cache_key]

    def _get_owner_name(self, owner_id: Any) -> Optional[str]:
        numeric_id = pd.to_numeric(owner_id, errors="coerce")
        if pd.isna(numeric_id):
            return None

        cache_key = int(numeric_id)
        if cache_key not in self._organization_cache:
            payload = self._request_json(f"/organizations/{cache_key}/")
            if not isinstance(payload, dict):
                self._organization_cache[cache_key] = None
            else:
                name = payload.get("name") or payload.get("ordering_string") or payload.get("acronym")
                self._organization_cache[cache_key] = str(name).strip() or None

        return self._organization_cache[cache_key]

    def get_metadata(self) -> pd.DataFrame:
        """Fetches live metadata for stations with supported Greek variables."""
        stations_by_id: dict[str, dict[str, Any]] = {}

        for query in self.STATION_SEARCH_QUERIES:
            for station in self._search_stations(query):
                station_id = str(station.get("id", "")).strip()
                if station_id:
                    stations_by_id[station_id] = station

        if not stations_by_id:
            return self._empty_metadata_frame()

        rows = []
        for gauge_id, station in stations_by_id.items():
            latitude, longitude = self._parse_geom(station.get("geom"))
            owner_id = pd.to_numeric(station.get("owner"), errors="coerce")
            rows.append(
                {
                    constants.GAUGE_ID: gauge_id,
                    constants.STATION_NAME: station.get("name"),
                    constants.RIVER: np.nan,
                    constants.LATITUDE: latitude,
                    constants.LONGITUDE: longitude,
                    constants.ALTITUDE: pd.to_numeric(station.get("altitude"), errors="coerce"),
                    constants.AREA: np.nan,
                    constants.COUNTRY: self.COUNTRY,
                    constants.SOURCE: self.SOURCE,
                    "station_code": station.get("code") or None,
                    "display_timezone": station.get("display_timezone") or None,
                    "owner_id": owner_id,
                    "owner_name": self._get_owner_name(owner_id),
                    "provider_start_date": station.get("start_date") or None,
                    "provider_end_date": station.get("end_date") or None,
                }
            )

        df = pd.DataFrame(rows)
        if df.empty:
            return self._empty_metadata_frame()

        df[constants.GAUGE_ID] = df[constants.GAUGE_ID].astype(str).str.strip()
        df = df.drop_duplicates(subset=[constants.GAUGE_ID]).sort_values(constants.GAUGE_ID)
        return df.set_index(constants.GAUGE_ID)

    def _group_kind(self, group: dict[str, Any]) -> Optional[str]:
        variable_id = pd.to_numeric(group.get("variable"), errors="coerce")
        if pd.notna(variable_id):
            variable_id = int(variable_id)
            if variable_id == 2:
                return constants.DISCHARGE
            if variable_id in {14, 5710}:
                return constants.STAGE

        variable_name = self._normalized_text(self._get_variable_name(group.get("variable")))
        if "discharge" in variable_name:
            return constants.DISCHARGE
        if variable_name in {"stage", "water level"}:
            return constants.STAGE
        return None

    def _group_semantic_score(self, group_name: Any, kind: str, aggregate_daily: bool) -> int:
        text = self._normalized_text(group_name)
        base_tokens = {"discharge"} if kind == constants.DISCHARGE else {"stage", "water level", "level"}
        has_base_name = not text or any(token in text for token in base_tokens)
        has_mean = any(token in text for token in ("mean", "avg", "average", "daily"))
        has_extreme = any(token in text for token in ("max", "maximum", "min", "minimum"))
        has_old = "old" in text

        if has_extreme:
            score = 4
        elif aggregate_daily and has_mean:
            score = 0
        elif has_base_name:
            score = 0
        elif not aggregate_daily and has_mean:
            score = 2
        else:
            score = 1

        if has_old:
            score += 2
        return score

    def _ranked_groups(self, gauge_id: str, variable: str) -> list[dict[str, Any]]:
        config = self.VARIABLE_MAP[variable]
        candidates = [
            group
            for group in self._get_timeseries_groups(gauge_id)
            if self._group_kind(group) == config["kind"]
            and pd.to_numeric(group.get("variable"), errors="coerce") in config["provider_variable_ids"]
        ]
        return sorted(
            candidates,
            key=lambda group: (
                self._group_semantic_score(group.get("name"), config["kind"], config["aggregate_daily"]),
                self._timestamp_sort_value(group.get("last_modified")),
            ),
        )

    @staticmethod
    def _time_step_minutes(time_step: Any) -> Optional[int]:
        text = str(time_step or "").strip().lower()
        if not text:
            return None

        match = re.fullmatch(r"(\d+)\s*([a-z]+)", text)
        if not match:
            return None

        amount = int(match.group(1))
        unit = match.group(2)
        multipliers = {
            "m": 1,
            "min": 1,
            "mins": 1,
            "minute": 1,
            "minutes": 1,
            "h": 60,
            "hr": 60,
            "hrs": 60,
            "hour": 60,
            "hours": 60,
            "d": 1440,
            "day": 1440,
            "days": 1440,
        }
        if unit not in multipliers:
            return None
        return amount * multipliers[unit]

    @classmethod
    def _timeseries_priority(cls, timeseries: dict[str, Any], aggregate_daily: bool) -> tuple[int, int, int]:
        timeseries_type = str(timeseries.get("type") or "").strip()
        type_rank = cls.TYPE_PRIORITY.get(timeseries_type, 99)
        step_minutes = cls._time_step_minutes(timeseries.get("time_step"))
        step_rank = step_minutes if step_minutes is not None else -1
        is_daily = step_minutes is not None and step_minutes >= 1440
        name = cls._normalized_text(timeseries.get("name"))

        if aggregate_daily:
            if timeseries_type == "Aggregated" and is_daily and "mean" in name:
                class_rank = 0
            elif timeseries_type == "Aggregated" and is_daily:
                class_rank = 1
            elif timeseries_type == "Checked" and not is_daily:
                class_rank = 2
            elif timeseries_type == "Initial" and not is_daily:
                class_rank = 3
            elif timeseries_type == "Aggregated" and not is_daily:
                class_rank = 4
            else:
                class_rank = 5
        else:
            if timeseries_type == "Checked" and not is_daily:
                class_rank = 0
            elif timeseries_type == "Initial" and not is_daily:
                class_rank = 1
            elif timeseries_type == "Aggregated" and not is_daily:
                class_rank = 2
            elif timeseries_type == "Checked" and is_daily:
                class_rank = 3
            elif timeseries_type == "Initial" and is_daily:
                class_rank = 4
            else:
                class_rank = 5

        return (
            class_rank,
            type_rank,
            step_rank,
        )

    def _ranked_timeseries(self, gauge_id: str, group_id: str, aggregate_daily: bool) -> list[dict[str, Any]]:
        series = [
            record
            for record in self._get_timeseries(gauge_id, group_id)
            if bool(record.get("publicly_available", True))
        ]
        if not series:
            return []

        return sorted(
            series,
            key=lambda record: (
                self._timeseries_priority(record, aggregate_daily),
                self._timestamp_sort_value(record.get("last_modified")),
            ),
        )

    @staticmethod
    def _unit_factor(kind: str, unit_symbol: str) -> float:
        normalized_unit = str(unit_symbol or "").strip()

        if kind == constants.STAGE and normalized_unit == "cm":
            return 0.01
        if kind == constants.DISCHARGE and normalized_unit in {"l/s", "L/s"}:
            return 0.001
        return 1.0

    @classmethod
    def _parse_csv_payload(cls, payload: str, factor: float) -> pd.DataFrame:
        if not payload.strip():
            return pd.DataFrame(columns=[constants.TIME_INDEX, "value"])

        raw_df = pd.read_csv(StringIO(payload), header=None)
        if raw_df.empty or raw_df.shape[1] < 2:
            return pd.DataFrame(columns=[constants.TIME_INDEX, "value"])

        result = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(raw_df.iloc[:, 0], errors="coerce"),
                "value": pd.to_numeric(raw_df.iloc[:, 1], errors="coerce"),
            }
        ).dropna(subset=[constants.TIME_INDEX, "value"])

        if result.empty:
            return result

        result.loc[result["value"].isin(cls.SENTINEL_VALUES), "value"] = np.nan
        result = result.dropna(subset=["value"])
        if result.empty:
            return result

        result["value"] = result["value"] * factor
        return result

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        config = self.VARIABLE_MAP[variable]
        ranked_groups = self._ranked_groups(gauge_id, variable)
        payloads: list[dict[str, Any]] = []

        for group_rank, group in enumerate(ranked_groups):
            group_id = str(group.get("id", "")).strip()
            if not group_id:
                continue

            ranked_series = self._ranked_timeseries(gauge_id, group_id, config["aggregate_daily"])
            if not ranked_series:
                continue

            for series_rank, timeseries in enumerate(ranked_series):
                timeseries_id = str(timeseries.get("id", "")).strip()
                if not timeseries_id:
                    continue

                try:
                    payload = self._request_text(
                        f"/stations/{gauge_id}/timeseriesgroups/{group_id}/timeseries/{timeseries_id}/data/",
                        params={
                            "fmt": "csv",
                            "start_date": start_date,
                            "end_date": end_date,
                        },
                    )
                except requests.exceptions.RequestException as exc:
                    logger.warning(
                        f"Failed to fetch Greece data for station {gauge_id}, group {group_id}, "
                        f"timeseries {timeseries_id}: {exc}"
                    )
                    continue

                if not payload.strip():
                    continue

                payloads.append(
                    {
                        "payload": payload,
                        "group_rank": group_rank,
                        "series_rank": series_rank,
                        "unit_symbol": self._get_unit_symbol(group.get("unit_of_measurement")),
                    }
                )

        return payloads

    def _parse_data(self, gauge_id: str, raw_data: list[dict[str, Any]], variable: str) -> pd.DataFrame:
        config = self.VARIABLE_MAP[variable]
        if not raw_data:
            return self._empty_data_frame(variable)

        frames = []
        for payload in raw_data:
            parsed = self._parse_csv_payload(
                payload.get("payload", ""),
                factor=self._unit_factor(config["kind"], payload.get("unit_symbol", "")),
            )
            if parsed.empty:
                continue

            if config["aggregate_daily"]:
                parsed[constants.TIME_INDEX] = parsed[constants.TIME_INDEX].dt.floor("D")
                parsed = parsed.groupby(constants.TIME_INDEX, as_index=False)["value"].mean()

            parsed["group_rank"] = payload.get("group_rank", 999)
            parsed["series_rank"] = payload.get("series_rank", 999)
            frames.append(parsed)

        if not frames:
            return self._empty_data_frame(variable)

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.sort_values([constants.TIME_INDEX, "group_rank", "series_rank"])
        combined = combined.drop_duplicates(subset=[constants.TIME_INDEX], keep="first")
        combined = combined[[constants.TIME_INDEX, "value"]].rename(columns={"value": variable})
        combined = combined.dropna(subset=[variable]).sort_values(constants.TIME_INDEX)
        return combined.set_index(constants.TIME_INDEX)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        gauge_id = str(gauge_id).strip()

        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data, variable)
        except Exception as exc:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {exc}")
            return self._empty_data_frame(variable)

        if df.empty:
            return df

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        if "instantaneous" in variable or "hourly" in variable:
            end_dt = end_dt + pd.Timedelta(days=1)
            return df[(df.index >= start_dt) & (df.index < end_dt)]

        return df[(df.index >= start_dt) & (df.index <= end_dt)]
