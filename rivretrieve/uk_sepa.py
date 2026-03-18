"""Fetcher for UK river gauge data."""

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from . import base, constants, utils

logger = logging.getLogger(__name__)

# from datetime import date, datetime, timedelta, timezone
# from typing import List

class UKSEPAFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from the Scottish Environment Protection Agency (SEPA). 
    
    Data Source: SEPA API ()

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_INSTANT`` (m)
    """

    BASE_URL = 'https://timeseries.sepa.org.uk/KiWIS/KiWIS'

    TOKEN_URL = 'https://timeseries.sepa.org.uk/KiWebPortal/rest/auth/oidcServer/token'

    GARBAGE_STATION_PATTERNS = [
        r"^#", r"^--", r"testing", r"^Template\s",
        r"\sTEST$", r"\sTEMP$", r"\stest\s"
    ]

    def _download_data(self, gauge_id: str, variable: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        pass 

    def _parse_data(self, raw_data: List[Dict[str, Any]], variable: str) -> pd.DataFrame:
        pass

    def get_available_variables(self): 
        return (
            constants.DISCHARGE_DAILY_MEAN, 
            constants.STAGE_DAILY_MEAN, 
            constants.DISCHARGE_INSTANT,
            constants.STAGE_INSTANT
        )
    
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available UK Environment Agency gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        pass
        # return utils.load_cached_metadata_csv("uk_ea")

    def _get_group_metadata(
        self, 
        group_id,
        return_fields: str | List[str] | None = None, 
        auth_header: dict | None = None, 
        timeout: int = 15
    ):
        if return_fields is None:
            return_fields = [
                "station_name", "station_no", "station_id",
                "station_latitude", "station_longitude"
            ]
        elif isinstance(return_fields, str):
            return_fields = [f.strip() for f in return_fields.split(",")]
        elif not isinstance(return_fields, (list, tuple)):
            raise TypeError(
                "return_fields must be a comma-separated string or a list/tuple of strings"
            )
        params = {
            "service": "kisters",
            "datasource": 0,
            "type": "queryServices",
            "request": "getStationList",
            "format": "json",
            "kvp": "true",
            "returnfields": ",".join(return_fields),
            "stationgroup_id": group_id
        }

        # Perform request
        headers = auth_header or {}
        response = requests.get(self.BASE_URL, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        json_content = response.json()

        if not json_content or len(json_content) < 2:
            return pd.DataFrame()

        # First row = column names
        column_names = list(map(str, json_content[0]))
        rows = [dict(zip(column_names, row)) for row in json_content[1:]]
        df = pd.DataFrame(rows)

        # Convert lat/lon if present
        for col in ["station_latitude", "station_longitude"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove bogus or test stations
        if "station_name" in df.columns:
            pattern = re.compile("|".join(self.GARBAGE_STATION_PATTERNS), flags=re.IGNORECASE)
            df = df[~df["station_name"].str.contains(pattern, na=False)]

        return df

    def get_metadata(
        self, 
        return_fields: str | List[str] | None = None, 
        auth_header: dict | None = None, 
        timeout: int = 15
    ) -> pd.DataFrame:
        """Fetches site metadata for all stations from the EA API.

        Returns:
            A pandas DataFrame indexed by gauge_id, containing site metadata.
        """
        # These groups correspond to StationsWithFlow and StationsWithLevel
        group_ids = ['270322', '615437']
        stn_list = []
        for group_id in group_ids: 
            df = self._get_group_metadata(group_id, return_fields, auth_header, timeout)
            stn_list.append(df)
        stns = pd.concat(stn_list)
        stns = stns.drop_duplicates().reset_index(drop=True)
        return stns

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
        

    def _sepa_auth_headers(self, api_key: str | None = None) -> dict:
        """Get access token to access the SEPA Timeseries API as a 
        registered user, which allows access to more data per day. See 
        https://timeseriesdoc.sepa.org.uk/api-documentation/before-you-start/what-controls-there-are-on-access/
        for more details. 
        """
        if api_key is None: 
            return {}
        auth_headers = {'Authorization' : 'Basic ' + api_key}
        response = requests.post(self.TOKEN_URL, headers = auth_headers, data='grant_type=client_credentials')
        response.raise_for_status()
        access_token = response.json()['access_token']
        return {'Authorization': 'Bearer ' + access_token}

    def _sepa_group_list(self, auth_header: dict | None = None, timeout: int = 15) -> pd.DataFrame:
        """Retrieve the list of available SEPA station groups from the KiWIS API."""
        params = {
            "service": "kisters",
            "datasource": 0,
            "type": "queryServices",
            "request": "getGroupList",
            "format": "json",
            "kvp": "true",
        }
        headers = auth_header or {}
        response = requests.get(self.BASE_URL, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        json_content = response.json()
        column_names = list(map(str, json_content[0]))
        rows = [dict(zip(column_names, row)) for row in json_content[1:]]
        df = pd.DataFrame(rows)
        return df

    def _sepa_station_list(
        self, 
        group_id: int | str | None = None,
        return_fields: str | List[str] | None = None,
        auth_header: dict | None = None,
        timeout: int = 15
    ) -> pd.DataFrame:
        """Retrieve a list of SEPA hydrometric stations via the KiWIS API."""
        if return_fields is None:
            return_fields = [
                "station_name", "station_no", "station_id",
                "station_latitude", "station_longitude"
            ]
        elif isinstance(return_fields, str):
            return_fields = [f.strip() for f in return_fields.split(",")]
        elif not isinstance(return_fields, (list, tuple)):
            raise TypeError(
                "return_fields must be a comma-separated string or a list/tuple of strings"
            )
        params = {
            "service": "kisters",
            "datasource": 0,
            "type": "queryServices",
            "request": "getStationList",
            "format": "json",
            "kvp": "true",
            "returnfields": ",".join(return_fields),
        }

        # Group ID
        if group_id is not None:
            params["stationgroup_id"] = group_id

        # Perform request
        headers = auth_header or {}
        response = requests.get(self.BASE_URL, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        json_content = response.json()

        if not json_content or len(json_content) < 2:
            return pd.DataFrame()

        # First row = column names
        column_names = list(map(str, json_content[0]))
        rows = [dict(zip(column_names, row)) for row in json_content[1:]]
        df = pd.DataFrame(rows)

        # Convert lat/lon if present
        for col in ["station_latitude", "station_longitude"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove bogus or test stations
        if "station_name" in df.columns:
            pattern = re.compile("|".join(self.GARBAGE_STATION_PATTERNS), flags=re.IGNORECASE)
            df = df[~df["station_name"].str.contains(pattern, na=False)]

        return df

    def _sepa_timeseries_list(
        self,
        station_id: str | None = None,
        stationparameter_name: str | None = None,
        ts_name: str | None = None,
        coverage: bool = True,
        group_id: str | None = None,
        return_fields: str | List[str] | None = None,
        auth_header: dict | None = None,
        timeout=15,
    ):
        """Query SEPA for available timeseries metadata."""
        # Check for missing input
        if not any([station_id, ts_name, group_id]):
            raise ValueError("No station_id, ts_name, or group_id provided.")

        # Handle user-provided return fields
        if return_fields is None:
            return_fields = [
                "station_name",
                "station_id",
                "stationparameter_name",
                "ts_id",
                "ts_name",
            ]
        elif not isinstance(return_fields, (str, list)):
            raise TypeError(
                "User-supplied `return_fields` must be a comma-separated string or list of strings."
            )

        # Convert list to comma-separated string
        if isinstance(return_fields, list):
            return_fields = ",".join(return_fields)

        # Remove any explicit 'coverage' mentions from return_fields
        return_fields = (
            return_fields.replace(",coverage", "")
            .replace("coverage,", "")
            .replace("coverage", "")
        )

        # Base query
        api_query = {
            "service": "kisters",
            "datasource": 0,
            "type": "queryServices",
            "request": "getTimeseriesList",
            "format": "json",
            "kvp": "true",
            "returnfields": return_fields,
        }

        # Handle inputs
        if station_id is not None:
            if isinstance(station_id, list):
                station_id = ",".join(station_id)
            api_query["station_id"] = station_id

        if stationparameter_name is not None: 
            api_query["stationparameter_name"] = stationparameter_name

        if ts_name is not None:
            api_query["ts_name"] = ts_name

        if group_id is not None:
            if station_id is not None or ts_name is not None:
                raise ValueError("`group_id` cannot be used with `station_id` or `ts_name`.")
            api_query["stationgroup_id"] = group_id

        if coverage:
            api_query["returnfields"] = f"{api_query['returnfields']},coverage"

        # Perform request
        headers = auth_header or {}
        response = requests.get(self.BASE_URL, params=api_query, headers=headers, timeout=timeout)
        response.raise_for_status()
        json_content = response.json()

        if not isinstance(json_content, list) or len(json_content) < 2:
            return pd.DataFrame()

        # Extract field names and rows
        col_names = [str(c) for c in json_content[0]]
        rows = [dict(zip(col_names, r)) for r in json_content[1:]]
        df = pd.DataFrame(rows)

        # Convert numeric lat/lon
        for col in ["station_latitude", "station_longitude"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Convert coverage columns to datetime
        for col in ["from", "to"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        return df

    def _sepa_timeseries_values(
        self,
        ts_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
        return_fields: str | List[str] | None = None,
        metadata: bool = False,
        md_return_fields: str | List[str] | None = None,
        ca_sta: bool = False,
        ca_sta_return_fields: str | List[str] | None = None,
        auth_header: dict | None = None,
        timeout: int = 60,
    ):
        """Retrieve time series values from the SEPA KiWIS API."""
        # Default to past 24 hours
        if start_date is None or end_date is None:
            print(
                "No start or end date provided, attempting to retrieve data for past 24 hours."
            )
            end_date = datetime.now(tz = timezone.utc).date()
            start_date = end_date - timedelta(days=1)
        else:
            start_date = pd.to_datetime(start_date).date()
            end_date = pd.to_datetime(end_date).date()

        start_date_str = datetime.strftime(start_date, "%Y-%m-%d")
        end_date_str = datetime.strftime(end_date, "%Y-%m-%d")

        if ts_id is None:
            raise ValueError("Please enter a valid ts_id.")
        if isinstance(ts_id, list):
            ts_id_str = ",".join(map(str, ts_id))
        else:
            ts_id_str = str(ts_id)

        if return_fields is None:
            return_fields = ["Timestamp", "Value"]
        elif isinstance(return_fields, str):
            return_fields = ["Timestamp", "Value"] + [return_fields]
        elif isinstance(return_fields, list):
            return_fields = ["Timestamp", "Value"] + return_fields
        else:
            raise TypeError("return_fields must be a string or list of strings.")
        return_fields_str = ",".join(return_fields)

        # Handle metadata fields
        if md_return_fields is None:
            if metadata:
                md_return_fields = [
                    "ts_unitname",
                    "ts_unitsymbol",
                    "ts_name",
                    "ts_id",
                    "stationparameter_name",
                    "station_name",
                    "station_id",
                ]
            else:
                md_return_fields = []
        md_return_fields = [f for f in md_return_fields if f != "ca_sta"]

        # Handle custom attributes
        if ca_sta:
            metadata = True
            md_return_fields.append("ca_sta")
        md_return_fields_str = ",".join(md_return_fields)

        if ca_sta_return_fields is None:
            ca_sta_return_fields = ["CATCHMENT_SIZE", "GAUGE_DATUM"]
        ca_sta_return_fields_str = ",".join(ca_sta_return_fields)

        # Build query
        api_query = {
            "service": "kisters",
            "datasource": 0,
            "type": "queryServices",
            "request": "getTimeseriesValues",
            "format": "json",
            "kvp": "true",
            "ts_id": ts_id_str,
            "from": start_date_str,
            "to": end_date_str,
            "metadata": str(metadata).lower(),
            "md_returnfields": md_return_fields_str,
            "ca_returnfields": ca_sta_return_fields_str,
            "returnfields": return_fields_str,
        }

        # Perform request
        headers = auth_header or {}
        response = requests.get(self.BASE_URL, params=api_query, headers=headers, timeout=timeout)
        response.raise_for_status()
        json_content = response.json()[0]

        # Handle API errors
        if len(json_content.keys()) == 3 and "message" in json_content:
            raise RuntimeError(json_content["message"])

        if "rows" in json_content:
            num_rows = sum(map(int, json_content["rows"]))
            if num_rows == 0:
                raise RuntimeError("No data available for selected ts_id(s).")

        # Parse data
        ts_cols = json_content["columns"]
        if isinstance(ts_cols, list): # Actually not sure whether this would ever be the case
            ts_cols = ts_cols[0]
        ts_cols = ts_cols.split(',')
        data = [dict(zip(ts_cols, row)) for row in json_content["data"]]
        df = pd.DataFrame(data)

        # Convert to proper types
        if "Timestamp" in df.columns:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True)
        if "Value" in df.columns:
            df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

        # Add metadata
        if metadata:
            for field in md_return_fields:
                if field != "ca_sta" and field in json_content:
                    df[field] = json_content[field]

        if ca_sta:
            for field in ca_sta_return_fields:
                if field in json_content:
                    df[field] = json_content[field]

        return df

# SEPA_BASE_URL = 'https://timeseries.sepa.org.uk/KiWIS/KiWIS'
# SEPA_TOKEN_URL = 'https://timeseries.sepa.org.uk/KiWebPortal/rest/auth/oidcServer/token'
# SEPA_GARBAGE_STATION_PATTERNS = [
#     r"^#", r"^--", r"testing", r"^Template\s",
#     r"\sTEST$", r"\sTEMP$", r"\stest\s"
# ]

# def sepa_auth_headers(api_key: str | None = None) -> dict:
#     """Get access token to access the SEPA Timeseries API as a 
#     registered user, which allows access to more data per day. See 
#     https://timeseriesdoc.sepa.org.uk/api-documentation/before-you-start/what-controls-there-are-on-access/
#     for more details. 
#     """
#     if api_key is None: 
#         return {}
#     auth_headers = {'Authorization' : 'Basic ' + api_key}
#     response = requests.post(SEPA_TOKEN_URL, headers = auth_headers, data='grant_type=client_credentials')
#     response.raise_for_status()
#     access_token = response.json()['access_token']
#     return {'Authorization': 'Bearer ' + access_token}


# def sepa_group_list(auth_header: dict | None = None, timeout: int = 15) -> pd.DataFrame:
#     """Retrieve the list of available SEPA station groups from the KiWIS API."""
#     params = {
#         "service": "kisters",
#         "datasource": 0,
#         "type": "queryServices",
#         "request": "getGroupList",
#         "format": "json",
#         "kvp": "true",
#     }
#     headers = auth_header or {}
#     response = requests.get(SEPA_BASE_URL, params=params, headers=headers, timeout=timeout)
#     response.raise_for_status()
#     json_content = response.json()
#     column_names = list(map(str, json_content[0]))
#     rows = [dict(zip(column_names, row)) for row in json_content[1:]]
#     df = pd.DataFrame(rows)
#     return df


# def sepa_station_list(
#     group_id: int | str | None = None,
#     return_fields: str | List[str] | None = None,
#     auth_header: dict | None = None,
#     timeout: int = 15
# ) -> pd.DataFrame:
#     """Retrieve a list of SEPA hydrometric stations via the KiWIS API."""
#     if return_fields is None:
#         return_fields = [
#             "station_name", "station_no", "station_id",
#             "station_latitude", "station_longitude"
#         ]
#     elif isinstance(return_fields, str):
#         return_fields = [f.strip() for f in return_fields.split(",")]
#     elif not isinstance(return_fields, (list, tuple)):
#         raise TypeError(
#             "return_fields must be a comma-separated string or a list/tuple of strings"
#         )
#     params = {
#         "service": "kisters",
#         "datasource": 0,
#         "type": "queryServices",
#         "request": "getStationList",
#         "format": "json",
#         "kvp": "true",
#         "returnfields": ",".join(return_fields),
#     }

#     # Group ID
#     if group_id is not None:
#         params["stationgroup_id"] = group_id

#     # Perform request
#     headers = auth_header or {}
#     response = requests.get(SEPA_BASE_URL, params=params, headers=headers, timeout=timeout)
#     response.raise_for_status()
#     json_content = response.json()

#     if not json_content or len(json_content) < 2:
#         return pd.DataFrame()

#     # First row = column names
#     column_names = list(map(str, json_content[0]))
#     rows = [dict(zip(column_names, row)) for row in json_content[1:]]
#     df = pd.DataFrame(rows)

#     # Convert lat/lon if present
#     for col in ["station_latitude", "station_longitude"]:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")

#     # Remove bogus or test stations
#     if "station_name" in df.columns:
#         pattern = re.compile("|".join(SEPA_GARBAGE_STATION_PATTERNS), flags=re.IGNORECASE)
#         df = df[~df["station_name"].str.contains(pattern, na=False)]

#     return df


# def sepa_timeseries_list(
#     station_id: str | None = None,
#     ts_name: str | None = None,
#     coverage: bool = True,
#     group_id: str | None = None,
#     return_fields: str | List[str] | None = None,
#     auth_header: dict | None = None,
#     timeout=15,
# ):
#     """Query SEPA for available timeseries metadata."""
#     # Check for missing input
#     if not any([station_id, ts_name, group_id]):
#         raise ValueError("No station_id, ts_name, or group_id provided.")

#     # Handle user-provided return fields
#     if return_fields is None:
#         return_fields = [
#             "station_name",
#             "station_id",
#             "stationparameter_name",
#             "ts_id",
#             "ts_name",
#         ]
#     elif not isinstance(return_fields, (str, list)):
#         raise TypeError(
#             "User-supplied `return_fields` must be a comma-separated string or list of strings."
#         )

#     # Convert list to comma-separated string
#     if isinstance(return_fields, list):
#         return_fields = ",".join(return_fields)

#     # Remove any explicit 'coverage' mentions from return_fields
#     return_fields = (
#         return_fields.replace(",coverage", "")
#         .replace("coverage,", "")
#         .replace("coverage", "")
#     )

#     # Base query
#     api_query = {
#         "service": "kisters",
#         "datasource": 0,
#         "type": "queryServices",
#         "request": "getTimeseriesList",
#         "format": "json",
#         "kvp": "true",
#         "returnfields": return_fields,
#     }

#     # Handle inputs
#     if station_id is not None:
#         if isinstance(station_id, list):
#             station_id = ",".join(station_id)
#         api_query["station_id"] = station_id

#     if ts_name is not None:
#         api_query["ts_name"] = ts_name

#     if group_id is not None:
#         if station_id is not None or ts_name is not None:
#             raise ValueError("`group_id` cannot be used with `station_id` or `ts_name`.")
#         api_query["stationgroup_id"] = group_id

#     if coverage:
#         api_query["returnfields"] = f"{api_query['returnfields']},coverage"

#     # Perform request
#     headers = auth_header or {}
#     response = requests.get(SEPA_BASE_URL, params=api_query, headers=headers, timeout=timeout)
#     response.raise_for_status()
#     json_content = response.json()

#     if not isinstance(json_content, list) or len(json_content) < 2:
#         return pd.DataFrame()

#     # Extract field names and rows
#     col_names = [str(c) for c in json_content[0]]
#     rows = [dict(zip(col_names, r)) for r in json_content[1:]]
#     df = pd.DataFrame(rows)

#     # Convert numeric lat/lon
#     for col in ["station_latitude", "station_longitude"]:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")

#     # Convert coverage columns to datetime
#     for col in ["from", "to"]:
#         if col in df.columns:
#             df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

#     return df


# def sepa_timeseries_values(
#     ts_id: str,
#     start_date: str | date | None = None,
#     end_date: str | date | None = None,
#     return_fields: str | List[str] | None = None,
#     metadata: bool = False,
#     md_return_fields: str | List[str] | None = None,
#     ca_sta: bool = False,
#     ca_sta_return_fields: str | List[str] | None = None,
#     auth_header: dict | None = None,
#     timeout: int = 60,
# ):
#     """Retrieve time series values from the SEPA KiWIS API."""
#     # Default to past 24 hours
#     if start_date is None or end_date is None:
#         print(
#             "No start or end date provided, attempting to retrieve data for past 24 hours."
#         )
#         end_date = datetime.now(tz = timezone.utc).date()
#         start_date = end_date - timedelta(days=1)
#     else:
#         start_date = pd.to_datetime(start_date).date()
#         end_date = pd.to_datetime(end_date).date()

#     start_date_str = datetime.strftime(start_date, "%Y-%m-%d")
#     end_date_str = datetime.strftime(end_date, "%Y-%m-%d")

#     if ts_id is None:
#         raise ValueError("Please enter a valid ts_id.")
#     if isinstance(ts_id, list):
#         ts_id_str = ",".join(map(str, ts_id))
#     else:
#         ts_id_str = str(ts_id)

#     if return_fields is None:
#         return_fields = ["Timestamp", "Value"]
#     elif isinstance(return_fields, str):
#         return_fields = ["Timestamp", "Value"] + [return_fields]
#     elif isinstance(return_fields, list):
#         return_fields = ["Timestamp", "Value"] + return_fields
#     else:
#         raise TypeError("return_fields must be a string or list of strings.")
#     return_fields_str = ",".join(return_fields)

#     # Handle metadata fields
#     if md_return_fields is None:
#         if metadata:
#             md_return_fields = [
#                 "ts_unitname",
#                 "ts_unitsymbol",
#                 "ts_name",
#                 "ts_id",
#                 "stationparameter_name",
#                 "station_name",
#                 "station_id",
#             ]
#         else:
#             md_return_fields = []
#     md_return_fields = [f for f in md_return_fields if f != "ca_sta"]

#     # Handle custom attributes
#     if ca_sta:
#         metadata = True
#         md_return_fields.append("ca_sta")
#     md_return_fields_str = ",".join(md_return_fields)

#     if ca_sta_return_fields is None:
#         ca_sta_return_fields = ["CATCHMENT_SIZE", "GAUGE_DATUM"]
#     ca_sta_return_fields_str = ",".join(ca_sta_return_fields)

#     # Build query
#     api_query = {
#         "service": "kisters",
#         "datasource": 0,
#         "type": "queryServices",
#         "request": "getTimeseriesValues",
#         "format": "json",
#         "kvp": "true",
#         "ts_id": ts_id_str,
#         "from": start_date_str,
#         "to": end_date_str,
#         "metadata": str(metadata).lower(),
#         "md_returnfields": md_return_fields_str,
#         "ca_returnfields": ca_sta_return_fields_str,
#         "returnfields": return_fields_str,
#     }

#     # Perform request
#     headers = auth_header or {}
#     response = requests.get(SEPA_BASE_URL, params=api_query, headers=headers, timeout=timeout)
#     response.raise_for_status()
#     json_content = response.json()[0]

#     # Handle API errors
#     if len(json_content.keys()) == 3 and "message" in json_content:
#         raise RuntimeError(json_content["message"])

#     if "rows" in json_content:
#         num_rows = sum(map(int, json_content["rows"]))
#         if num_rows == 0:
#             raise RuntimeError("No data available for selected ts_id(s).")

#     # Parse data
#     ts_cols = json_content["columns"]
#     if isinstance(ts_cols, list): # Actually not sure whether this would ever be the case
#         ts_cols = ts_cols[0]
#     ts_cols = ts_cols.split(',')
#     data = [dict(zip(ts_cols, row)) for row in json_content["data"]]
#     df = pd.DataFrame(data)

#     # Convert to proper types
#     if "Timestamp" in df.columns:
#         df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True)
#     if "Value" in df.columns:
#         df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

#     # Add metadata
#     if metadata:
#         for field in md_return_fields:
#             if field != "ca_sta" and field in json_content:
#                 df[field] = json_content[field]

#     if ca_sta:
#         for field in ca_sta_return_fields:
#             if field in json_content:
#                 df[field] = json_content[field]

#     return df

# # https://timeseriesdoc.sepa.org.uk/api-documentation/before-you-start/what-controls-there-are-on-access/
# api_key = None 
# auth_header = sepa_auth_headers(api_key)
# groups = sepa_group_list(auth_header=auth_header)
# flow_group = '270322'
# level_group = '615437'
# stns = sepa_station_list(group_id=flow_group, auth_header=auth_header)
# ts_list = sepa_timeseries_list(station_id = stns['station_id'].iloc[0], auth_header=auth_header)

# # Get the timeseries ID and coverage to build a request:
# ts_id = ts_list[(ts_list['stationparameter_name']=='Flow') & (ts_list['ts_name']=='Day.Mean')]['ts_id'].iloc[0]
# ts_start = ts_list[ts_list['ts_id']==ts_id]['from'].iloc[0]
# ts_end = ts_list[ts_list['ts_id']==ts_id]['to'].iloc[0]
# ts = sepa_timeseries_values(ts_id = ts_id, start_date=ts_start, end_date=ts_end, metadata=True)```