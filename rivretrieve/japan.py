
"Fetcher for Japanese river gauge data."

import io
import logging
import re
import calendar
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

from . import base, constants, utils

logger = logging.getLogger(__name__)

# Map Japanese descriptions to RivRetrieve constants
KIND_DESC_MAP = {
    "時間水位": constants.STAGE_INSTANT,
    "日水位": constants.STAGE_DAILY_MEAN,
    "時間流量": constants.DISCHARGE_INSTANT,
    "日流量": constants.DISCHARGE_DAILY_MEAN,
    "リアルタイム水位": constants.STAGE_INSTANT,  # Real-time is also instantaneous
    "リアルタイム流量": constants.DISCHARGE_INSTANT,
}

# Inverse map to get KIND from variable
# This is a preliminary map, can be refined
VARIABLE_KIND_MAP = {
    constants.STAGE_INSTANT: [1, 9],
    constants.STAGE_DAILY_MEAN: [2],
    constants.DISCHARGE_INSTANT: [5, 10], # Assuming 10 might exist for real-time discharge
    constants.DISCHARGE_DAILY_MEAN: [6],
}

class JapanFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Japan's Ministry of Land, Infrastructure, Transport and Tourism (MLIT).

    Data Source: Water Information System (http://www1.river.go.jp/)

    Supported Variables:
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
        - ``constants.DISCHARGE_INSTANT`` (m³/s)
        - ``constants.STAGE_INSTANT`` (m)
    """

    BASE_URL = "http://www1.river.go.jp"
    DSP_URL = f"{BASE_URL}/cgi-bin/DspWaterData.exe"
    SITE_INFO_URL = f"{BASE_URL}/cgi-bin/SiteInfo.exe"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available Japanese gauge IDs and metadata.

        This method loads the metadata from a cached CSV file located in
        the ``rivretrieve/cached_site_data/`` directory.

        Returns:
            pd.DataFrame: A DataFrame indexed by gauge_id, containing site metadata.
        """
        return utils.load_cached_metadata_csv("japan")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.DISCHARGE_DAILY_MEAN,
            constants.STAGE_DAILY_MEAN,
            constants.DISCHARGE_INSTANT,
            constants.STAGE_INSTANT,
        )

    def _get_kind(self, variable: str) -> Optional[List[int]]:
        if variable in VARIABLE_KIND_MAP:
            return VARIABLE_KIND_MAP[variable]
        else:
            raise ValueError(f"Unsupported variable: {variable}")

    def _download_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """Downloads raw .dat file contents month by month."""
        possible_kinds = self._get_kind(variable)
        if not possible_kinds:
            logger.error(f"No KIND found for variable {variable}")
            return []

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        current_dt = start_dt.replace(day=1)
        monthly_dat_contents = []
        s = utils.requests_retry_session()

        # To pick the best KIND, we might need to check SiteInfo.exe,
        # but for now, let's try the first one in the list.
        kind = possible_kinds[0]
        logger.info(f"Using KIND={kind} for {variable}")

        while current_dt <= end_dt:
            year = current_dt.year
            month = current_dt.month
            month_str = f"{month:02d}"
            last_day = calendar.monthrange(year, month)[1]

            month_start_str = f"{year}{month_str}01"
            month_end_str = f"{year}{month_str}{last_day}"

            params = {
                "KIND": kind,
                "ID": gauge_id,
                "BGNDATE": month_start_str,
                "ENDDATE": month_end_str,
                "KAWABOU": "NO",
            }

            try:
                logger.debug(f"Fetching DspWaterData page for {gauge_id} {year}-{month_str}")
                response = s.get(self.DSP_URL, params=params)
                response.raise_for_status()
                response.encoding = "EUC-JP"
                soup = BeautifulSoup(response.text, 'html.parser')

                link_tag = soup.find('a', href=re.compile(r"/dat/dload/download/.*\.dat"))
                if link_tag:
                    dat_url_path = link_tag['href']
                    dat_url = f"{self.BASE_URL}{dat_url_path}"
                    logger.debug(f"Found .dat link: {dat_url}")

                    dat_response = s.get(dat_url)
                    dat_response.raise_for_status()
                    dat_content = dat_response.content.decode('shift_jis', errors='replace')
                    monthly_dat_contents.append(dat_content)
                    logger.info(f"Successfully downloaded {dat_url_path.split('/')[-1]}")
                else:
                    logger.warning(f"No .dat link found for site {gauge_id} for {year}-{month_str} with KIND {kind}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data for site {gauge_id} for {year}-{month_str}: {e}")
            except Exception as e:
                logger.error(f"Error processing data for site {gauge_id} for {year}-{month_str}: {e}")

            current_dt += relativedelta(months=1)

        return monthly_dat_contents

    def _parse_data(
        self,
        gauge_id: str,
        raw_data_list: List[str],
        variable: str,
    ) -> pd.DataFrame:
        """Parses the list of monthly .dat file contents."""
        if not raw_data_list:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        all_dfs = []
        for dat_content in raw_data_list:
            try:
                lines = dat_content.strip().split('\r\n')
                data_lines = [line for line in lines if not line.startswith('#') and line.strip()]
                
                if not data_lines:
                    continue

                if not data_lines[0].startswith(','): # Data starts after header
                    data_lines = data_lines[1:]

                if not data_lines:
                    continue

                # The first column is Date, followed by 24 pairs of (Value, Flag)
                col_names = [constants.TIME_INDEX]
                for i in range(1, 25):
                    col_names.append(f"{i}時")
                    col_names.append(f"{i}時フラグ")

                # Read the data part
                csv_io = io.StringIO('\n'.join(data_lines))
                df = pd.read_csv(csv_io, header=None, names=col_names, na_values=["-9999.00"], dtype={constants.TIME_INDEX: str})

                df[constants.TIME_INDEX] = pd.to_datetime(df[constants.TIME_INDEX], format="%Y/%m/%d", errors="coerce")
                df = df.dropna(subset=[constants.TIME_INDEX])

                # Melt hourly columns
                value_cols = [f"{i}時" for i in range(1, 25)]
                
                df_long = df.melt(id_vars=[constants.TIME_INDEX], value_vars=value_cols, var_name='Hour', value_name='Value')
                df_long['Hour'] = df_long['Hour'].str.replace('時', '').astype(int)
                df_long['Value'] = pd.to_numeric(df_long['Value'], errors='coerce')
                df_long = df_long.dropna(subset=['Value'])

                if constants.INSTANTANEOUS in variable:
                    # Build datetime index for hourly data
                    df_long[constants.TIME_INDEX] = df_long.apply(
                        lambda row: row[constants.TIME_INDEX] + timedelta(hours=row['Hour'] - 1), axis=1
                    )
                    hourly_df = df_long[[constants.TIME_INDEX, 'Value']].rename(columns={'Value': variable})
                    all_dfs.append(hourly_df)
                elif constants.DAILY in variable:
                    # Calculate daily mean
                    daily_df = df_long.groupby(constants.TIME_INDEX)['Value'].mean().reset_index()
                    daily_df = daily_df.rename(columns={'Value': variable})
                    all_dfs.append(daily_df)

            except Exception as e:
                logger.error(f"Error parsing .dat content for {gauge_id}: {e}", exc_info=True)
                continue

        if not all_dfs:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.sort_values(by=constants.TIME_INDEX)

        return final_df.set_index(constants.TIME_INDEX)

    def get_data(
        self,
        gauge_id: str,
        variable: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetches and parses time series data for a specific gauge and variable.

        This method retrieves the requested data from the provider's API or data source,
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
            requests.exceptions.RequestException: If a network error occurs during data download.
            Exception: For other unexpected errors during data fetching or parsing.
        """
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data_list = self._download_data(gauge_id, variable, start_date, end_date)
            df = self._parse_data(gauge_id, raw_data_list, variable)

            if not df.empty:
                start_date_dt = pd.to_datetime(start_date)
                end_date_dt = pd.to_datetime(end_date)
                # For daily data, index is date. For hourly, index is datetime.
                if constants.DAILY in variable:
                    df = df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]
                elif constants.INSTANTANEOUS in variable:
                     df = df[(df.index >= start_date_dt) & (df.index <= pd.to_datetime(end_date) + timedelta(days=1))]
            return df

        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}")
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def get_metadata(self, gauge_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Fetches metadata for given gauge IDs from the MLIT Water Information System.

        Args:
            gauge_ids: A list of gauge IDs to fetch metadata for. If None, IDs are loaded from the cached CSV.

        Returns:
            A pandas DataFrame containing metadata for the stations, indexed by gauge_id.
        """
        if gauge_ids is None:
            cached_meta = self.get_cached_metadata()
            gauge_ids = cached_meta.index.tolist()

        all_station_data = []
        s = utils.requests_retry_session()

        for gauge_id in gauge_ids:
            logger.info(f"Fetching metadata for station: {gauge_id}")
            site_info_url = f"{self.BASE_URL}/cgi-bin/SiteInfo.exe?ID={gauge_id}"
            try:
                response = s.get(site_info_url)
                response.raise_for_status()
                response.encoding = "EUC-JP"
                soup = BeautifulSoup(response.text, 'html.parser')

                station_data = {constants.GAUGE_ID: gauge_id}

                # Extract metadata from the main table
                info_table = soup.find('table', {'align': 'CENTER', 'width': '600'})
                if info_table:
                    for row in info_table.find_all('tr'):
                        cells = row.find_all('td')
                        if len(cells) == 2:
                            key = cells[0].text.strip()
                            value = cells[1].text.strip()
                            if key == '観測所名':
                                station_data[constants.STATION_NAME] = value
                            elif key == '所在地':
                                station_data['location'] = value
                            elif key == '水系名':
                                station_data[constants.RIVER] = value # Approximate
                            elif key == '河川名':
                                station_data['river_name_jp'] = value
                            elif key == '緯度経度':
                                try:
                                    # Format: N34度2分2秒  E132度26分5秒
                                    lat_match = re.search(r'N(\d+)度(\d+)分(\d+)秒', value)
                                    lon_match = re.search(r'E(\d+)度(\d+)分(\d+)秒', value)
                                    if lat_match:
                                        lat = float(lat_match.group(1)) + float(lat_match.group(2))/60 + float(lat_match.group(3))/3600
                                        station_data[constants.LATITUDE] = lat
                                    if lon_match:
                                        lon = float(lon_match.group(1)) + float(lon_match.group(2))/60 + float(lon_match.group(3))/3600
                                        station_data[constants.LONGITUDE] = lon
                                except Exception as e:
                                    logger.warning(f"Could not parse lat/lon for {gauge_id}: {value} - {e}")

                # Extract available data types (KINDs)
                kind_map = {}
                data_links = soup.find_all('a', href=re.compile(r"DspWaterData\.exe\?KIND="))
                for link in data_links:
                    href = link['href']
                    kind_match = re.search(r"KIND=(\d+)", href)
                    if kind_match:
                        kind = int(kind_match.group(1))
                        img_tag = link.find('img')
                        if img_tag and img_tag.get('alt'):
                            alt_text = img_tag['alt'].strip()
                            kind_map[kind] = alt_text
                station_data['available_kinds'] = kind_map

                all_station_data.append(station_data)

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching SiteInfo for {gauge_id}: {e}")
            except Exception as e:
                logger.error(f"Error parsing SiteInfo for {gauge_id}: {e}", exc_info=True)

        df = pd.DataFrame(all_station_data)
        if not df.empty:
            return df.set_index(constants.GAUGE_ID)
        else:
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)

