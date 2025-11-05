"Fetcher for Japanese river gauge data."

import calendar
import io
import logging
import re
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

from . import base, constants, utils

logger = logging.getLogger(__name__)

# Maps RivRetrieve variable to the single confirmed KIND value.
VARIABLE_KIND_MAP = {
    constants.STAGE_HOURLY_MEAN: 2,
    constants.STAGE_DAILY_MEAN: 3,
    constants.DISCHARGE_HOURLY_MEAN: 6,
    constants.DISCHARGE_DAILY_MEAN: 7,
}


class JapanFetcher(base.RiverDataFetcher):
    """Fetches river gauge data from Japan's Ministry of Land, Infrastructure, Transport and Tourism (MLIT).

    Data Source: Water Information System (http://www1.river.go.jp/)

    Note: KINDs 2 and 6, described as "Daily" on the website, actually provide HOURLY data.
    KINDs 3 and 7 are expected to provide true DAILY data.
    This fetcher returns data at the resolution provided in the source .dat files.

    Supported Variables:
        - ``constants.DISCHARGE_HOURLY_MEAN`` (m³/s)
        - ``constants.STAGE_HOURLY_MEAN`` (m)
        - ``constants.DISCHARGE_DAILY_MEAN`` (m³/s)
        - ``constants.STAGE_DAILY_MEAN`` (m)
    """

    BASE_URL = "http://www1.river.go.jp"
    DSP_URL = f"{BASE_URL}/cgi-bin/DspWaterData.exe"
    SITE_INFO_URL = f"{BASE_URL}/cgi-bin/SiteInfo.exe"

    @staticmethod
    def get_cached_metadata() -> pd.DataFrame:
        """Retrieves a DataFrame of available Japanese gauge IDs and metadata."""
        return utils.load_cached_metadata_csv("japan")

    @staticmethod
    def get_available_variables() -> tuple[str, ...]:
        return (
            constants.STAGE_HOURLY_MEAN,
            constants.STAGE_DAILY_MEAN,
            constants.DISCHARGE_HOURLY_MEAN,
            constants.DISCHARGE_DAILY_MEAN,
        )

    def _get_kind(self, variable: str) -> int:
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
        """Downloads raw .dat file contents."""
        s = utils.requests_retry_session()
        kind_to_try = self._get_kind(variable)

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        dat_contents = []
        headers = {"User-Agent": "Mozilla/5.0", "Referer": self.BASE_URL}

        if kind_to_try in [2, 6]:  # Monthly requests for hourly data
            current_dt = start_dt.replace(day=1)
            while current_dt <= end_dt:
                year = current_dt.year
                month = current_dt.month
                month_str = f"{month:02d}"
                last_day = calendar.monthrange(year, month)[1]
                month_start_str = f"{year}{month_str}01"
                month_end_str = f"{year}{month_str}{last_day}"

                params = {
                    "KIND": kind_to_try,
                    "ID": gauge_id,
                    "BGNDATE": month_start_str,
                    "ENDDATE": month_end_str,
                    "KAWABOU": "NO",
                }
                try:
                    logger.debug(f"Fetching DspWaterData page for {gauge_id} {year}-{month_str} KIND {kind_to_try}")
                    response = s.get(self.DSP_URL, params=params, headers=headers)
                    response.raise_for_status()
                    response.encoding = "EUC-JP"
                    soup = BeautifulSoup(response.text, "html.parser")
                    link_tag = soup.find(re.compile("a", re.IGNORECASE), href=re.compile(r"/dat/dload/download/"))
                    if link_tag:
                        dat_url = f"{self.BASE_URL}{link_tag['href']}"
                        dat_response = s.get(dat_url, headers=headers)
                        dat_response.raise_for_status()
                        dat_contents.append(dat_response.content.decode("shift_jis", errors="replace"))
                        logger.info(f"Successfully downloaded {link_tag['href'].split('/')[-1]}")
                    else:
                        logger.warning(f"No .dat link found for {gauge_id} {year}-{month_str} KIND {kind_to_try}")
                except Exception as e:
                    logger.error(f"Error fetching for {gauge_id} {year}-{month_str} KIND {kind_to_try}: {e}")
                current_dt += relativedelta(months=1)
        elif kind_to_try in [3, 7]:  # Yearly requests for daily data
            for year in range(start_dt.year, end_dt.year + 1):
                year_start_str = f"{year}0131"
                year_end_str = f"{year}1231"
                params = {
                    "KIND": kind_to_try,
                    "ID": gauge_id,
                    "BGNDATE": year_start_str,
                    "ENDDATE": year_end_str,
                    "KAWABOU": "NO",
                }
                try:
                    logger.debug(f"Fetching DspWaterData page for {gauge_id} {year} KIND {kind_to_try}")
                    response = s.get(self.DSP_URL, params=params, headers=headers)
                    response.raise_for_status()
                    response.encoding = "EUC-JP"
                    soup = BeautifulSoup(response.text, "html.parser")
                    link_tag = soup.find(re.compile("a", re.IGNORECASE), href=re.compile(r"/dat/dload/download/"))
                    if link_tag:
                        dat_url = f"{self.BASE_URL}{link_tag['href']}"
                        dat_response = s.get(dat_url, headers=headers)
                        dat_response.raise_for_status()
                        dat_contents.append(dat_response.content.decode("shift_jis", errors="replace"))
                        logger.info(f"Successfully downloaded {link_tag['href'].split('/')[-1]}")
                    else:
                        logger.warning(f"No .dat link found for {gauge_id} {year} KIND {kind_to_try}")
                except Exception as e:
                    logger.error(f"Error fetching for {gauge_id} {year} KIND {kind_to_try}: {e}")
        return dat_contents

    def _parse_data(
        self,
        gauge_id: str,
        raw_data_list: List[str],
        variable: str,
    ) -> pd.DataFrame:
        """Parses the list of monthly .dat file contents."""
        if not raw_data_list:
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

        kind = self._get_kind(variable)
        all_dfs = []

        for dat_content in raw_data_list:
            try:
                lines = dat_content.strip().splitlines()
                data_lines = [line for line in lines if not line.startswith("#") and line.strip()]

                if not data_lines:
                    continue

                header_line = next((line for line in lines if line.startswith(",")), None)
                if header_line:
                    try:
                        header_index = lines.index(header_line)
                        data_lines = lines[header_index + 1 :]
                        data_lines = [line for line in data_lines if not line.startswith("#") and line.strip()]
                    except ValueError:
                        pass
                if not data_lines:
                    continue

                csv_io = io.StringIO("\n".join(data_lines))

                if kind in [2, 6]:  # Hourly data format
                    col_names = [constants.TIME_INDEX]
                    for i in range(1, 25):
                        col_names.append(f"{i}時")
                        col_names.append(f"{i}時フラグ")

                    df = pd.read_csv(
                        csv_io, header=None, names=col_names, na_values=["-9999.00"], dtype={constants.TIME_INDEX: str}
                    )
                    df[constants.TIME_INDEX] = pd.to_datetime(
                        df[constants.TIME_INDEX], format="%Y/%m/%d", errors="coerce"
                    )
                    df = df.dropna(subset=[constants.TIME_INDEX])

                    value_cols = [f"{i}時" for i in range(1, 25)]
                    df_long = df.melt(
                        id_vars=[constants.TIME_INDEX], value_vars=value_cols, var_name="Hour", value_name="Value"
                    )
                    df_long["Hour"] = df_long["Hour"].str.replace("時", "").astype(int)
                    df_long["Value"] = pd.to_numeric(df_long["Value"], errors="coerce")
                    df_long = df_long.dropna(subset=["Value"])

                    df_long[constants.TIME_INDEX] = df_long.apply(
                        lambda row: row[constants.TIME_INDEX] + timedelta(hours=row["Hour"] - 1), axis=1
                    )
                    parsed_df = df_long[[constants.TIME_INDEX, "Value"]].rename(columns={"Value": variable})
                    all_dfs.append(parsed_df)

                elif kind in [3, 7]:  # Daily data format
                    year = None
                    for line in lines:
                        if "年" in line:
                            year_match = re.search(r"(\d{4})年", line)
                            if year_match:
                                year = int(year_match.group(1))
                                break
                    if year is None:
                        logger.warning(f"Could not extract year from .dat file for {gauge_id} KIND {kind}")
                        continue

                    col_names = ["月"]
                    for i in range(1, 32):
                        col_names.append(f"{i}日")
                        col_names.append(f"{i}日フラグ")

                    df = pd.read_csv(
                        csv_io, header=None, names=col_names, na_values=["　", "-9999.00"], encoding="utf-8"
                    )

                    month_map = {f"{i}月": i for i in range(1, 13)}
                    df["Month"] = df["月"].map(month_map)
                    df = df.dropna(subset=["Month"])
                    df["Year"] = year

                    value_cols = [f"{i}日" for i in range(1, 32)]
                    df_long = df.melt(
                        id_vars=["Year", "Month"], value_vars=value_cols, var_name="Day", value_name="Value"
                    )
                    df_long["Day"] = df_long["Day"].str.replace("日", "").astype(int)
                    df_long["Value"] = pd.to_numeric(df_long["Value"], errors="coerce")
                    df_long = df_long.dropna(subset=["Value"])

                    df_long[constants.TIME_INDEX] = pd.to_datetime(df_long[["Year", "Month", "Day"]], errors="coerce")
                    parsed_df = df_long[[constants.TIME_INDEX, "Value"]].rename(columns={"Value": variable})
                    parsed_df = parsed_df.dropna(subset=[constants.TIME_INDEX])
                    all_dfs.append(parsed_df)
                else:
                    logger.warning(f"Unsupported KIND {kind} for parsing in _parse_data")
                    continue

            except Exception as e:
                logger.error(f"Error parsing .dat content for {gauge_id} KIND {kind}: {e}", exc_info=True)
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
        """Fetches and parses time series data for a specific gauge and variable."""
        start_date = utils.format_start_date(start_date)
        end_date = utils.format_end_date(end_date)
        if variable not in self.get_available_variables():
            raise ValueError(f"Unsupported variable: {variable}")

        try:
            raw_data_list = self._download_data(gauge_id, variable, start_date, end_date)
            if not raw_data_list:
                return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

            df = self._parse_data(gauge_id, raw_data_list, variable)

            if not df.empty:
                start_date_dt = pd.to_datetime(start_date)
                end_date_dt = pd.to_datetime(end_date) + timedelta(days=1)  # Include end date
                df = df[(df.index >= start_date_dt) & (df.index < end_date_dt)]
            return df

        except Exception as e:
            logger.error(f"Failed to get data for site {gauge_id}, variable {variable}: {e}", exc_info=True)
            return pd.DataFrame(columns=[constants.TIME_INDEX, variable])

    def get_metadata(self, gauge_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Fetches metadata for given gauge IDs from the MLIT Water Information System."""
        if gauge_ids is None:
            cached_meta = self.get_cached_metadata()
            gauge_ids = cached_meta.index.tolist()

        all_station_data = []
        s = utils.requests_retry_session()
        headers = {"User-Agent": "Mozilla/5.0", "Referer": self.BASE_URL}

        for gauge_id in gauge_ids:
            logger.info(f"Fetching metadata for station: {gauge_id}")
            site_info_url = f"{self.SITE_INFO_URL}?ID={gauge_id}"
            try:
                response = s.get(site_info_url, headers=headers)
                response.raise_for_status()
                response.encoding = "EUC-JP"
                soup = BeautifulSoup(response.text, "html.parser")

                station_data = {constants.GAUGE_ID: gauge_id}

                info_table = soup.find("table", {"align": "CENTER", "width": "600"})
                if info_table:
                    for row in info_table.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) == 2:
                            key = cells[0].text.strip()
                            value = cells[1].text.strip()
                            if key == "観測所名":
                                station_data[constants.STATION_NAME] = value
                            elif key == "所在地":
                                station_data["location"] = value
                            elif key == "水系名":
                                station_data[constants.RIVER] = value
                            elif key == "河川名":
                                station_data["river_name_jp"] = value
                            elif key == "緯度経度":
                                try:
                                    lat_match = re.search(r"N(\d+)度(\d+)分(\d+)秒", value)
                                    lon_match = re.search(r"E(\d+)度(\d+)分(\d+)秒", value)
                                    if lat_match:
                                        lat = (
                                            float(lat_match.group(1))
                                            + float(lat_match.group(2)) / 60
                                            + float(lat_match.group(3)) / 3600
                                        )
                                        station_data[constants.LATITUDE] = lat
                                    if lon_match:
                                        lon = (
                                            float(lon_match.group(1))
                                            + float(lon_match.group(2)) / 60
                                            + float(lon_match.group(3)) / 3600
                                        )
                                        station_data[constants.LONGITUDE] = lon
                                except Exception as e:
                                    logger.warning(f"Could not parse lat/lon for {gauge_id}: {value} - {e}")

                # Fetch available kinds for the station
                kind_map = {}
                # Commenting out the SiteInfo fetch for KINDs due to 403 errors
                # try:
                #     # This part is still blocked by 403, so kind_map will be empty
                #     pass # s_kinds = utils.requests_retry_session()
                #     # response = s_kinds.get(site_info_url, headers=headers)
                #     # response.raise_for_status()
                #     # ... parsing logic ...
                # except Exception as e:
                #     logger.error(f"Error fetching/parsing SiteInfo for {gauge_id} for KINDS: {e}")
                station_data["available_kinds"] = kind_map
                all_station_data.append(station_data)

            except requests.exceptions.RequestException as e:
                if e.response and e.response.status_code == 403:
                    logger.error(f"Access forbidden for SiteInfo {gauge_id}: {e}")
                else:
                    logger.error(f"Error fetching SiteInfo for {gauge_id}: {e}")
            except Exception as e:
                logger.error(f"Error parsing SiteInfo for {gauge_id}: {e}", exc_info=True)

        df = pd.DataFrame(all_station_data)
        if not df.empty:
            return df.set_index(constants.GAUGE_ID)
        else:
            return pd.DataFrame(columns=[constants.GAUGE_ID]).set_index(constants.GAUGE_ID)
