"""Utility functions for the RivRetrieve package."""

import datetime
import io
import logging
import pkgutil
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0"}


def format_start_date(start_date: Optional[str]) -> str:
    """Formats the start date, defaulting to 1900-01-01 if None."""
    if start_date is None:
        return "1900-01-01"
    try:
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        return start_date
    except ValueError:
        raise ValueError("Incorrect start_date format, should be YYYY-MM-DD")


def format_end_date(end_date: Optional[str]) -> str:
    """Formats the end date, defaulting to today if None."""
    if end_date is None:
        return datetime.date.today().strftime("%Y-%m-%d")
    try:
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
        return end_date
    except ValueError:
        raise ValueError("Incorrect end_date format, should be YYYY-MM-DD")


def get_column_name(variable: str) -> str:
    """Returns the standard column name for the given variable."""
    if variable == "stage":
        return "H"
    elif variable == "discharge":
        return "Q"
    else:
        raise ValueError(
            f"Unsupported variable: {variable}. Must be 'stage' or 'discharge'."
        )


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
) -> requests.Session:
    """Creates a requests session with retry logic."""
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def load_sites_csv(country_code: str) -> pd.DataFrame:
    """Loads site data from a CSV file in the data directory."""
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, "cached_site_data", f"{country_code}_sites.csv")
    try:
        return pd.read_csv(file_path, dtype={'site': str})
    except FileNotFoundError:
        logger.error(f"Site file not found: {file_path}")
        raise