# Design Document: Data Fetcher Implementation

This document provides a guide for implementing a new river gauge data fetcher for the RivRetrieve library.

## Goal

The goal is to create a standardized interface for downloading and parsing streamflow, water level and any other river measurement data from various national and regional data providers.

## Architecture

Every data fetcher must inherit from the `RiverDataFetcher` abstract base class defined in `rivretrieve.base`.

### Key Components

#### 1. Variable Definitions (`rivretrieve.constants`)

All fetchers must use the standardized variable names defined in `rivretrieve.constants`. Common variables include:

- `DISCHARGE_DAILY_MEAN`
- `DISCHARGE_INSTANT`
- `STAGE_DAILY_MEAN`
- `STAGE_INSTANT`

If data providers include variables that are not yet defined in `rivretrieve.constants`, we define
a new name there, then again used the globally defined constant.

Output data must be converted to SI units, e.g.
- Discharge: Cubic meters per second (m³/s)
- Stage: Meters (m)

#### 2. Class Structure

A new fetcher class (e.g., `USAFetcher`) should be implemented in its own file (e.g., `rivretrieve/usa.py`).

```python
from typing import Optional
import pandas as pd
from . import base, constants, utils

class MyCountryFetcher(base.RiverDataFetcher):
    # Implementation details...
```

#### 3. Common Utility Functions (`rivretrieve.utils`)

- **`format_start_date(start_date)` / `format_end_date(end_date)`**: Standardizes date strings to 'YYYY-MM-DD'.
- **`requests_retry_session()`**: Returns a `requests.Session` object with built-in retry logic for handling transient network errors.
- **`load_cached_metadata_csv(country_code)`**: Loads the site metadata from `rivretrieve/cached_site_data/{country_code}_sites.csv`.

#### 4. Mandatory Methods

- **`get_available_variables() -> tuple[str, ...]`**:
  Returns a tuple of the `constants` supported by this fetcher.

- **`get_cached_metadata() -> pd.DataFrame`**:
  Retrieves available gauge IDs and metadata from a cached CSV file. Use `utils.load_cached_metadata_csv("country_name")`.

- **`_download_data(gauge_id, variable, start_date, end_date) -> any`**:
  Handles the low-level data retrieval (e.g., via `requests` or a provider-specific library).
  - `start_date` and `end_date` are strings in 'YYYY-MM-DD' format.
  - Returns raw data (e.g., a `pd.DataFrame`, `dict`, or `str`).

- **`_parse_data(gauge_id, raw_data, variable) -> pd.DataFrame`**:
  Parses the raw data into a standardized `pd.DataFrame`.
  - Index: `pd.DatetimeIndex` named `constants.TIME_INDEX`.
  - Column: A single column named after the `variable`.
  - Handles unit conversions to SI.
  - Handles missing data (NaN). Important: Different countries might use different constants to 
  indicate missing data (e.g. sometimes negativ values like `-999`, sometimes strings `MISSING`, `LUECKE`). We always want to convert these country specific constants to `np.nan`. 

- **`get_data(gauge_id, variable, start_date, end_date) -> pd.DataFrame`**:
  The main entry point for users. It should:
  1. Format dates using `utils.format_start_date` and `utils.format_end_date`.
  2. Validate the `variable`.
  3. Call `_download_data` and `_parse_data`.
  4. Return the standardized `pd.DataFrame`.

#### 5. Metadata Handling

Metadata should be cached as a CSV file in `rivretrieve/cached_site_data/`. The CSV should use standard column names from `constants.py` for commonly used information:
- `GAUGE_ID` (index)
- `STATION_NAME`
- `RIVER`
- `LATITUDE`
- `LONGITUDE`
- `ALTITUDE`
- `AREA`
- `COUNTRY`

However, the metadata doesn't have to be restricted to these columns and can include any additional 
column with it's original column name.

#### 6. Optional Methods

- **`get_metadata(self) -> pd.DataFrame`**:
  Downloads and parses site metadata directly from the data provider. If a live metadata endpoint is available, this method should download the raw data, rename the columns to the standard `constants`, add `constants.COUNTRY` and `constants.SOURCE` where appropriate, ensure coordinate types are correctly converted, and return a DataFrame indexed by `constants.GAUGE_ID`.

## Implementation Steps

1.  **Identify the Data Source**: Determine the provider's API or data download URL.
2.  **Define Supported Variables**: Map the provider's variables to `rivretrieve.constants`.
3.  **Implement `_download_data`**: Use `requests` or other tools to fetch raw data.
4.  **Implement `_parse_data`**: Convert the raw format to the standardized `pd.DataFrame`.
5.  **Create Metadata**: Prepare the `cached_site_data/country.csv` file.


### 6. Verification

- **Example Script**: Add a script to `examples/` (e.g., `download_mycountry_data.py`) that demonstrates using the new fetcher for a single gauge and plots the result.
- **Unit Tests**: **Crucial Step**. You must create a corresponding test file (e.g. `tests/test_country.py`). 
  - **See the full testing guide in [data_fetcher_test.md](data_fetcher_test.md) for detailed instructions.**
  - **The Golden Rule**: Each unit test must ONLY mock the call to the external data provider. Everything else from our code (parsing, date formatting, unit conversions) MUST be tested. The mocked data must be a **real, raw payload** obtained from the API and stored in `tests/test_data/`.
  - Use `pandas.testing.assert_frame_equal` to compare the fetcher's output against a known `expected_df`.

## Best Practices

- **Standardized Empty DataFrames**: If an API request fails or no data is found, always catch exceptions and return an empty DataFrame with the correct columns: `pd.DataFrame(columns=[constants.TIME_INDEX, variable])`. Do not return `None`.
- **Date Filtering**: APIs frequently return data in whole months or years. Make sure the final return in `get_data()` perfectly filters the DataFrame to exactly match the requested `start_date` and `end_date` using `df[(df.index >= start_date_dt) & (df.index <= end_date_dt)]`.
- **Authentication & Credentials**: If the provider requires an API key or password, use `python-dotenv` and load credentials from a `.env` file (e.g., `os.environ.get("MY_API_KEY")`). Ensure `__init__` can optionally accept these credentials directly as kwargs.
- **Pagination & Chunking**: When fetching large time ranges, chunk the requests (e.g., by year or month) within `_download_data` to prevent timeout or payload size errors.
- **API Limits & Throttling**: Some APIs have strict request limits. Implement proper throttling (e.g., with `time.sleep()`) and handle `HTTP 429` appropriately to be respectful of external servers.
- **Bulk Downloads & Caching**: For providers without a robust time-series API, a common architectural pattern is to download bulk datasets (e.g., a large zip file) on the first request, save it to `rivretrieve/data/`, cache the processed data locally (e.g., as `.zarr` or `.sqlite3`), and serve subsequent queries directly from this local cache.
- **Class Docstrings**: Ensure the fetcher class has a docstring specifying the "Data Source:" (with a URL) and "Supported Variables:" (listing the `constants` used).
- Use `logging` for errors and warnings.
- Use `pd.to_numeric(..., errors="coerce")` to handle malformed data gracefully.
- Ensure all datetime objects are timezone-naive or consistently handled (prefer UTC).
- Avoid dropping columns that are not explicitly renamed during metadata parsing.
