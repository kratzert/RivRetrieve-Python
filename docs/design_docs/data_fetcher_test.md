# Design Document: Data Fetcher Unit Tests

This document provides a comprehensive guide for writing unit tests for new river gauge data fetchers in the RivRetrieve library.

## The Golden Rule of RivRetrieve Testing

**Each unit test must ONLY mock the call to the external data provider. Everything else from our code (parsing, date formatting, unit conversions) MUST be tested.** 

The mocked data must be a **real, raw payload** obtained from the API. We do not invent mock data structures; we capture real API responses and use them to ensure our parsing logic works against the actual data formats provided by the sources. However, it is enough to test against a short time period
of a few days.

## Directory Structure

- **Test File:** `tests/test_<country>.py` (e.g., `tests/test_brazil.py`)
- **Test Data:** `tests/test_data/<country>_<gauge_id>_<variable>_<date>.<ext>` (e.g., `tests/test_data/uk_nrfa_1001_discharge_20220101.json`)

## 1. Obtaining Test Data

Before writing the test, you need real payloads.
1. Temporarily add print statements or a debugger to your fetcher's `_download_data` method just before it parses the raw response.
2. Run your fetcher using a script in `examples/` for a short time range (e.g., 3-5 days).
3. Save the exact raw response (JSON, CSV, HTML, XML, or binary) to a file in the `tests/test_data/` directory.
4. *Exception*: If the payload is extremely small (e.g., a simple JSON dict with a few keys), you can define it directly in the test file as a Python dictionary.

## 2. Test Class Structure

All tests should inherit from `unittest.TestCase`.

### `setUp` Method
Use the `setUp` method to initialize your fetcher and define the path to your test data.

```python
import os
import json
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from pandas.testing import assert_frame_equal
from rivretrieve import MyCountryFetcher, constants

class TestMyCountryFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = MyCountryFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def load_sample_data(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r", encoding="utf-8") as f:
            return f.read()

    def load_sample_json(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r", encoding="utf-8") as f:
            return json.load(f)
```

## 3. Mocking Strategies

You must mock the boundary where our code leaves the system. In 95% of cases, this is the `requests` library.

### Mocking `requests_retry_session`
If your fetcher uses `utils.requests_retry_session().get(...)`:

```python
    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self.load_sample_data("mycountry_sample.csv")
        # OR: mock_response.json.return_value = self.load_sample_json("mycountry_sample.json")
        mock_response.raise_for_status = MagicMock()
        
        mock_session.get.return_value = mock_response

        # ... proceed with calling fetcher.get_data(...)
```

### Mocking Multiple Sequential API Calls
If the fetcher needs to call multiple endpoints (e.g., one for a token/metadata, one for the actual data), use `side_effect`:

```python
        def mock_get_side_effect(url, *args, **kwargs):
            mock_response = MagicMock()
            if "metadata_endpoint" in url:
                mock_response.json.return_value = self.load_sample_json("meta.json")
            elif "data_endpoint" in url:
                mock_response.json.return_value = self.load_sample_json("data.json")
            mock_response.raise_for_status = MagicMock()
            return mock_response

        mock_session.get.side_effect = mock_get_side_effect
```

### Mocking External Libraries
If the fetcher uses a dedicated external client library (e.g., `dataretrieval` for USA), mock the library's function:

```python
    @patch("dataretrieval.nwis.get_dv")
    def test_get_data_discharge(self, mock_get_dv):
        mock_get_dv.return_value = (self.load_sample_csv_as_df(), MagicMock())
```

## 4. Assertions and Validation

Your test must execute `get_data()` and validate the returned DataFrame against an expected DataFrame constructed manually in the test.

```python
        gauge_id = "12345"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2020-01-01"
        end_date = "2020-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        # Build the exact expected output. 
        # Make sure to apply any unit conversions here that the fetcher should have done!
        expected_dates = pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"])
        expected_values = [10.5, 11.2, 9.8] # Already converted to SI units (m³/s)
        
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            variable: expected_values,
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        # Assert DataFrame matches perfectly
        assert_frame_equal(result_df, expected_df, check_dtype=False)

        # Assert the mocked API was called with the correct parameters
        mock_session.get.assert_called_once()
        args, kwargs = mock_session.get.call_args
        self.assertIn("12345", args[0] if args else kwargs.get("url", ""))
```

## 5. Testing Metadata (`get_metadata`)

If your fetcher implements the optional `get_metadata()` method, write a test for it:
1. Save the raw metadata payload.
2. Mock the request.
3. Assert that the resulting DataFrame has the index named `constants.GAUGE_ID`.
4. Assert that standard columns like `constants.STATION_NAME`, `constants.LATITUDE`, `constants.LONGITUDE`, etc., are present and correctly mapped.

## Summary Checklist
- [ ] Named `test_<country>.py`
- [ ] Raw test payload saved in `tests/test_data/`
- [ ] Only the HTTP call or external library call is mocked
- [ ] DataFrame is compared using `assert_frame_equal`
- [ ] Mock call arguments are verified (`assert_called_once_with`, etc.)
- [ ] Target variables are tested independently (e.g., test Discharge, test Stage)
