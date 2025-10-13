import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
import os
from pathlib import Path
import xarray as xr
import shutil
import requests  # Import requests

from rivretrieve import PolandFetcher
from rivretrieve import constants


class TestPolandFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = PolandFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"
        self.test_cache_file = self.test_data_dir / "poland_test.zarr"

        # Ensure the test cache exists
        if not self.test_cache_file.exists():
            raise FileNotFoundError(
                f"Test cache file not found at {self.test_cache_file}. "
                "Run scripts/create_poland_test_data.py to generate it."
            )

    @patch("rivretrieve.poland.PolandFetcher._create_cache")  # Mock cache creation
    @patch(
        "rivretrieve.poland.PolandFetcher.CACHE_FILE",
        new_callable=lambda: Path(os.path.dirname(__file__))
        / "test_data"
        / "poland_test.zarr",
    )
    def test_get_data_discharge(self, mock_cache_file, mock_create_cache):
        gauge_id = "149180010"
        variable = constants.DISCHARGE
        start_date = "2020-01-01"
        end_date = "2020-01-05"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(
            ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]
        )
        # These values are from the generated test cache
        expected_values = [45.9, 41.0, 39.3, 37.5, 38.1]
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.DISCHARGE: expected_values,
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df.reset_index(drop=True), expected_df)
        mock_create_cache.assert_not_called()  # Cache should not be recreated

    @patch("rivretrieve.poland.PolandFetcher._create_cache")  # Mock cache creation
    @patch(
        "rivretrieve.poland.PolandFetcher.CACHE_FILE",
        new_callable=lambda: Path(os.path.dirname(__file__))
        / "test_data"
        / "poland_test.zarr",
    )
    def test_get_data_stage(self, mock_cache_file, mock_create_cache):
        gauge_id = "149180010"
        variable = constants.STAGE
        start_date = "2020-01-03"
        end_date = "2020-01-07"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(
            ["2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06", "2020-01-07"]
        )
        # These values are from the generated test cache (and divided by 100)
        expected_values = [1.19, 1.16, 1.17, 1.12, 1.07]
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.STAGE: expected_values,
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df.reset_index(drop=True), expected_df)
        mock_create_cache.assert_not_called()

    @patch("rivretrieve.utils.requests_retry_session")
    @patch("rivretrieve.poland.PolandFetcher._get_metadata_headers")
    def test_download_and_parse_all_data(self, mock_get_headers, mock_requests_session):
        # Mock headers
        mock_get_headers.return_value = [
            "Kod stacji",
            "Nazwa stacji",
            "Nazwa rzeki",
            "Rok hydrologiczny",
            "Miesiąc kalendarzowy",
            "Dzień",
            "Wskaźnik",
            "Przepływ [m3/s]",
            "Stan wody [cm]",
            "Temperatura wody [st. C]",
        ]

        # Mock requests to return local zip files
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        test_zip_dir = self.test_data_dir / "poland_zip_files" / "2022"

        def mock_get_side_effect(url, *args, **kwargs):
            mock_response = MagicMock()
            test_zip_dir = self.test_data_dir / "poland_zip_files" / "2022"
            if url.endswith(".zip"):
                fname = url.split("/")[-1]
                zip_path = test_zip_dir / fname
                if zip_path.exists():
                    with open(zip_path, "rb") as f:
                        mock_response.content = f.read()
                    mock_response.raise_for_status = MagicMock()
                    return mock_response
                else:
                    mock_response.status_code = 404
                    mock_response.raise_for_status.side_effect = (
                        requests.exceptions.HTTPError("404 Client Error")
                    )
                    return mock_response
            elif url.endswith("2022/"):
                # List only the test zip files
                mock_response.text = '<a href="codz_2022_01.zip">codz_2022_01.zip</a> <a href="codz_2022_02.zip">codz_2022_02.zip</a>'
                mock_response.raise_for_status = MagicMock()
                return mock_response
            return MagicMock()  # Should not be called for other URLs

        mock_session.get.side_effect = mock_get_side_effect

        raw_data_list = self.fetcher._download_all_data(2022, 2022)
        self.assertEqual(len(raw_data_list), 2)  # Two zip files

        parsed_df = self.fetcher._parse_all_data(raw_data_list)
        self.assertFalse(parsed_df.empty)
        self.assertGreater(
            len(parsed_df), 20000
        )  # Expect many rows from two months of data
        self.assertIn(constants.GAUGE_ID, parsed_df.columns)
        self.assertIn(constants.TIME_INDEX, parsed_df.columns)
        self.assertIn(constants.DISCHARGE, parsed_df.columns)
        self.assertIn(constants.STAGE, parsed_df.columns)
        self.assertIn(constants.WATER_TEMPERATURE, parsed_df.columns)

        # Check date range
        self.assertEqual(
            parsed_df[constants.TIME_INDEX].min(), pd.to_datetime("2022-01-01")
        )
        self.assertEqual(
            parsed_df[constants.TIME_INDEX].max(), pd.to_datetime("2022-02-28")
        )


if __name__ == "__main__":
    unittest.main()
