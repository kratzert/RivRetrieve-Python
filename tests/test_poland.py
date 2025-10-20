import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import requests
from pandas.testing import assert_frame_equal

from rivretrieve import PolandFetcher, constants


class TestPolandFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = PolandFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"
        self.test_zip_file = self.test_data_dir / "poland_test.zarr.zip"

        if not self.test_zip_file.exists():
            raise FileNotFoundError(f"Test zip file not found at {self.test_zip_file}.")

        self.temp_dir = tempfile.TemporaryDirectory()
        with zipfile.ZipFile(self.test_zip_file, "r") as zip_ref:
            zip_ref.extractall(self.temp_dir.name)

        self.test_cache_file = Path(self.temp_dir.name) / "poland_test.zarr"

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("rivretrieve.poland.PolandFetcher._create_cache")  # Mock cache creation
    def test_get_data_discharge(self, mock_create_cache):
        with patch("rivretrieve.poland.PolandFetcher.CACHE_FILE", self.test_cache_file):
            gauge_id = "149180010"
            variable = constants.DISCHARGE_DAILY_MEAN
            start_date = "2020-01-01"
            end_date = "2020-01-05"

            result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

            expected_dates = pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"])
            expected_values = [45.9, 41.0, 39.3, 37.5, 38.1]
            expected_data = {
                constants.TIME_INDEX: expected_dates,
                constants.DISCHARGE_DAILY_MEAN: expected_values,
            }
            expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

            assert_frame_equal(result_df, expected_df)
            mock_create_cache.assert_not_called()  # Cache should not be recreated

    @patch("rivretrieve.poland.PolandFetcher._create_cache")  # Mock cache creation
    def test_get_data_stage(self, mock_create_cache):
        with patch("rivretrieve.poland.PolandFetcher.CACHE_FILE", self.test_cache_file):
            gauge_id = "149180010"
            variable = constants.STAGE_DAILY_MEAN
            start_date = "2020-01-03"
            end_date = "2020-01-07"

            result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

            expected_dates = pd.to_datetime(["2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06", "2020-01-07"])
            expected_values = [1.19, 1.16, 1.17, 1.12, 1.07]
            expected_data = {
                constants.TIME_INDEX: expected_dates,
                constants.STAGE_DAILY_MEAN: expected_values,
            }
            expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

            assert_frame_equal(result_df, expected_df)
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
                    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error")
                    return mock_response
            elif url.endswith("2022/"):
                # List only the test zip files
                mock_response.text = (
                    '<a href="codz_2022_01.zip">codz_2022_01.zip</a> <a href="codz_2022_02.zip">codz_2022_02.zip</a>'
                )
                mock_response.raise_for_status = MagicMock()
                return mock_response
            return MagicMock()  # Should not be called for other URLs

        mock_session.get.side_effect = mock_get_side_effect

        raw_data_list = self.fetcher._download_all_data(2022, 2022)
        self.assertEqual(len(raw_data_list), 2)  # Two zip files

        parsed_df = self.fetcher._parse_all_data(raw_data_list)
        self.assertFalse(parsed_df.empty)
        self.assertGreater(len(parsed_df), 20000)  # Expect many rows from two months of data
        self.assertIn(constants.GAUGE_ID, parsed_df.columns)
        self.assertIn(constants.TIME_INDEX, parsed_df.columns)
        self.assertIn(constants.DISCHARGE_DAILY_MEAN, parsed_df.columns)
        self.assertIn(constants.STAGE_DAILY_MEAN, parsed_df.columns)
        self.assertIn(constants.WATER_TEMPERATURE_DAILY_MEAN, parsed_df.columns)

        # Check date range
        self.assertEqual(parsed_df[constants.TIME_INDEX].min(), pd.to_datetime("2022-01-01"))
        self.assertEqual(parsed_df[constants.TIME_INDEX].max(), pd.to_datetime("2022-02-28"))


    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        with open(self.test_data_dir / "poland_metadata.csv", "rb") as f:
            mock_content = f.read()

        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        metadata_df = self.fetcher.get_metadata()

        expected_data = {
            constants.GAUGE_ID: ["152140010", "153140020", "153140030"],
            constants.STATION_NAME: ["BIELINEK", "WIDUCHOWA", "GRYFINO"],
            constants.RIVER: ["Odra (1)", "Odra (1)", "Odra (1)"],
            "Kod Hydro": ["00101", "00110", "00111"],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.GAUGE_ID)

        assert_frame_equal(metadata_df, expected_df)


if __name__ == "__main__":
    unittest.main()
