import os
import unittest
from unittest.mock import patch

import pandas as pd

from rivretrieve import JapanFetcher, constants


class TestJapanFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = JapanFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.gauge_id = "301011281104010"

    def load_sample_data(self, filename):
        file_path = os.path.join(self.test_data_dir, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            self.fail(f"Test data file not found: {file_path}")

    def mocked_download_data(self, gauge_id, variable, start_date, end_date):
        kind = self.fetcher._get_kind(variable)
        contents = []
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        if kind == 6:  # DISCHARGE_HOURLY_MEAN
            # Loop through each month in the range
            current_dt = start_dt.replace(day=1)
            while current_dt <= end_dt:
                year = current_dt.year
                month = current_dt.month
                if year == 2004 and month == 1:
                    contents.append(self.load_sample_data(f"japan_{self.gauge_id}_kind6_200401.dat"))
                elif year == 2004 and month == 2:
                    contents.append(self.load_sample_data(f"japan_{self.gauge_id}_kind6_200402.dat"))

                if current_dt.month == 12:
                    current_dt = current_dt.replace(year=year + 1, month=1)
                else:
                    current_dt = current_dt.replace(month=month + 1)
        elif kind == 7:  # DISCHARGE_DAILY_MEAN
            # Loop through each year in the range
            for year in range(start_dt.year, end_dt.year + 1):
                if year == 2004:
                    contents.append(self.load_sample_data(f"japan_{self.gauge_id}_kind7_2004.dat"))
                elif year == 2005:
                    contents.append(self.load_sample_data(f"japan_{self.gauge_id}_kind7_2005.dat"))
        return contents

    @patch("rivretrieve.japan.JapanFetcher._download_data")
    def test_get_data_hourly_discharge(self, mock_download):
        mock_download.side_effect = self.mocked_download_data

        variable = constants.DISCHARGE_HOURLY_MEAN
        start_date = "2004-01-30"
        end_date = "2004-02-02"

        result_df = self.fetcher.get_data(self.gauge_id, variable, start_date, end_date)
        self.assertFalse(result_df.empty)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        self.assertIn(variable, result_df.columns)

        # Check if dates are within the requested range
        self.assertTrue((result_df.index >= pd.to_datetime(start_date)).all())
        self.assertTrue((result_df.index <= pd.to_datetime(end_date) + pd.Timedelta(days=1)).all())

        # Check for expected number of hourly data points (3 days * 24 hours)
        self.assertTrue(len(result_df) > 24 * 3)
        self.assertTrue(len(result_df) <= 24 * 4)

    @patch("rivretrieve.japan.JapanFetcher._download_data")
    def test_get_data_daily_discharge(self, mock_download):
        mock_download.side_effect = self.mocked_download_data

        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2004-12-25"
        end_date = "2005-01-05"

        result_df = self.fetcher.get_data(self.gauge_id, variable, start_date, end_date)
        self.assertFalse(result_df.empty)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        self.assertIn(variable, result_df.columns)

        # Check if dates are within the requested range
        self.assertTrue((result_df.index >= pd.to_datetime(start_date)).all())
        self.assertTrue((result_df.index <= pd.to_datetime(end_date)).all())

        # Check for expected number of daily data points
        expected_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1
        self.assertEqual(len(result_df), expected_days)


if __name__ == "__main__":
    unittest.main()
