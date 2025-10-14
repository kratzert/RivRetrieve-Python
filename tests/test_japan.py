import os
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import JapanFetcher, constants


class TestJapanFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = JapanFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def load_sample_data(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r", encoding="utf-8") as f:
            return f.read()

    @patch("requests.Session.get")
    def test_get_data_discharge(self, mock_get):
        sample_html = self.load_sample_data("japan_daily.html")

        mock_response = MagicMock()
        mock_response.text = sample_html
        mock_response.encoding = "shift_jis"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        gauge_id = "301011281104010"
        variable = constants.DISCHARGE
        start_date = "2019-01-13"
        end_date = "2019-01-17"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["2019-01-13", "2019-01-14", "2019-01-15", "2019-01-16", "2019-01-17"])
        expected_values = [2.09, 1.78, 1.78, 2.09, 1.78]
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.DISCHARGE: expected_values,
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df.reset_index(drop=True), expected_df)
        mock_get.assert_called_once()
        # Check that the params are correct
        mock_args, mock_kwargs = mock_get.call_args
        self.assertEqual(mock_kwargs["params"]["KIND"], 6)
        self.assertEqual(mock_kwargs["params"]["ID"], gauge_id)
        self.assertEqual(mock_kwargs["params"]["BGNDATE"], "20190101")
        self.assertEqual(mock_kwargs["params"]["ENDDATE"], "20190117")


if __name__ == "__main__":
    unittest.main()
