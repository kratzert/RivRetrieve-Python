import os
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import PortugalFetcher, constants


class TestPortugalFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = PortugalFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def _load_mock_html(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r") as f:
            return f.read()

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_stage(self, mock_session):
        mock_response = MagicMock()
        mock_html = self._load_mock_html("portugal_19B01H_stage.html")
        mock_response.text = mock_html
        mock_response.raise_for_status = MagicMock()
        mock_session.return_value.get.return_value = mock_response

        gauge_id = "19B/01H"
        variable = constants.STAGE_DAILY_MEAN
        start_date = "2020-11-06"
        end_date = "2020-11-10"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["2020-11-06", "2020-11-07", "2020-11-08", "2020-11-09", "2020-11-10"])
        expected_values = [0.53, 0.46, 0.48, 0.40, 0.33]
        expected_df = pd.DataFrame(
            {constants.TIME_INDEX: expected_dates, constants.STAGE_DAILY_MEAN: expected_values}
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.return_value.get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
