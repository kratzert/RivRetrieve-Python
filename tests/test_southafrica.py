import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from rivretrieve import SouthAfricaFetcher, constants


class TestSouthAfricaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SouthAfricaFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"
        # Assuming no data in sample files, so we mock the response text
        self.no_data_html = "<html><body><pre>No data for this period</pre></body></html>"

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_nodata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self.no_data_html
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        gauge_id = "X3H023"
        variable = constants.DISCHARGE
        start_date = "2022-01-01"
        end_date = "2022-01-05"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        self.assertTrue(result_df.empty)
        self.assertEqual(list(result_df.columns), [constants.TIME_INDEX, constants.DISCHARGE])
        mock_session.get.assert_called_once()

    # TODO: Add tests with actual data when sample is available


if __name__ == "__main__":
    unittest.main()
