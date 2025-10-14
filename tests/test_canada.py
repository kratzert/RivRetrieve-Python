import os
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import CanadaFetcher, constants


class TestCanadaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = CanadaFetcher()
        self.test_db_path = Path(os.path.dirname(__file__)) / "test_data" / "test_hydat.sqlite3"

    @patch("rivretrieve.utils.requests_retry_session")
    @patch("rivretrieve.canada.CanadaFetcher._download_hydat")
    @patch(
        "rivretrieve.canada.CanadaFetcher.HYDAT_PATH",
        new_callable=lambda: Path(os.path.join(os.path.dirname(__file__), "test_data", "test_hydat.sqlite3")),
    )
    def test_get_data_discharge(self, mock_hydat_path, mock_download, mock_requests):
        mock_download.return_value = True  # Prevent download attempt

        gauge_id = "08GA031"
        variable = constants.DISCHARGE
        start_date = "2010-01-01"
        end_date = "2010-01-05"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(
                ["2010-01-01", "2010-01-02", "2010-01-03", "2010-01-04", "2010-01-05"]
            ),
            constants.DISCHARGE: [1.1, 1.2, 1.3, 1.4, 1.5],
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    @patch("rivretrieve.canada.CanadaFetcher._download_hydat")
    @patch(
        "rivretrieve.canada.CanadaFetcher.HYDAT_PATH",
        new_callable=lambda: Path(os.path.join(os.path.dirname(__file__), "test_data", "test_hydat.sqlite3")),
    )
    def test_get_data_stage(self, mock_hydat_path, mock_download, mock_requests):
        mock_download.return_value = True  # Prevent download attempt

        gauge_id = "08GA031"
        variable = constants.STAGE
        start_date = "2010-01-01"
        end_date = "2010-01-05"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(
                ["2010-01-01", "2010-01-02", "2010-01-03", "2010-01-04", "2010-01-05"]
            ),
            constants.STAGE: [10.1, 10.2, 10.3, 10.4, 10.5],
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df, expected_df)


if __name__ == "__main__":
    unittest.main()
