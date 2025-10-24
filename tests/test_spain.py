import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import SpainFetcher, constants


class TestSpainFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SpainFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"
        self.sample_metadata_zip = self.test_data_dir / "spain-listado-estaciones-aforo-sample.zip"
        self.sample_data_html = self.test_data_dir / "spain_sample_data.html"

    def load_sample_html(self):
        with open(self.sample_data_html, "r", encoding="utf-8") as f:
            return f.read()

    def load_sample_zip_content(self):
        with open(self.sample_metadata_zip, "rb") as f:
            return f.read()

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_zip_response = MagicMock()
        mock_zip_response.content = self.load_sample_zip_content()
        mock_zip_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_zip_response

        result_df = self.fetcher.get_metadata()

        expected_data = {
            constants.GAUGE_ID: ["1010", "1011"],
            constants.STATION_NAME: ["PUENTE QUEROL", "RUAPETÍN"],
            constants.RIVER: ["Sil", "Sil"],
            "REGIMEN_RIO": [np.nan, np.nan],
            "COD_SAIH": [np.nan, np.nan],
            "COD_DMA": [np.nan, np.nan],
            "COD_MASA_AGUA": ["ES010MSPFES414MAR000770", "ES010MSPFES436MAR001180"],
            "FOTOGRAFIA": ["N.D.jpg", "N.D.jpg"],
            "PLANO": ["N.D.jpg", "N.D.jpg"],
            "SECCION": ["N.D.jpg", "N.D.jpg"],
            "ORGANISMO_CUENCA_VISOR": ["C.H. MIÑO-SIL", "C.H. MIÑO-SIL"],
            "COD_SITUACION_ESTACION": [4, 4],
            "SITUACION_ESTACION": ["RÍO", "RÍO"],
            "ESTADO": ["BAJA", "BAJA"],
            "ANO_INICIO_MEDIDAS": [1913.0, 1912.0],
            "ANO_FIN_MEDIDAS": [1959.0, 1940.0],
            "COD_SAICA": [np.nan, np.nan],
            "COORD_UTMX_H30_ETRS89": [204647, 160356],
            "COORD_UTMY_H30_ETRS89": [4716265, 4701177],
            constants.ALTITUDE: [490.0, 282.0],
            "CUENCA_RECEP": [833, 6346],
            constants.AREA: [7983.0, 7983.0],
            "NUM_CUENCA": [1414.0, 1436.0],
            "HOJA_1_50000": ["PONFERRADA (158)", "BARCO DE VALDEORRAS (190)"],
            "SISTEMA_EXPLO": [np.nan, "SIL INFERIOR"],
            "ESCALA_RC": [np.nan, np.nan],
            "LONG_RC": [np.nan, np.nan],
            "ANCH_RC": [np.nan, np.nan],
            "CASETA_RC": [np.nan, np.nan],
            "PASARELA_RC": [np.nan, np.nan],
            "PROPIETARIO": ["ESTADO", np.nan],
            "SENSOR_1": [np.nan, np.nan],
            "TIPO_CASETA_RC": [np.nan, np.nan],
            "TIPO_ESC_RC": [np.nan, np.nan],
            "TIPO_ESTACION": [np.nan, np.nan],
            "TRANSM_1": [np.nan, np.nan],
            "VERTEDERO_RC": [np.nan, np.nan],
            "TIPO_VERT_RC": [np.nan, np.nan],
            "TERMINO_MUNICIPAL": ["Ponferrada", "Rua"],
            "PROVINCIA": ["León", "Orense"],
            constants.COUNTRY: ["Spain", "Spain"],
            constants.SOURCE: ["ROAN", "ROAN"],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.GAUGE_ID)

        # Convert columns to numeric where appropriate, matching get_metadata types
        expected_numeric_columns = [
            constants.ALTITUDE,
            constants.AREA,
            "ANO_INICIO_MEDIDAS",
            "ANO_FIN_MEDIDAS",
            "CUENCA_RECEP",
            "NUM_CUENCA",
        ]
        for col in expected_numeric_columns:
            if col in expected_df.columns:
                expected_df[col] = pd.to_numeric(expected_df[col], errors="coerce")

        assert_frame_equal(result_df, expected_df, check_dtype=False, check_like=True)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self.load_sample_html()
        mock_response.apparent_encoding = "utf-8"
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        gauge_id = "1080"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2021-01-01"
        end_date = "2021-01-02"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["2021-01-01", "2021-01-02"])
        expected_values = [1.56, 1.57]  # 156/100, 157/100
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.DISCHARGE_DAILY_MEAN: expected_values,
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        mock_session.get.assert_called_once()
        mock_args, mock_kwargs = mock_session.get.call_args
        self.assertIn(f"valores={gauge_id}|2021|2021", mock_args[0])


if __name__ == "__main__":
    unittest.main()
