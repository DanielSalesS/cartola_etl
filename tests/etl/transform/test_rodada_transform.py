import json
import shutil
import unittest
from unittest.mock import patch
from tests.utils_test.utils import get_test_staging_area_path
from cartola_etl.etl.transform.rodada import RodadaTransform

rodada_test_data = [
    {
        "inicio": "2023-04-15 16:00:00",
        "fim": "2023-04-16 18:30:00",
        "nome_rodada": "Rodada 1",
        "rodada_id": 1,
        "outros": "outros",
    },
    {
        "inicio": "2023-04-22 16:00:00",
        "fim": "2023-04-24 20:00:00",
        "nome_rodada": "Rodada 2",
        "rodada_id": 2,
        "outros": "outros",
    },
]

destaques_test_data = {
    "rodada01": {"mensagem": "Error"},
    "rodada02": {"media_cartoletas": 88.0, "media_pontos": 50.0, "outros": "outros"},
}

status_test_data = {
    "rodada01": {"times_escalados": 1514144, "outros": "outros"},
    "rodada02": {"times_escalados": 1798498, "outros": "outros"},
}


def get_test_source_data_path(folder_name, filename):
    staging_area_path = get_test_staging_area_path()
    folder_path = staging_area_path.joinpath(folder_name)
    folder_path.mkdir(parents=True, exist_ok=True)
    source_path = folder_path.joinpath(filename)
    return source_path


def save_data(file_path, data):
    with open(file_path, "w") as file:
        json.dump(data, file)


def test_config_factory(round_number_str):
    folder_name = f"rodada{round_number_str}"
    key_round = f"rodada{round_number_str}"

    filename = "mercado_status.json"
    status_source_path = get_test_source_data_path(folder_name, filename)
    save_data(status_source_path, status_test_data[key_round])

    filename = "pos_rodada_destaques.json"
    destaque_source_path = get_test_source_data_path(folder_name, filename)
    save_data(destaque_source_path, destaques_test_data[key_round])


class TestRodadaTransform(unittest.TestCase):
    def setUp(self):
        self.staging_area_path = get_test_staging_area_path()

        folder_name = "fixed_data"
        filename = "rodadas.json"
        self.rodadas_source_data_path = get_test_source_data_path(folder_name, filename)
        save_data(self.rodadas_source_data_path, rodada_test_data)

    def tearDown(self):
        shutil.rmtree(self.staging_area_path)

    def test_load_data(self):
        # Config
        round_number = 1

        expected_data = rodada_test_data

        # Run
        transformer = RodadaTransform(round_number)
        result_data = transformer.load_data(self.rodadas_source_data_path)

        # Check
        self.assertEqual(result_data, expected_data)

    @patch("cartola_etl.etl.transform.rodada.get_staging_area_path")
    def test_first_round_transform_data(self, mock_staging_area_path):
        # Config
        round_number = 1
        round_number_str = str(round_number).zfill(2)

        expected_data = (
            1,
            "2023-04-15 16:00:00",
            "2023-04-16 18:30:00",
            1514144,
            100,
            0,
        )

        test_config_factory(round_number_str)

        test_staging_area_path = get_test_staging_area_path()
        mock_staging_area_path.return_value = test_staging_area_path

        # Run
        transformer = RodadaTransform(round_number)
        result_data = transformer.transform_data()

        # Check
        self.assertEqual(result_data, expected_data)

    @patch("cartola_etl.etl.transform.rodada.get_staging_area_path")
    def test_second_round_transform_data(self, mock_staging_area_path):
        # Config
        round_number = 2
        round_number_str = str(round_number).zfill(2)

        expected_data = (
            2,
            "2023-04-22 16:00:00",
            "2023-04-24 20:00:00",
            1798498,
            88.0,
            50.0,
        )

        test_config_factory(round_number_str)

        test_staging_area_path = get_test_staging_area_path()
        mock_staging_area_path.return_value = test_staging_area_path

        # Run
        transformer = RodadaTransform(round_number)
        result_data = transformer.transform_data()

        # Check
        self.assertEqual(result_data, expected_data)


if __name__ == "__main__":
    unittest.main()
