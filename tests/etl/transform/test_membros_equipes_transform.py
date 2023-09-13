import json
import shutil
import unittest
from unittest.mock import patch
from tests.utils_test.utils import get_test_staging_area_path
from cartola_etl.etl.transform.membros_equipes import MembrosEquipesTransform

test_data = {
    "posicoes": {
        "1": {"id": 1, "nome": "Goleiro", "abreviacao": "gol"},
        "2": {"id": 2, "nome": "Lateral", "abreviacao": "lat"},
    },
    "atletas": [
        {"atleta_id": 1, "nome": "name1", "posicao_id": 1},
        {"atleta_id": 2, "nome": "name2", "posicao_id": 2},
    ],
}


class TestMembrosEquipesTransform(unittest.TestCase):
    def setUp(self):
        self.round_number = 1
        round_number_str = str(self.round_number).zfill(2)

        self.staging_area_path = get_test_staging_area_path()
        folder_name = f"rodada{round_number_str}"
        path = self.staging_area_path.joinpath(folder_name)
        path.mkdir(parents=True, exist_ok=True)
        filename = "atletas_mercado.json"
        self.file_path = path.joinpath(filename)

        with open(self.file_path, "w") as file:
            json.dump(test_data, file)

    def tearDown(self):
        shutil.rmtree(self.staging_area_path)

    def test_load_data(self):
        # Config
        expected_data = test_data

        # Run
        transformer = MembrosEquipesTransform(self.round_number)
        result_data = transformer.load_data(self.file_path)

        # Check
        self.assertEqual(result_data, expected_data)

    @patch("cartola_etl.etl.transform.membros_equipes.get_staging_area_path")
    def test_transform_data(self, mock_staging_area_path):
        # Config
        expected_data = [(1, "name1", "Goleiro"), (2, "name2", "Lateral")]

        test_staging_area_path = get_test_staging_area_path()
        mock_staging_area_path.return_value = test_staging_area_path

        # Run
        transformer = MembrosEquipesTransform(self.round_number)
        result_data = transformer.transform_data()

        # Check
        self.assertEqual(result_data, expected_data)


if __name__ == "__main__":
    unittest.main()
