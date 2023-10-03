import json
import shutil
import unittest
from unittest.mock import patch
from tests.utils_test.utils import get_test_staging_area_path
from cartola_etl.etl.transform.clubes import ClubesTransform

partidas_test_data = {
    "clubes": {
        "3": {"id": 3, "nome": "name3", "abreviacao": "abbr3", "outros": "outros3"},
        "4": {"id": 4, "nome": "name4", "abreviacao": "abbr4", "outros": "outros4"},
    }
}

clubes_test_data = {
    "1": {"id": 1, "nome": "nome1", "abreviacao": "abbr1", "outros": "outros1"},
    "2": {"id": 2, "nome": "nome2", "abreviacao": "abbr2", "outros": "outros2"},
    "3": {"id": 3, "nome": "nome3", "abreviacao": "abbr3", "outros": "outros3"},
    "4": {"id": 4, "nome": "nome4", "abreviacao": "abbr4", "outros": "outros4"},
}

clubes_special_values = [
    {"id": -1, "nome": "nome1", "abreviacao": "abbr1"},
    {"id": -2, "nome": "nome2", "abreviacao": "abbr2"},
]


def save_test_data(data, path):
    with open(path, "w") as file:
        json.dump(data, file)


class TestClubesTransform(unittest.TestCase):
    def setUp(self):
        self.round_number = 1
        round_number_str = str(self.round_number).zfill(2)

        self.staging_area_path = get_test_staging_area_path()

        folder_name = f"rodada{round_number_str}"
        path = self.staging_area_path.joinpath(folder_name)
        path.mkdir(parents=True, exist_ok=True)
        filename = "partidas.json"
        self.file_path = path.joinpath(filename)
        save_test_data(partidas_test_data, self.file_path)

        folder_name = "fixed_data"
        path = self.staging_area_path.joinpath(folder_name)
        path.mkdir(parents=True, exist_ok=True)
        filename = "clubes.json"
        clubes_test_path = path.joinpath(filename)
        save_test_data(clubes_test_data, clubes_test_path)

    def tearDown(self):
        shutil.rmtree(self.staging_area_path)

    def test_load_data(self):
        # Config
        expected_data = partidas_test_data

        # Run
        transformer = ClubesTransform(self.round_number)
        result_data = transformer.load_data(self.file_path)

        # Check
        self.assertEqual(result_data, expected_data)

    @patch("cartola_etl.etl.transform.clubes.get_staging_area_path")
    @patch("cartola_etl.etl.transform.clubes.clubes_special_values",new=clubes_special_values)
    def test_transform_data(self, mock_staging_area_path):
        # Config
        expected_data = [
            (3, "name3", "abbr3"),
            (4, "name4", "abbr4"),
            (-1, "nome1", "abbr1"),
            (-2, "nome2", "abbr2"),
        ]

        test_staging_area_path = get_test_staging_area_path()
        mock_staging_area_path.return_value = test_staging_area_path

        # Run
        transformer = ClubesTransform(self.round_number)
        result_data = transformer.transform_data()

        # Check
        self.assertEqual(result_data, expected_data)


if __name__ == "__main__":
    unittest.main()
