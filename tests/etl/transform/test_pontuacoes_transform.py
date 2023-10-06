import json
import shutil
import unittest
from unittest.mock import patch
import pandas as pd
from tests.utils_test.utils import get_test_staging_area_path
from cartola_etl.etl.transform.pontuacoes import PontuacaoDataUtils, PontuacoesTransform


class PontuacoesDataBuilder:
    posicoes = {
        "1": {"id": 1, "nome": "Goleiro", "abreviacao": "gol"},
        "2": {"id": 2, "nome": "Lateral", "abreviacao": "lat"},
    }

    def __init__(self):
        self.data = {}

    def add_atleta(
        self,
        atleta_id,
        clube_id,
        nome,
        posicao_id,
        scout,
        preco_num,
        media_num,
        pontos_num,
    ):
        atleta = {
            "atleta_id": atleta_id,
            "clube_id": clube_id,
            "nome": nome,
            "posicao_id": posicao_id,
            "scout": scout,
            "preco_num": preco_num,
            "media_num": media_num,
            "pontos_num": pontos_num,
        }

        if "atletas" not in self.data:
            self.data["atletas"] = []

        self.data["atletas"].append(atleta)
        
        return self

    def build(self):
        self.data["posicoes"] = self.posicoes
        rodada_data = self.data
        return rodada_data


def get_test_source_data_path(folder_name, filename):
    staging_area_path = get_test_staging_area_path()
    folder_path = staging_area_path.joinpath(folder_name)
    folder_path.mkdir(parents=True, exist_ok=True)
    source_path = folder_path.joinpath(filename)
    return source_path


def create_test_atletas_files(round_number_str, rodada_data, return_path=False):
    folder_name = f"rodada{round_number_str}"

    filename = "atletas_mercado.json"
    atletas_source_path = get_test_source_data_path(folder_name, filename)

    with open(atletas_source_path, "w") as file:
        json.dump(rodada_data, file)

    if return_path:
        return atletas_source_path


class TestPontuacoesTransform(unittest.TestCase):
    def setUp(self):
        self.staging_area_path = get_test_staging_area_path()

    def tearDown(self):
        shutil.rmtree(self.staging_area_path)

    def test_load_json_source_data(self):
        # Config
        round_number = 1
        round_number_str = str(round_number).zfill(2)

        data_builder = PontuacoesDataBuilder()
        rodada_data = (
            data_builder
            .add_atleta(1, 10, "name1", 1, {"DS": 0, "FC": 0}, 15.2, 0, 0)
            .add_atleta(2, 20, "name2", 2, {"DS": 0, "FC": 0}, 20, 0, 0)
            .build()
        )

        expected_data = rodada_data

        file_path = create_test_atletas_files(round_number_str, rodada_data, return_path=True)

        # Run
        transformer = PontuacoesTransform(round_number)
        result_data = transformer.load_data(file_path)

        # Check
        self.assertEqual(result_data, expected_data)

    @patch("cartola_etl.etl.transform.pontuacoes.get_staging_area_path")
    def test_extract_data_from_json_source(self, mock_staging_area_path):
        # Config
        round_number = 1
        round_number_str = str(round_number).zfill(2)

        data_builder = PontuacoesDataBuilder()
        rodada_data = (
            data_builder
            .add_atleta(1, 10, "name1", 1, {"DS": 0, "FC": 0}, 15.2, 0, 0)
            .add_atleta(2, 20, "name2", 2, {"DS": 0, "FC": 0}, 20, 0, 0)
            .build()
        )

        columns = PontuacaoDataUtils.template.keys()
        expected_data = [
            (1, 10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            0, 15.2, 0, 0),

            (1, 20, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            0, 20, 0, 0)
        ]

        expected_data = pd.DataFrame(expected_data, columns=columns)

        create_test_atletas_files(round_number_str, rodada_data)

        source_data = rodada_data

        test_staging_area_path = self.staging_area_path
        mock_staging_area_path.return_value = test_staging_area_path

        # Run
        transformer = PontuacoesTransform(round_number)
        result_data = transformer.extract_data_from_json_source(source_data)

        # Check
        self.assertTrue(result_data.equals(expected_data))

    def test_calculate_scouts_difference(self):
        # Config
        round_number = 1
        current_data = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})

        accumulated_scouts = pd.DataFrame(
            {"A": [1, 2, 2], "B": [3, 4, 4], "C": [5, 6, 6]}
        )

        expected_difference = pd.DataFrame(
            {"A": [0, 0, 1], "B": [1, 1, 2], "C": [2, 2, 3]}
        )

        # Run
        transformer = PontuacoesTransform(round_number)
        result_difference = transformer.calculate_scouts_difference(
            current_data, accumulated_scouts
        )

        # Check
        self.assertTrue(expected_difference.equals(result_difference))

    @patch("cartola_etl.etl.transform.pontuacoes.PontuacaoDataUtils")
    def test_create_null_rows_for_missing_members(self, mock_data_utils):
        # Config
        round_number = 1

        current_data = pd.DataFrame(
            {
                "rodada_id": [1, 1, 1],
                "clube_id": [2, 2, 6],
                "membro_equipe_id": [1, 2, 3],
                "outros": [4, 5, 3],
            }
        )

        accumulated_scouts = pd.DataFrame(
            {"membro_equipe_id": [1, 2, 4], "outros": [3, 2, 3]}
        )

        expected_null_rows = pd.DataFrame(
            [[1, -1, 4, None]],
            columns=["rodada_id", "clube_id", "membro_equipe_id", "outros"],
        )

        current_data.set_index("membro_equipe_id", inplace=True)
        accumulated_scouts.set_index("membro_equipe_id", inplace=True)
        expected_null_rows.set_index("membro_equipe_id", inplace=True)

        mock_instance = mock_data_utils.return_value
        mock_instance.template = {
            "rodada_id": None,
            "clube_id": None,
            "membro_equipe_id": None,
            "outros": 0,
        }

        # Run
        transformer = PontuacoesTransform(round_number)
        result_null_rows = transformer.create_null_rows_for_missing_members(
                current_data, accumulated_scouts
        )
        
        # Check
        self.assertTrue(expected_null_rows.equals(result_null_rows))

    @patch("cartola_etl.etl.transform.pontuacoes.get_staging_area_path")
    def test_initial_round_transform_data(self, mock_staging_area_path):
        # Config
        round_number = 1
        round_number_str = str(round_number).zfill(2)

        data_builder = PontuacoesDataBuilder()
        rodada_data = (
            data_builder
            .add_atleta(1, 10, "name1", 1, {"DS": 0, "FC": 0}, 15.2, 0, 0)
            .add_atleta(2, 20, "name2", 2, {"DS": 0, "FC": 0}, 20, 0, 0)
            .build()
        )

        expected_data = [
            (1, 10, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            15.2, 0, 0),

            (1, 20, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            20, 0, 0)
        ]

        create_test_atletas_files(round_number_str, rodada_data)

        test_staging_area_path = self.staging_area_path
        mock_staging_area_path.return_value = test_staging_area_path

        # Run
        transformer = PontuacoesTransform(round_number)
        result_data = transformer.transform_data()

        # Check
        self.assertEqual(expected_data, result_data)

    @patch("cartola_etl.etl.transform.pontuacoes.ScoutsDatabaseExtractor")
    @patch("cartola_etl.etl.transform.pontuacoes.get_staging_area_path")
    def test_null_rows_transform_data(self, mock_staging_area_path, mock_scouts_database_extractor):
        # Config
        round_number = 2
        round_number_str = str(round_number).zfill(2)

        data_builder = PontuacoesDataBuilder()
        rodada_data = (
            data_builder
            .add_atleta(1, 10, "name1", 1, {"DS": 1, "FC": 0}, 16.2, 4.3, 4.3)
            .add_atleta(3, 22, "name3", 2, {"DS": 1, "FC": 3}, 20, 5.6, 5.6)
            .build()
        )

        extracted_data = [
            (1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            (2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ]

        expected_data = [
            (2, 10, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            16.2, 4.3, 4.3),

            (2, 22, 3, 1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            20, 5.6, 5.6),

            (2, -1, 2, None, None, None, None, None, None, None, None, None, None,\
            None, None, None, None, None, None, None, None,None, None, None, None, None),
           
        ]

        create_test_atletas_files(round_number_str, rodada_data)

        test_staging_area_path = self.staging_area_path
        mock_staging_area_path.return_value = test_staging_area_path

        mock_db_extractor_instance = mock_scouts_database_extractor.return_value
        mock_db_extractor_instance.extract_data.return_value = extracted_data

        # Run
        transformer = PontuacoesTransform(round_number)
        result_data = transformer.transform_data()

        # Check
        self.assertTrue(set(result_data).issuperset(set(expected_data)))

    @patch("cartola_etl.etl.transform.pontuacoes.ScoutsDatabaseExtractor")
    @patch("cartola_etl.etl.transform.pontuacoes.get_staging_area_path")
    def test_next_round_transform_data(self, mock_staging_area_path, mock_scouts_database_extractor):
        # Config
        round_number = 3
        round_number_str = str(round_number).zfill(2)

        data_builder = PontuacoesDataBuilder()
        rodada_data = (
            data_builder
            .add_atleta(1, 10, "name1", 1, {"DS": 3, "FC": 5}, 17.2, 5.3, 6.3)
            .add_atleta(3, 22, "name2", 2, {"DS": 4, "FC": 5}, 20, 7.1, 8.6)
            .build()
        )

        extracted_data = [
            (1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            (2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            (3, 1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ]

        expected_data = [
            (3, 10, 1, 2, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            17.2, 5.3, 6.3),

            (3, 22, 3, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            20, 7.1, 8.6),

            (3, -1, 2, None, None, None, None, None, None, None, None, None, None,\
            None, None, None, None, None, None, None, None, None, None, None, None, None),
           
        ]

        create_test_atletas_files(round_number_str, rodada_data)

        test_staging_area_path = self.staging_area_path
        mock_staging_area_path.return_value = test_staging_area_path

        mock_db_extractor_instance = mock_scouts_database_extractor.return_value
        mock_db_extractor_instance.extract_data.return_value = extracted_data

        # Run
        transformer = PontuacoesTransform(round_number)
        result_data = transformer.transform_data()

        # Check
        self.assertTrue(set(result_data).issuperset(set(expected_data)))


if __name__ == "__main__":
    unittest.main()
