import unittest
from unittest.mock import patch
from cartola_etl.etl.load.base_load import BaseLoad
from cartola_etl.etl.load.membros_equipes import MembrosEquipesLoad


class TestMembrosClubesLoad(unittest.TestCase):
    @patch.object(BaseLoad, "load_multiple_data")
    def test_load_data(self, mock_load_multiple_data):
        # Config
        data = [(1, "name1", "Goleiro"), (2, "name2", "Lateral")]

        expected_query = """
            INSERT IGNORE INTO dim_membros_equipes (membro_equipe_id, nome_jogador, posicao)
            VALUES (%s, %s, %s)
        """

        # Run
        loader = MembrosEquipesLoad()
        loader.load_data(data)

        # Check
        mock_load_multiple_data.assert_called_once_with(data, expected_query)


if __name__ == "__main__":
    unittest.main()
