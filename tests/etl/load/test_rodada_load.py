import unittest
from unittest.mock import patch
from cartola_etl.etl.load.base_load import BaseLoad
from cartola_etl.etl.load.rodada import RodadaLoad


class TestRodadaTransform(unittest.TestCase):
    @patch.object(BaseLoad, "load_single_data")
    def test_load_data(self, mock_load_single_data):
        # Config
        data = (1, "2023-04-15 16:00:00", "2023-04-16 18:30:00", 1514144, 100, 0)

        expected_query = """
            INSERT IGNORE INTO dim_rodadas (rodada_id, inicio, fim, times_escalados, media_cartoletas, media_pontos)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        # Run
        loader = RodadaLoad()
        loader.load_data(data)

        # Check
        mock_load_single_data.assert_called_once_with(data, expected_query)


if __name__ == "__main__":
    unittest.main()
