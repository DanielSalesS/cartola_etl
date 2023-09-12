import unittest
from unittest.mock import patch
from cartola_etl.etl.load.base_load import BaseLoad
from cartola_etl.etl.load.clubes import ClubesLoad


class TestClubesLoad(unittest.TestCase):
    @patch.object(BaseLoad, "load_multiple_data")
    def test_load_data(self, mock_load_multiple_data):
        # Config
        data = [
            (1, "Flamengo", "FLA"),
            (2, "Corinthians", "COR"),
            (3, "São Paulo", "SAO"),
        ]

        expected_query = """
            INSERT IGNORE INTO dim_clubes (clube_id, nome_clube, abreviacao)
            VALUES (%s, %s, %s)
        """
        
        # Run
        loader = ClubesLoad()
        loader.load_data(data)

        # Check
        mock_load_multiple_data.assert_called_once_with(data, expected_query)


if __name__ == "__main__":
    unittest.main()
