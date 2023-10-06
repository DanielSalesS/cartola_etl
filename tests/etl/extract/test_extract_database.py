import unittest
from unittest.mock import patch
from cartola_etl.database.connection import ConnectionManager
from cartola_etl.etl.extract.database import ScoutsDatabaseExtractor


class TestScoutsDatabaseExtractor(unittest.TestCase):
    @patch.object(ConnectionManager, "connect")
    def test_extract_data(self, mock_connect):
        # Config
        expected_data = [
            (1255948, 10, 5, 2, 3, 1, 0, 4, 2, 5, 1, 8, 3, 2, 1, 6, 2, 1, 0, 1, 1)
        ]

        query = """
            SELECT 
                membro_equipe_id, 
                SUM(COALESCE(desarmes, 0)) AS total_desarmes,
                SUM(COALESCE(falta_cometida, 0)) AS total_falta_cometida,
                SUM(COALESCE(gol_contra, 0)) AS total_gol_contra,
                SUM(COALESCE(cartao_amarelo, 0)) AS total_cartao_amarelo,
                SUM(COALESCE(cartao_vermelho, 0)) AS total_cartao_vermelho,
                SUM(COALESCE(jogo_sem_sofrer_gol, 0)) AS total_jogo_sem_sofrer_gol,
                SUM(COALESCE(defesa_dificil, 0)) AS total_defesa_dificil,
                SUM(COALESCE(defesa_penalti, 0)) AS total_defesa_penalti,
                SUM(COALESCE(gol_sofrido, 0)) AS total_gol_sofrido,
                SUM(COALESCE(penalti_cometido, 0)) AS total_penalti_cometido,
                SUM(COALESCE(falta_sofrida, 0)) AS total_falta_sofrida,
                SUM(COALESCE(assistencia, 0)) AS total_assistencia,
                SUM(COALESCE(finalizacao_trave, 0)) AS total_finalizacao_trave,
                SUM(COALESCE(finalizacao_defendida, 0)) AS total_finalizacao_defendida,
                SUM(COALESCE(finalizacao_fora, 0)) AS total_finalizacao_fora,
                SUM(COALESCE(gols, 0)) AS total_gols,
                SUM(COALESCE(impedimento, 0)) AS total_impedimento,
                SUM(COALESCE(penalti_perdido, 0)) AS total_penalti_perdido,
                SUM(COALESCE(penalti_sofrido, 0)) AS total_penalti_sofrido,
                SUM(COALESCE(vitoria_ponto_extra, 0)) AS total_vitoria_ponto_extra
            FROM fact_pontuacoes
            GROUP BY membro_equipe_id;
        """

        mock_connection = mock_connect.return_value.__enter__.return_value
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_connection.cursor.return_value.__enter__.return_value.fetchall.return_value = (
            expected_data
        )

        # Run
        extractor = ScoutsDatabaseExtractor()
        result_data = extractor.extract_data()

        # Check
        mock_connection.cursor.assert_called_once_with()
        mock_cursor.execute.assert_called_once_with(query)
        mock_cursor.fetchall.assert_called_once_with()
        self.assertEqual(result_data, expected_data)


if __name__ == "__main__":
    unittest.main()
