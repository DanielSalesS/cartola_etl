import unittest
from unittest.mock import patch
from cartola_etl.etl.load.base_load import BaseLoad
from cartola_etl.etl.load.pontuacao import PontuacaoLoad


class TestPontuacaoLoad(unittest.TestCase):
    @patch.object(BaseLoad, "load_multiple_data")
    def test_load_data(self, mock_load_multiple_data):
        # Config
        data = [
            (3, 10, 1, 2, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            17.2, 5.3, 6.3),

            (3, 22, 3, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
            20, 7.1, 8.6),

            (3, -1, 2, None, None, None, None, None, None, None, None, None, None,\
            None, None, None, None, None, None, None, None, None, None, None, None, None),
           
        ]

        expected_query = """
            INSERT INTO fact_pontuacao (
                rodada_id, clube_id, membro_equipe_id, desarmes, falta_cometida, gol_contra,
                cartao_amarelo, cartao_vermelho, jogo_sem_sofrer_gol, defesa_dificil, defesa_penalti,
                gol_sofrido, penalti_cometido, falta_sofrida, assistencia,finalizacao_trave,
                finalizacao_defendida, finalizacao_fora, gols, impedimento, penalti_perdido,
                penalti_sofrido, vitoria_ponto_extra, preco, media, pontuacao_rodada
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s
            )
        """

        # Run
        loader = PontuacaoLoad()
        loader.load_data(data)

        # Check
        mock_load_multiple_data.assert_called_once_with(data, expected_query)


if __name__ == "__main__":
    unittest.main()
