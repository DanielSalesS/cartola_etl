from cartola_etl.etl.load.base_load import BaseLoad


class PontuacoesLoad(BaseLoad):
    """
    Loads data into the 'fact_pontuacoes' table.

    This class provides a common interface for loading data into the 
    'fact_pontuacoes' table.

    Attributes:
        conn_manager (ConnectionManager): The connection manager object.
    """
    def load_data(self, data):
        """
        Loads data into the 'fact_pontuacoes' table.

        Executes a SQL query to insert data into the 'fact_pontuacoes' table.

        Args:
            data (list): A list of tuples containing the data to be loaded into the
            'fact_pontuacoes' table.
        
        Returns:
            None
        """
        query = """
            INSERT INTO fact_pontuacoes (
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

        self.load_multiple_data(data, query)
