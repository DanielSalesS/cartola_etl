from cartola_etl.etl.load.base_load import BaseLoad


class PontuacoesLoad(BaseLoad):
    def load_data(self, data):
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
