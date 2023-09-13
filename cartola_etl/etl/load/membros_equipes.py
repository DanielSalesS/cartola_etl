from cartola_etl.etl.load.base_load import BaseLoad


class MembrosEquipesLoad(BaseLoad):
    def load_data(self, data):
        query = """
            INSERT IGNORE INTO dim_membros_equipes (membro_equipe_id, nome_jogador, posicao)
            VALUES (%s, %s, %s)
        """

        self.load_multiple_data(data, query)
