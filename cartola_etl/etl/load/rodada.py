from cartola_etl.etl.load.base_load import BaseLoad


class RodadaLoad(BaseLoad):
    def load_data(self, data):
        query = """
            INSERT IGNORE INTO dim_rodada (rodada_id, inicio, fim, times_escalados, media_cartoletas, media_pontos)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        self.load_single_data(data, query)
