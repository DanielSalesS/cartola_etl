from cartola_etl.etl.load.base_load import BaseLoad


class ClubesLoad(BaseLoad):
    def load_data(self, data):
        query = """
            INSERT IGNORE INTO dim_clube (clube_id, nome_clube, abreviacao)
            VALUES (%s, %s, %s)
        """

        self.load_multiple_data(data, query)
