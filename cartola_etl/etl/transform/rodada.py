import json
from cartola_etl.config.etl_config import INITIAL_ROUND, rodadas_initial_values
from cartola_etl.utils.utils import get_staging_area_path


class RodadaTransform:
    def __init__(self, round_number):
        self.round_number = round_number

    def load_data(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def get_source_data_path(self, folder_name, filename):
        staging_area_path = get_staging_area_path()
        source_path = staging_area_path.joinpath(folder_name, filename)
        return source_path

    def transform_data(self):
        data = []
        round_number_str = str(self.round_number).zfill(2)

        folder_name = "fixed_data"
        filename = "rodadas.json"
        rodadas_source_data_path = self.get_source_data_path(folder_name, filename)
        rodadas_source_data = self.load_data(rodadas_source_data_path)

        folder_name = f"rodada{round_number_str}"

        filename = "mercado_status.json"
        status_source_path = self.get_source_data_path(folder_name, filename)
        status_source_data = self.load_data(status_source_path)

        filename = "pos_rodada_destaques.json"
        destaque_source_path = self.get_source_data_path(folder_name, filename)
        destaque_source_data = self.load_data(destaque_source_path)

        inicio, fim = None, None
        for rodada in rodadas_source_data:
            if rodada["rodada_id"] == self.round_number:
                inicio = rodada["inicio"]
                fim = rodada["fim"]
                break

        times_escalados = status_source_data["times_escalados"]

        if self.round_number == INITIAL_ROUND:
            media_cartoletas = rodadas_initial_values["cartoletas"]
            media_pontos_destaque = rodadas_initial_values["pontos"]
        else:
            media_cartoletas = destaque_source_data["media_cartoletas"]
            media_pontos_destaque = destaque_source_data["media_pontos"]

        data = tuple(
            [
                self.round_number,
                inicio,
                fim,
                times_escalados,
                media_cartoletas,
                media_pontos_destaque,
            ]
        )

        return data
