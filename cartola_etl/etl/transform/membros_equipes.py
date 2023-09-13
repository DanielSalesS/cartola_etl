import json
from cartola_etl.utils.utils import get_staging_area_path


class MembrosEquipesTransform:
    def __init__(self, round_number):
        self.round_number = round_number

    def load_data(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def transform_data(self):
        data = []

        round_number_str = str(self.round_number).zfill(2)

        staging_area_path = get_staging_area_path()

        folder_name = f"rodada{round_number_str}"
        filename = "atletas_mercado.json"
        source_path = staging_area_path.joinpath(folder_name, filename)

        source_data = self.load_data(source_path)

        posicoes = source_data["posicoes"]
        atletas = source_data["atletas"]

        for atleta in atletas:
            atleta_id = atleta["atleta_id"]
            nome = atleta["nome"]
            posicao = posicoes[str(atleta["posicao_id"])]["nome"]

            row = tuple([atleta_id, nome, posicao])
            data.append(row)

        return data
