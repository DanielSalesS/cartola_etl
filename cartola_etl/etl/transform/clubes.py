import json
from cartola_etl.config.etl_config import clubes_special_values
from cartola_etl.utils.utils import get_staging_area_path


class ClubesTransform:
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
        filename = "partidas.json"
        partidas_source_path = staging_area_path.joinpath(folder_name, filename)
        partidas_source_data = self.load_data(partidas_source_path)

        folder_name = "fixed_data"
        filename = "clubes.json"
        clubes_source_path = staging_area_path.joinpath(folder_name, filename)
        clubes_source_data = self.load_data(clubes_source_path)

        valids_ids = tuple(int(_id) for _id in clubes_source_data.keys())

        selected_attributes = ["id", "nome", "abreviacao"]
        for club in partidas_source_data["clubes"].values():
            if club["id"] not in valids_ids:
                continue

            row = tuple([club[attr] for attr in selected_attributes])
            data.append(row)

        for club in clubes_special_values:
            data.append(tuple([club["id"], club["nome"], club["abreviacao"]]))

        return data
