import json
import numpy as np
import pandas as pd
from cartola_etl.config.etl_config import (
    INITIAL_ROUND,
    NO_CLUBE_ID,
    fact_table_selected_metrics,
    scouts_data,
)
from cartola_etl.etl.extract.database import ScoutsDatabaseExtractor
from cartola_etl.utils.utils import get_staging_area_path


class PontuacaoDataUtils:
    template = {
        "rodada_id": None,
        "clube_id": None,
        "membro_equipe_id": None,
        **{v["abreviacao"]: 0 for v in scouts_data.values()},
        **{k: 0 for k in fact_table_selected_metrics},
    }

    def create_empty_template(self):
        return self.template

    def get_columns_for_difference_calculation(self):
        return ["membro_equipe_id"] + list(
            {v["abreviacao"]: 0 for v in scouts_data.values()}
        )


class PontuacoesTransform:
    def __init__(self, round_number):
        self.round_number = round_number
        self.data_utils = PontuacaoDataUtils()
        self.sorted_columns = list(self.data_utils.template.keys())

    def load_data(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def get_json_source_data(self):
        staging_area_path = get_staging_area_path()

        round_number_str = str(self.round_number).zfill(2)
        folder_name = f"rodada{round_number_str}"
        filename = "atletas_mercado.json"
        source_path = staging_area_path.joinpath(folder_name, filename)

        return self.load_data(source_path)

    def extract_data_from_json_source(self, source_data):
        atletas = source_data["atletas"]
        data_template = self.data_utils.create_empty_template()
        data_template["rodada_id"] = self.round_number

        data = []
        for atleta in atletas:
            clube_id = atleta["clube_id"]

            atleta_scouts = {
                k: v for k, v in atleta["scout"].items() if k in data_template
            }

            merged_dict = {**data_template, **atleta_scouts}
            merged_dict["clube_id"] = clube_id
            merged_dict["membro_equipe_id"] = atleta["atleta_id"]

            for k, v in fact_table_selected_metrics.items():
                merged_dict[k] = atleta[v]

            row = tuple(merged_dict.values())
            data.append(row)

        return pd.DataFrame(data, columns=self.sorted_columns)

    def calculate_scouts_difference(self, current_data, accumulated_scouts):
        selected_scouts = current_data[accumulated_scouts.columns]

        common_index = selected_scouts.index.intersection(accumulated_scouts.index)
        selected_scouts_common = selected_scouts.loc[common_index]
        sum_scouts_common = accumulated_scouts.loc[common_index]

        difference = selected_scouts_common.sub(sum_scouts_common, fill_value=0)

        return difference

    def create_null_rows_for_missing_members(self, current_data, accumulated_scouts):
        missing_indices = accumulated_scouts.index.difference(current_data.index)
        null_rows = pd.DataFrame(index=missing_indices, columns=self.sorted_columns)
        null_rows["rodada_id"] = self.round_number
        null_rows["clube_id"] = NO_CLUBE_ID
        null_rows.drop("membro_equipe_id", axis=1, inplace=True)

        return null_rows

    def transform_data(self):
        source_data = self.get_json_source_data()
        current_data = self.extract_data_from_json_source(source_data)

        if self.round_number > INITIAL_ROUND:
            scouts_columns = self.data_utils.get_columns_for_difference_calculation()

            db_extractor = ScoutsDatabaseExtractor()
            scouts_extract = db_extractor.extract_data()
            accumulated_scouts = pd.DataFrame(scouts_extract, columns=scouts_columns)

            current_data.set_index("membro_equipe_id", inplace=True)
            accumulated_scouts.set_index("membro_equipe_id", inplace=True)

            scouts_difference = self.calculate_scouts_difference(
                current_data, accumulated_scouts
            )

            current_data.update(scouts_difference)

            null_rows = self.create_null_rows_for_missing_members(
                current_data, accumulated_scouts
            )

            current_data = pd.concat([current_data, null_rows])
            current_data.reset_index(inplace=True)
            current_data = current_data[self.sorted_columns]
            current_data.replace(np.nan, None, inplace=True)

        return list(current_data.itertuples(name=None, index=False))
