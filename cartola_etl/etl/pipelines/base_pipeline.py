import os
from cartola_etl.utils import get_staging_area_path
from cartola_etl.config.etl_config import fixed_data_endpoints, dynamic_data_endpoints
from cartola_etl.etl.extract.api import DynamicApiDataExtractor, FixedApiDataExtractor
from cartola_etl.etl.load.clubes import ClubesLoad
from cartola_etl.etl.load.rodada import RodadaLoad
from cartola_etl.etl.load.membros_equipes import MembrosEquipesLoad
from cartola_etl.etl.load.pontuacoes import PontuacoesLoad
from cartola_etl.etl.transform.clubes import ClubesTransform
from cartola_etl.etl.transform.rodada import RodadaTransform
from cartola_etl.etl.transform.membros_equipes import MembrosEquipesTransform
from cartola_etl.etl.transform.pontuacoes import PontuacoesTransform


def extract_fixed_api_data():
    for endpoint_name in fixed_data_endpoints:
        try:
            extractor = FixedApiDataExtractor(endpoint_name)
            extractor.execute()
        except Exception as e:
            print(f"Error extracting data from endpoint {endpoint_name}: {e}")


def extract_dynamic_api_data():
    for endpoint_name in dynamic_data_endpoints:
        try:
            extractor = DynamicApiDataExtractor(endpoint_name)
            extractor.execute()
        except Exception as e:
            print(f"Error extracting data from endpoint {endpoint_name}: {e}")


def clubes_transform_load(round_number):
    transform = ClubesTransform(round_number)
    transformed_data = transform.transform_data()
    ClubesLoad().load_data(transformed_data)


def round_transform_load_pipeline(round_number):
    transform = RodadaTransform(round_number)
    transformed_data = transform.transform_data()
    RodadaLoad().load_data(transformed_data)

    transform = MembrosEquipesTransform(round_number)
    transformed_data = transform.transform_data()
    MembrosEquipesLoad().load_data(transformed_data)

    transform = PontuacoesTransform(round_number)
    transformed_data = transform.transform_data()
    PontuacoesLoad().load_data(transformed_data)


def find_rounds_stored_in_staging_area():
    rounds = []
    staging_area_path = get_staging_area_path()

    for folder_name in os.listdir(staging_area_path):
        folder_path = os.path.join(staging_area_path, folder_name)

        if os.path.isdir(folder_path) and os.listdir(folder_path):
            round_start_position = 6
            round_str = folder_name[round_start_position:]

            if round_str.isdigit():
                rounds.append(int(round_str))

    return sorted(rounds)
