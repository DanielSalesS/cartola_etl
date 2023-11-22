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
    """
    Extracts data from fixed API endpoints.

    Iterates through a list of fixed_data_endpoints, attempts to extract data from each
    endpoint using FixedApiDataExtractor, and handles any exceptions that occur.

    Returns:
        None
    """
    for endpoint_name in fixed_data_endpoints:
        try:
            extractor = FixedApiDataExtractor(endpoint_name)
            extractor.execute()
        except Exception as e:
            print(f"Error extracting data from endpoint {endpoint_name}: {e}")


def extract_dynamic_api_data():
    """
    Extracts data from dynamic API endpoints.

    Iterates through a list of dynamic_data_endpoints, attempts to extract data from each 
    endpoint using DynamicApiDataExtractor, and handles any exceptions that occur.

    Returns:
        None
    """
    for endpoint_name in dynamic_data_endpoints:
        try:
            extractor = DynamicApiDataExtractor(endpoint_name)
            extractor.execute()
        except Exception as e:
            print(f"Error extracting data from endpoint {endpoint_name}: {e}")


def clubes_transform_load(round_number):
    """
    Transforms and loads data from the "Clubes" dimension for a specific round.

    The function retrieves data from the "Clubes" dimension, performs necessary 
    transformations, and loads the transformed data into the database.

    Args:
        round_number (int): The round number for which the club data is to be transformed and loaded.

    Returns:
        None
    """
    transform = ClubesTransform(round_number)
    transformed_data = transform.transform_data()
    ClubesLoad().load_data(transformed_data)


def round_transform_load_pipeline(round_number):
    """
    Executes a pipeline to transform and load data for specific dimensions and a fact 
    table.

    This function executes a series of transformation and loading operations for data
    related to specific dimensions 'Rodada' and 'MembrosEquipes', and a fact table 
    'Pontuacoes' corresponding to a given round number. The function takes the round 
    number as input and performs the following steps:

    1. Transforms the 'Rodada' data using the 'RodadaTransform' class.
    2. Loads the transformed 'Rodada' data into the database using the 'RodadaLoad' class.
    3. Transforms the 'MembrosEquipes' data using the 'MembrosEquipesTransform' class.
    4. Loads the transformed 'MembrosEquipes' data into the database using the 'MembrosEquipesLoad' class.
    5. Transforms the 'Pontuacoes' data using the 'PontuacoesTransform' class.
    6. Loads the transformed 'Pontuacoes' data into the database using the 'PontuacoesLoad' class.

    Args:
        round_number (int): The round number for which the data is to be transformed and loaded.

    Returns:
        None
    """
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
    """
    Identifies the rounds for which data is stored in the staging area.

    Retrieves the path to the staging area and iterates over its contents. For each 
    folder in the staging area, it checks whether the folder represents a round and 
    extracts the round number. It then appends the extracted round number to a list 
    and returns the sorted list.

    Returns:
        A list of round numbers for which data is stored in the staging area.
    """
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
