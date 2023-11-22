from cartola_etl.config.etl_config import INITIAL_ROUND
from cartola_etl.etl.pipelines.base_pipeline import (
    extract_fixed_api_data,
    extract_dynamic_api_data,
    find_rounds_stored_in_staging_area,
    clubes_transform_load,
    round_transform_load_pipeline,
)
from cartola_etl.utils.utils import get_current_round, get_last_round_in_database


def main():
    """
    Extracts, transforms, and loads data from Cartola FC APIs to the database.

    The pipeline fetches the latest dynamic data from the API, identifies data stored 
    in the staging area, and validates its consistency. For the initial data setup 
    (round 1), it retrieves fixed data and loads "Clubes" table data. Subsequently, it 
    processes the current round by transforming and loading data for the respective 
    dimensions and fact table.

    Returns:
        None
    """
    current_round = get_current_round()
    last_round_in_database = get_last_round_in_database()

    if current_round == last_round_in_database:
        raise Exception("The data from the current round has already been processed.")

    extract_dynamic_api_data()

    stored_rounds_in_staging_area = find_rounds_stored_in_staging_area()
    last_round_in_staging_area = stored_rounds_in_staging_area[-1]

    if last_round_in_staging_area - last_round_in_database != 1:
        raise Exception(
            "The difference between the last stored round and the last round in the "
            "database is not equal to 1."
        )

    if current_round == INITIAL_ROUND:
        extract_fixed_api_data()
        clubes_transform_load(round_number=current_round)

    round_transform_load_pipeline(round_number=current_round)
    print(f"Round {current_round} processed.")


if __name__ == "__main__":
    main()
