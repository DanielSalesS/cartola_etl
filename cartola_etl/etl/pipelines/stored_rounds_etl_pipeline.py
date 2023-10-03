from cartola_etl.config.etl_config import INITIAL_ROUND
from cartola_etl.etl.pipelines.base_pipeline import (
    clubes_transform_load,
    round_transform_load_pipeline,
    find_rounds_stored_in_staging_area,
)
from cartola_etl.utils.utils import get_last_round_in_database


def main():
    clubes_transform_load(round_number=INITIAL_ROUND)

    stored_rounds_in_staging_area = find_rounds_stored_in_staging_area()
    last_round_in_staging_area = stored_rounds_in_staging_area[-1]
    last_round_in_database = get_last_round_in_database()

    if last_round_in_staging_area == last_round_in_database:
        raise Exception("The data from the current round has already been processed.")

    expected_rounds = range(last_round_in_database + 1, last_round_in_staging_area + 1)
    missing_rounds = sorted(set(expected_rounds) - set(stored_rounds_in_staging_area))

    if missing_rounds:
        raise Exception(f"Some data from rounds {missing_rounds} were not stored.")

    for round_number in expected_rounds:
        round_transform_load_pipeline(round_number=round_number)
        print(f"Round {round_number} processed.")


if __name__ == "__main__":
    main()
