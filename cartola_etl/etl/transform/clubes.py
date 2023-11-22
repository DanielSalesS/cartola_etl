import json
from cartola_etl.config.etl_config import clubes_special_values
from cartola_etl.utils.utils import get_staging_area_path


class ClubesTransform:
    """
    Transforms source data into a structured dataset for the 'clubes' dimension.

    The class provides methods to transform source data into a structured dataset
    for the 'clubes' dimension.

    Attributes:
        round_number (int): The round number.
    """
    def __init__(self, round_number):
        """
        Initializes an instance of the class.

        Args:
            round_number (int): The round number.

        Returns:
            None
        """
        self.round_number = round_number

    def load_data(self, path):
        """
        Loads JSON data from a specified file path.

        The method opens the file in read mode and utilizes the `json` module to parse 
        the JSON data into a Python object. The parsed JSON data is then returned to 
        the caller.

        Args:
            path (str): The path to the file containing the JSON data.

        Returns:
            dict: The parsed JSON data represented as a Python dictionary.
        """
        with open(path, "r") as f:
            return json.load(f)

    def transform_data(self):
        """
        Transforms source data into a structured dataset for the 'clubes' dimension.

        Retrieves and processes club information and processes it to create a dataset 
        tailored for the 'clubes' dimension. It extracts relevant attributes and 
        filters clubs based on validity criteria, ensuring data accuracy and 
        consistency. The transformed data is returned as a list of tuples, each 
        representing a club entry.

        Returns:
            list: A list of tuples representing the transformed data.
        """
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
