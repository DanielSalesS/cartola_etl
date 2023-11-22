import json
from cartola_etl.config.etl_config import INITIAL_ROUND, rodadas_initial_values
from cartola_etl.utils.utils import get_staging_area_path


class RodadaTransform:
    """
    Transforms source data into a structured dataset for the 'rodadas' dimension.

    The class provides methods to transform source data into a structured dataset
    for the 'rodadas' dimension.

    Attributes:
        round_number (int): The round number.
    """
    def __init__(self, round_number):
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

    def get_source_data_path(self, folder_name, filename):
        """
        Retrieves the path to a source data file within the staging area.

        The method constructs the full path to the specified source data file using the provided folder name and filename. It utilizes the `get_staging_area_path` function to determine the root directory of the staging area.

        Args:
            folder_name (str): The name of the folder containing the source data file.
            filename (str): The name of the source data file.

        Returns:
            Path: The full path to the source data file within the staging area.
        """
        staging_area_path = get_staging_area_path()
        source_path = staging_area_path.joinpath(folder_name, filename)
        return source_path

    def transform_data(self):
        """
        Transforms source data into a structured dataset for the 'rodada' dimension.

        Retrieves and processes round information from the source file within the 
        staging area for the current round. Extracts relevant attributes and compiles
        this information into a structured dataset tailored for the 'rodada' dimension.

        Returns:
            list: A list of tuples representing the transformed data.
        """
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
