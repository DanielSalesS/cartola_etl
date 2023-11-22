import json
from cartola_etl.utils.utils import get_staging_area_path


class MembrosEquipesTransform:
    """
    Transforms source data into a structured dataset for the 'membros_equipes' 
    dimension.

    The class provides methods to transform source data into a structured dataset
    for the 'membros_equipes' dimension.

    Attributes:
        round_number (int): The round number.
    """
    def __init__(self, round_number):
        """
        Initializes a new instance of the class.

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
        Transforms source data into a structured dataset for the 'membros_equipes' 
        dimension.

        Retrieves and processes team members' data from the source file within the 
        staging area for the current round. Extracts relevant attributes and compiles
        this information into a structured dataset tailored for the 'membros_equipes' 
        dimension.

        Returns:
            list: A list of tuples representing the transformed data.
        """
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
