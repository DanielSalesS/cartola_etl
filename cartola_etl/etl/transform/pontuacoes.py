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
    """
    Provides methods for transforming and manipulating pontuacao data.

    The class provides methods for transforming and manipulating pontuacao data
    in different formats.

    Attributes:
        round_number (int): The round number.
        template (dict): A template structure for pontuacao data.
    """
    template = {
        "rodada_id": None,
        "clube_id": None,
        "membro_equipe_id": None,
        **{v["abreviacao"]: 0 for v in scouts_data.values()},
        **{k: 0 for k in fact_table_selected_metrics},
    }

    def create_empty_template(self):
        """
        Creates and returns an empty template for scoring data.

        Generates and returns an empty template based on predefined keys and 
        default values to structure scoring data.
        
        Returns:
            dict: An empty template structure for scoring data.
        """
        return self.template

    def get_columns_for_difference_calculation(self):
        """
        Returns columns required for difference calculation.

        Retrieves and returns the list of columns necessary for calculating 
        score differences. This includes keys for different scoring metrics 
        to facilitate the calculation process.
        
        Returns:
            list: A list of columns necessary for difference calculation.
        """
        return ["membro_equipe_id"] + list(
            {v["abreviacao"]: 0 for v in scouts_data.values()}
        )


class PontuacoesTransform:
    """
    Transforms source data into a structured dataset for the 'pontuacoes' fact table.

    The class provides methods to transform source data into a structured dataset
    for the 'pontuacoes' fact table.
    
    Attributes:
        round_number (int): The round number.
        data_utils (PontuacaoDataUtils): A utility class for transforming and 
            manipulating score data.
        sorted_columns (list): A list of columns in the order they should be sorted.
        
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
        self.data_utils = PontuacaoDataUtils()
        self.sorted_columns = list(self.data_utils.template.keys())

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

    def get_json_source_data(self):
        """
        Retrieves team member data from the staging area.

        This method constructs the path to the JSON file containing team member data 
        for the specified round, retrieves the data using the `load_data` method, and 
        returns the retrieved data.

        Returns:
            dict: JSON source data for processing.
        """
        staging_area_path = get_staging_area_path()

        round_number_str = str(self.round_number).zfill(2)
        folder_name = f"rodada{round_number_str}"
        filename = "atletas_mercado.json"
        source_path = staging_area_path.joinpath(folder_name, filename)

        return self.load_data(source_path)

    def extract_data_from_json_source(self, source_data):
        """
        Extracts relevant data from the JSON source.

        Iterates through team members in the source data, extracts scout 
        information, and retrieves specific metrics defined in 'fact_table_selected_metrics'. 
        This extracted data is organized into rows and compiled into a Pandas DataFrame.

        Args:
            source_data (dict): The JSON source data to be processed.

        Returns:
            pd.DataFrame: Processed data in a DataFrame format.
        """
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

    def get_invalid_scouts_indices(self, current_scouts, accumulated_scouts):
        """
        Finds indices of invalid scouts based on current and accumulated scout data.

        Compares the current scout data with the accumulated scout data to identify 
        indices where the difference between the two is negative or where the current 
        scout data itself is negative. This method helps identify and retrieve indices 
        indicating invalid or contradictory scout information.

        Args:
            current_scouts (pd.DataFrame): Current scout data.
            accumulated_scouts (pd.DataFrame): Accumulated scout data.

        Returns:
            pd.Index: Indices of invalid scout data.
        """
        common_indices = current_scouts.index.intersection(accumulated_scouts.index)

        current_scouts_filtered = current_scouts.loc[common_indices]
        accumulated_scouts_filtered = accumulated_scouts.loc[common_indices]

        scouts_difference = current_scouts_filtered.sub(accumulated_scouts_filtered)

        negative_difference_indices = scouts_difference.loc[
            (scouts_difference < 0).any(axis=1)
        ].index

        negative_current_scouts_indices = current_scouts.loc[
            (current_scouts < 0).any(axis=1)
        ].index

        return negative_difference_indices.union(negative_current_scouts_indices)

    def get_scouts_subtraction_invalid_indices(
        self, current_scouts, accumulated_scouts
    ):
        """
        Identifies invalid scout data indices for subtraction.

        Determines the indices representing invalid data resulting from subtracting 
        between current and accumulated scout data. These indices consist 
        of 'new_indices' (representing data without accumulated values to subtract) 
        and 'invalid_indices' (representing data that is invalid for subtraction).

        Args:
            current_scouts (pd.DataFrame): Current scout data.
            accumulated_scouts (pd.DataFrame): Accumulated scout data.

        Returns:
            pd.Index: Indices representing invalid or new scout data.
        """
        new_indices = current_scouts.index.difference(accumulated_scouts.index)
        invalid_indices = self.get_invalid_scouts_indices(
            current_scouts, accumulated_scouts
        )

        return new_indices.union(invalid_indices)

    def get_null_data_indices(self, current_scouts, accumulated_scouts):
        """
        Identifies indices that should be null.

        Determines the indices representing data that should be null. These indices 
        consist of 'missing_indices' (representing data missing in the current set) 
        and 'invalid_indices' (representing invalid).

        Args:
            current_scouts (pd.DataFrame): Current scout data.
            accumulated_scouts (pd.DataFrame): Accumulated scout data.

        Returns:
            pd.Index: Indices representing null data.
        """
        missing_indices = accumulated_scouts.index.difference(current_scouts.index)
        invalid_indices = self.get_invalid_scouts_indices(
            current_scouts, accumulated_scouts
        )

        return missing_indices.union(invalid_indices)

    def calculate_scouts_difference(self, current_data, accumulated_scouts):
        """
        Calculates the difference between current and accumulated scout data.

        Computes the difference between the current scout data and the accumulated 
        scout data after removing invalid indices. It uses the filtered indices 
        to extract valid data for computation.

        Args:
            current_data (pd.DataFrame): Current scout data.
            accumulated_scouts (pd.DataFrame): Accumulated scout data.

        Returns:
            pd.DataFrame: The computed difference between current and accumulated 
            scout data.
        """
        current_scouts = current_data[accumulated_scouts.columns]

        invalid_indices = self.get_scouts_subtraction_invalid_indices(
            current_scouts, accumulated_scouts
        )
        filtered_indices = current_scouts.index.difference(invalid_indices)

        current_scouts_filtered = current_scouts.loc[filtered_indices]
        accumulated_scouts_filtered = accumulated_scouts.loc[filtered_indices]

        scouts_difference = current_scouts_filtered.sub(
            accumulated_scouts_filtered, fill_value=0
        )

        return scouts_difference

    def create_null_rows_for_missing_members(self, current_data, accumulated_scouts):
        """
        Creates null rows for missing or invalid member data in the current data.

        Generates rows with null values for members that are missing or have invalid data 
        in the current scout data but exist in the accumulated scout data. The function 
        uses the filtered null indices to construct these rows.

        Args:
            current_data (pd.DataFrame): Current scout data.
            accumulated_scouts (pd.DataFrame): Accumulated scout data.

        Returns:
            pd.DataFrame: DataFrame with null rows for missing or invalid members.
        """
        current_scouts = current_data[accumulated_scouts.columns]

        null_data_indices = self.get_null_data_indices(
            current_scouts, accumulated_scouts
        )

        null_rows = pd.DataFrame(index=null_data_indices, columns=self.sorted_columns)
        null_rows["rodada_id"] = self.round_number
        null_rows["clube_id"] = NO_CLUBE_ID
        null_rows.drop("membro_equipe_id", axis=1, inplace=True)

        return null_rows

    def transform_data(self):
        """
        Transforms source data into a structured dataset for the 'pontuacoes' fact.

        Retrieves source data, extracts and processes current scout data. If the current 
        round is higher than the initial round, it calculates the difference between 
        the current and accumulated scout data, updates the current data, and creates 
        null rows for missing or invalid members. The function then combines the updated 
        data with the null rows, resets the index, and returns a list of tuples.

        Returns:
            list: A list of tuples representing the transformed data.
        """
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

            current_data = current_data.combine_first(null_rows)
            current_data.reset_index(inplace=True)
            current_data = current_data[self.sorted_columns]
            current_data.replace(np.nan, None, inplace=True)

        return list(current_data.itertuples(name=None, index=False))
