from typing import Dict
import json
import requests
from pandas import DataFrame, read_csv, read_json, to_datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src import config

# def temp() -> DataFrame:
#     """Get the temperature data.
#     Returns:
#         DataFrame: A dataframe with the temperature data.
#     """
#     return read_csv("data/temperature.csv")

year = "2017"

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil.
    Args:
        public_holidays_url (str): url to the public holidays.
        year (str): The year to get the public holidays for.
    Raises:
        SystemExit: If the request fails.
    Returns:
        DataFrame: A dataframe with the public holidays.
    """
    
    # TODO: Implementa esta función.
    # Debes usar la biblioteca requests para obtener los días festivos públicos del año dado.
    # La URL es public_holidays_url/{year}/BR.
    # Debes eliminar las columnas "types" y "counties" del DataFrame.
    # Debes convertir la columna "date" a datetime.
    # Debes lanzar SystemExit si la solicitud falla. Investiga el método raise_for_status
    # de la biblioteca requests.

    response = requests.get(f"{public_holidays_url}/{year}/BR")
    try:
        response.raise_for_status()
        datajson = response.json()
        df = DataFrame(datajson)
        df["date"] = to_datetime(df["date"])
        df = df.drop(columns=["types", "counties"])
        return df
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    
   
def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the csv's folder.
        csv_table_mapping (Dict[str, str]): The mapping of the csv file names to the
        table names.
        public_holidays_url (str): The url to the public holidays.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as
        the dataframes.
    """
    
    
    dataframes = {
        
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }
    holidays = get_public_holidays(public_holidays_url, year)

    dataframes["public_holidays"] = holidays

    return dataframes


