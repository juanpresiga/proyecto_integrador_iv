from typing import Dict

from pandas import DataFrame
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine
from extract import extract
import sys
import os
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src import config
import sqlite3

database_path = Path(config.SQLITE_BD_ABSOLUTE_PATH).as_posix()
database_url = f"sqlite:///{database_path}"

database: Engine = create_engine(
    database_url,  
    connect_args={"check_same_thread": False} 
)

def create_connection():
    connection = None
    try:
        connection = sqlite3.connect(config.SQLITE_BD_ABSOLUTE_PATH)
        print("Connection to SQLite DB successful")
    except sqlite3.Error as e:
        print(f"The error '{e}' occurred")
    return connection


public_holidays_url = config.PUBLIC_HOLIDAYS_URL
csv_folder = "dataset"
csv_table_mapping = config.get_csv_to_table_mapping()

data_frames = extract(csv_folder, csv_table_mapping, public_holidays_url)


def load(data_frames: Dict[str, DataFrame], database: Engine):

    conn = create_connection()
    if conn:
        for table_name, df in data_frames.items():
            print(f"Inserting data into {table_name} table.")
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print("Datos insertados en la base de datos exitosamente.")

    conn.close()
