import json
import os
import subprocess
from typing import Optional

import pandas as pd

from lib.helper import CURRENT_TIME_STR

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

def dump_df_to_csv(
    df: pd.DataFrame,
    table_name: str,
    filename: Optional[str] = f"{CURRENT_TIME_STR}.csv"
) -> None:
    """Dumps a pandas df to .csv.
    
    Takes as argument the table name, which will be the folder that the data is
    stored in. The filename is the name of the .csv file. By default, it will
    be determined by the timestamp.
    """
    # create directory for table if it doesn't exist
    table_dir = os.path.join(DATA_DIR, table_name)
    if not os.path.exists(table_dir):
        os.makedirs(table_dir)
    
    # dump df to csv
    df.to_csv(os.path.join(table_dir, filename), index=False)


def dump_dict_as_tmp_json(data: dict, table_name: str, filename: str) -> None:
    """Write a dictionary to a temporary filepath."""
    try:
        table_dir = os.path.join(DATA_DIR, table_name)
        if not os.path.exists(table_dir):
            os.makedirs(table_dir)
        tmp_dir = os.path.join(table_dir, "tmp")
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        with open(os.path.join(tmp_dir, filename), "w") as f:
            json.dump(data, f)
        print(f"Successfully wrote {filename} to {tmp_dir}.")
    except Exception as e:
        print(f"Error writing {filename} to {tmp_dir}.")
        print(e)


def load_tmp_json_data(table_name: str) -> list[dict]:
    """Load json data from a temporary filepath as a pandas df."""
    dir = os.path.join(DATA_DIR, table_name, "tmp")
    if not os.path.exists(dir):
        print("No tmp data to load.")
        return []
    files = os.listdir(dir)
    if len(files) == 0:
        raise ValueError(f"No files in directory {dir}.")

    data = []
    for file in files:
        with open(os.path.join(dir, file), "r") as f:
            data.append(json.load(f))
    
    return data


def delete_tmp_json_data(table_name: str) -> None:
    """Delete tmp json data from tmp directory."""
    table_dir = os.path.join(DATA_DIR, table_name)
    if not os.path.exists(table_dir):
        print(f"No tmp data to delete. Directory {table_dir} doesn't exist.")
        return
    tmp_dir = os.path.join(table_dir, "tmp")
    files = os.listdir(tmp_dir)
    if not files:
        print("No tmp data to delete.")
        return
    for file in files:
        os.remove(os.path.join(tmp_dir, file))
    os.rmdir(tmp_dir)
    print(f"Successfully deleted {len(files)} files from tmp directory.")


def load_tmp_json_data_as_df(
    table_name: str, delete_tmp_data: bool=False
) -> pd.DataFrame:
    data = load_tmp_json_data(table_name)
    df = pd.DataFrame(data)
    if delete_tmp_data:
        delete_tmp_json_data(table_name)
    return df
