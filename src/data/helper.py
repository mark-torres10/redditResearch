import json
import os
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
    

def load_tmp_json_data_as_df(
    table_name: str, delete_tmp_data: bool=False
) -> pd.DataFrame:
    """Load json data from a temporary filepath as a pandas df."""
    dir = os.path.join(DATA_DIR, table_name, "tmp")
    if not os.path.exists(dir):
        raise ValueError(f"Directory {dir} doesn't exist.")
    files = os.listdir(dir)
    if len(files) == 0:
        raise ValueError(f"No files in directory {dir}.")

    data = []
    for file in files:
        with open(os.path.join(dir, file), "r") as f:
            data.append(json.load(f))
    df = pd.DataFrame(data)

    if delete_tmp_data:
        for file in files:
            os.remove(os.path.join(dir, file))
        os.rmdir(dir)

    return df


def backup_postgres_data_to_sql() -> None:
    """Dump Postgres data to .sql file for backup."""
    postgres_dir = os.path.join(DATA_DIR, "postgres_data")
    if not os.path.exists(postgres_dir):
        os.makedirs(postgres_dir)
    filename = f"{CURRENT_TIME_STR}.sql"
    compressed_filename= f"{CURRENT_TIME_STR}.sql.gz"
    full_fp = os.path.join(postgres_dir, filename)
    compressed_fp = os.path.join(postgres_dir, compressed_filename)
    print(f"Dumping Postgres data into {full_fp}.")
    os.system(f"pg_dump -h localhost -U postgres -d reddit_data -f {full_fp}")
    os.system(f"pg_dump -h localhost -U postgres -d reddit_data | gzip > {compressed_fp}") # noqa
    print(f"Successfully dumped Postgres data into {full_fp}.")


def backup_postgres_data(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        backup_postgres_data_to_sql()
        return result

    return wrapper
