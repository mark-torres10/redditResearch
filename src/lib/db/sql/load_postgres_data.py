"""Loads the backup data into a Postgres table.

By default, takes the latest snapshot.
"""
import os

import pandas as pd

from lib.db.sql.helper import (
    current_file_directory as sql_helper_dir,
    write_df_to_database
)
from lib.db.sql.tables import TABLE_TO_KEYS_MAP

snapshots_dir = os.path.join(sql_helper_dir, "snapshots")


def get_latest_snapshot() -> str:
    return max(os.listdir(snapshots_dir))


def get_compressed_files_to_load(latest_snapshot_timestamp: str) -> list[str]:
    full_dir_path = os.path.join(
        snapshots_dir, latest_snapshot_timestamp
    )
    full_filepaths = [
        os.path.join(full_dir_path, file)
        for file in os.listdir(full_dir_path)
        if file.endswith(".csv.gz")
    ]
    return full_filepaths


def load_single_data_dump(table_name: str, file_path: str) -> None:
    try:
        df = pd.read_csv(file_path, compression="gzip")
        write_df_to_database(
            df=df, table_name=table_name, rebuild_table=False, upsert=True
        )
        print(f"Successfully dumped {file_path} to table {table_name} in DB.")
    except Exception as e:
        print(f"Unable to load {file_path} into the database, with error {e}.")
        raise e


def load_dumps_into_db(compressed_files: list[str]) -> None:
    # creating a map like this is necessary so that the tables are all build
    # in the correct order. Otherwise, we will try to create tables that
    # reference other tables before those other tables exist.
    table_name_to_filepath_map = {
        os.path.basename(file_path).split(".")[0]: file_path
        for file_path in compressed_files
    }
    table_to_filepath_map = {
        table: table_name_to_filepath_map[table]
        for table in TABLE_TO_KEYS_MAP.keys()
    }
    for table_name, file_path in table_to_filepath_map.items():
        load_single_data_dump(table_name=table_name, file_path=file_path)



def main() -> None:
    latest_snapshot = get_latest_snapshot()
    compressed_files = get_compressed_files_to_load(latest_snapshot)
    load_dumps_into_db(compressed_files)
    print(f"Finished loading data from {latest_snapshot} into the database.")


if __name__ == "__main__":
    main()
