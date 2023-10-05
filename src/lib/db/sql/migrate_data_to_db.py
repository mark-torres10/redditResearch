"""Migrate existing .csv data to DB"""
import os

import pandas as pd

from data.helper import DATA_DIR
from lib.db.sql.helper import write_df_to_database


def migrate_csv_to_db(table_name: str, rebuild_table: bool = False) -> None:
    """Migrate existing .csv data to DB"""
    table_dir_path = os.path.join(DATA_DIR, table_name)
    # loop through all the .csv files and create pandas dataframes
    # then, union together all the dataframes.
    df = pd.concat([
        pd.read_csv(os.path.join(table_dir_path, filename))
        for filename in os.listdir(table_dir_path)
        if filename.endswith(".csv")
    ], ignore_index=True)

    print(df.columns)
    # write to Postgres db.
    write_df_to_database(
        df=df, table_name=table_name, rebuild_table=rebuild_table
    )
    print(f"Successfully uploaded .csv data to DB, for table {table_name}")


if __name__ == "__main__":
    tables = ["subreddits", "users", "threads", "comments"]
    rebuild_table = True
    for table_name in tables:
        migrate_csv_to_db(table_name=table_name, rebuild_table=rebuild_table)
