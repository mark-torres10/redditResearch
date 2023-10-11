"""Migrate legacy data to .csv and to DB."""
import os

import pandas as pd

from data.helper import dump_df_to_csv
from legacy.helper import LEGACY_SYNC_DATA_DIR
from lib.db.sql.helper import write_df_to_database


def load_legacy_sync_data() -> dict[str, pd.DataFrame]:
    """Loads in the legacy sync .csv."""
    list_sync_data_dfs: list[pd.DataFrame] = []
    list_sync_metadata_dfs: list[pd.DataFrame] = []
    for _, dirnames, _ in os.walk(LEGACY_SYNC_DATA_DIR):
        for timestamp_dir in dirnames:
            if timestamp_dir == "tests":
                continue
            full_directory = os.path.join(LEGACY_SYNC_DATA_DIR, timestamp_dir)
            results_jsonl_fp = os.path.join(full_directory, "results.jsonl")
            metadata_fp = os.path.join(full_directory, "metadata.csv")
            sync_data_df = pd.read_json(results_jsonl_fp)
            sync_metadata_df = pd.read_csv(metadata_fp)            
            list_sync_data_dfs.append(sync_data_df)
            list_sync_metadata_dfs.append(sync_metadata_df)

    return {
        "sync_data": pd.concat(list_sync_data_dfs),
        "sync_metadata": pd.concat(list_sync_metadata_dfs)
    }


def get_user_data_from_legacy_df(legacy_df: pd.DataFrame) -> list[dict]:
    """Grabs user data from the legacy data syncs. Returns as a list of
    dicts so that we can add default values to the dfs for any missing fields
    and then write them to our database.
    """
    legacy_users_list_dicts: list[dict] = []
    return legacy_users_list_dicts


def convert_legacy_user_data_to_new_format(
    legacy_user_data: list[dict]
) -> list[dict]:
    """Converts legacy user data into new `user` table format by adding any
    necessary fields as well as doing any necessary transformations.

    We want to migrate the legacy user data to the new DB since we don't want
    to, for example, message users who we've already seen.
    """
    converted_users_list_dicts: list[dict] = []
    return converted_users_list_dicts


def convert_legacy_sync_data() -> dict[str, pd.DataFrame]:
    """Loads in legacy sync data as dataframes, performs necessary
    transformations and conversions, and them returns a dictionary
    where the key is the table name and the value is the converted dataframe.
    """
    sync_data_dict: dict[str, pd.DataFrame] = load_legacy_sync_data()
    legacy_sync_data_df: pd.DataFrame = sync_data_dict["sync_data"]
    legacy_user_data: list[dict] = get_user_data_from_legacy_df(legacy_sync_data_df) # noqa
    converted_user_data: list[dict] = convert_legacy_user_data_to_new_format(legacy_user_data) # noqa
    users_df = pd.DataFrame(converted_user_data)

    table_to_df_map = {
        "users": users_df
    }

    return table_to_df_map


def convert_legacy_data() -> None:
    table_to_converted_legacy_data_map = convert_legacy_sync_data()
    for table_name, df in table_to_converted_legacy_data_map.items():
        print(f"Dumping legacy data to {table_name}...")
        dump_df_to_csv(df=df, table_name=table_name)
        write_df_to_database(df=df, table_name=table_name)
        print(f"Finished dumping legacy data to {table_name}")


if __name__ == "__main__":
    convert_legacy_data()
