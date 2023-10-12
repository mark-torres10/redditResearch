"""Migrate legacy data to .csv and to DB."""
import json
import os

import pandas as pd

from data.helper import dump_df_to_csv
from legacy.helper import LEGACY_SYNC_DATA_DIR
from lib.db.sql.helper import load_table_as_df, write_df_to_database
from lib.reddit import init_api_access
from services.determine_authors_to_message.helper import DENYLIST_AUTHORS
from services.sync_single_subreddit.transformations import remove_prefix_from_id # noqa


EXCLUDELIST_DIRS = ["tests", "__pycache__"]
api = init_api_access()


def read_jsonl_as_list_dicts(filepath: str) -> list[dict]:
    json_list = []
    with open(filepath, 'r') as file:
        for line in file:
            json_object = json.loads(line)
            json_list.append(json_object)
    return json_list


def load_legacy_sync_data() -> dict[str, pd.DataFrame]:
    """Loads in the legacy sync .csv."""
    list_sync_data_dfs: list[pd.DataFrame] = []
    list_sync_metadata_dfs: list[pd.DataFrame] = []
    for _, dirnames, _ in os.walk(LEGACY_SYNC_DATA_DIR):
        for timestamp_dir in dirnames:
            if timestamp_dir in EXCLUDELIST_DIRS:
                continue
            full_directory = os.path.join(LEGACY_SYNC_DATA_DIR, timestamp_dir)
            results_jsonl_fp = os.path.join(full_directory, "results.jsonl")
            metadata_fp = os.path.join(full_directory, "metadata.csv")
            sync_data_list_dicts = read_jsonl_as_list_dicts(results_jsonl_fp)
            sync_data_df = pd.DataFrame(sync_data_list_dicts)
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

    In the old syncs, we prioritized syncing only the comments. Each comment,
    however, has the author ID and name attached. We can identify users using
    this information.
    """
    # note: author_fullname is the ID, author is the actual name
    relevant_cols = ["author_fullname", "author"]
    legacy_users_info: list[dict] = legacy_df[relevant_cols].to_dict("records")
    return legacy_users_info


def convert_legacy_user_data_to_new_format(
    legacy_user_data: list[dict]
) -> list[dict]:
    """Converts legacy user data into new `user` table format by adding any
    necessary fields as well as doing any necessary transformations.

    We want to migrate the legacy user data to the new DB since we don't want
    to, for example, message users who we've already seen.

    We take the author information from past syncs (so, the author ID and the
    author names) and then for the other values in the user table, we add None
    as a default value. We then create a list of dictionaries for each author
    so that we can write them to the `users` table.
    """
    users_df = load_table_as_df(table_name="users")

    # convert legacy data to use the same fieldnames as the `users` table
    # also, filter out users who we already have in the `users` table
    existing_user_names = users_df["name"].tolist()
    existing_user_ids = users_df["id"].tolist()

    # if there are nulls for some reason in the user data, we can hydrate.
    hydrated_legacy_user_data = []

    for legacy_user in legacy_user_data:
        author_name = legacy_user["author"]
        author_id = legacy_user["author_fullname"]
        if pd.isna(author_id) and not pd.isna(author_name):
            print(f"Hydrating missing ID for author {author_name}")
            author_obj = api.redditor(name=author_name)
            author_id = author_obj.id
        if pd.isna(author_name) and not pd.isna(author_id):
            print(f"Hydrating missing name for author id {author_id}")
            author_id = author_id if author_id.startswith("t2_") else f"t2_{author_id}" # noqa
            author_obj = api.redditor(fullname=author_id)
            author_name = author_obj.name
        if "t2_" in author_id:
            author_id = remove_prefix_from_id(author_id)
        hydrated_legacy_user_data.append(
            {"name": author_name, "id": author_id}
        )

    legacy_user_data = hydrated_legacy_user_data

    # dedupe the legacy user data
    deduped_legacy_user_data = []
    seen_legacy_names = set()
    seen_legacy_ids = set()
    duplicate_legacy_user_data_count = 0
    for user_data in legacy_user_data:
        if (
            user_data["name"] not in seen_legacy_names
            and user_data["id"] not in seen_legacy_ids
        ):
            deduped_legacy_user_data.append(user_data)
            seen_legacy_names.add(user_data["name"])
            seen_legacy_ids.add(user_data["id"])
        else:
            duplicate_legacy_user_data_count += 1

    if duplicate_legacy_user_data_count > 0:
        print(f"Removed {duplicate_legacy_user_data_count} duplicate legacy user data.") # noqa

    # insert legacy data only if we haven't seen it before.
    legacy_user_data = [
        user_data
        for user_data in deduped_legacy_user_data
        if user_data["name"] not in existing_user_names
        and user_data["id"] not in existing_user_ids
        and user_data["name"] not in DENYLIST_AUTHORS
    ]

    default_values = {
        col: None
        for col in users_df.columns
        if col not in legacy_user_data[0].keys()
    }

    return [
        {**legacy_data, **default_values}
        for legacy_data in legacy_user_data
    ]


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
    """Converts legacy data to new format and writes to DB.
    
    Dump raw data as .csv, but write in a new file directory so we know which
    writes were from the legacy syncs and which are from this iteration of
    the pipeline.
    """
    table_to_converted_legacy_data_map = convert_legacy_sync_data()
    for table_name, df in table_to_converted_legacy_data_map.items():
        print(f"Dumping legacy data to {table_name}...")
        write_df_to_database(df=df, table_name=table_name)
        dump_df_to_csv(df=df, table_name=f"legacy_{table_name}")
        print(f"Finished dumping legacy data to {table_name}")


if __name__ == "__main__":
    convert_legacy_data()
