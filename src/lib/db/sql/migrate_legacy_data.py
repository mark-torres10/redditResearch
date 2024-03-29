"""Migrate legacy data to .csv and to DB."""
import datetime
import json
import os

import pandas as pd
from prawcore.exceptions import NotFound

from data.helper import dump_df_to_csv
from legacy.helper import (
    LEGACY_AUTHOR_PHASE_DATA_DIR, LEGACY_MESSAGES_RECEIVED_DIR,
    LEGACY_OBSERVER_PHASE_DATA_DIR, LEGACY_SYNC_DATA_DIR
)
from lib.db.sql.helper import load_table_as_df, write_df_to_database
from lib.helper import DENYLIST_AUTHORS
from lib.reddit import init_api_access
from services.get_received_messages.helper import (
    message_columns_to_extract,
    table_fields as messages_received_table_fields
)
from services.sync_single_subreddit.transformations import remove_prefix_from_id # noqa


EXCLUDELIST_DIRS = ["tests", "__pycache__"]
api = init_api_access()
default_user_to_message_status_values = {
    "message_status": "messaged_successfully",
    "last_update_timestamp": datetime.datetime.utcnow().isoformat(),
    "last_update_step": "legacy_data_migration",
    "comment_id": None,
    "comment_text": None,
    "dm_text": None
}


def read_jsonl_as_list_dicts(filepath: str) -> list[dict]:
    json_list = []
    with open(filepath, 'r') as file:
        for line in file:
            json_object = json.loads(line)
            json_list.append(json_object)
    return json_list


def hydrate_author_information(
    author_name: str, author_id: str
) -> dict[str, str]:
    """Hydrates author information by providing either the missing author ID or
    the missing author name, if applicable.
    """
    if pd.isna(author_id) and pd.isna(author_name):
        print("Unable to hydrate missing author information. Both author ID and name are missing.") # noqa
        return {"name": None, "id": None}
    if pd.isna(author_id) and not pd.isna(author_name):
        print(f"Hydrating missing ID for author {author_name}")
        author_obj = api.redditor(name=author_name)
        author_id = author_obj.id
    if pd.isna(author_name) and not pd.isna(author_id):
        print(f"Hydrating missing name for author id {author_id}")
        author_id = author_id if author_id.startswith("t2_") else f"t2_{author_id}" # noqa
        author_obj = api.redditor(fullname=author_id)
        try:
            author_name = author_obj.name
        except NotFound as e:
            print(f"NotFound error: {e}")
            if author_obj._fetched:
                author_name = author_obj.__dict__["name"]
            else:
                print(f"Unable to hydrate with author name for author with id={author_id}. Possibly private or deleted account...") # noqa
    if "t2_" in author_id:
        author_id = remove_prefix_from_id(author_id)
    return {"name": author_name, "id": author_id}


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

    # if there are nulls in the author information, we can hydrate
    hydrated_legacy_user_data: list[dict] = []

    for legacy_user in legacy_user_data:
        author_name = legacy_user["author"]
        author_id = legacy_user["author_fullname"]
        author_info = hydrate_author_information(
            author_name=author_name, author_id=author_id
        )
        hydrated_legacy_user_data.append(author_info)

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

    if len(legacy_user_data) == 0:
        print("No more legacy data to migrate.")
        return []

    default_values = {
        col: None
        for col in users_df.columns
        if col not in legacy_user_data[0].keys()
    }

    return [
        {**legacy_data, **default_values}
        for legacy_data in legacy_user_data
    ]


def convert_legacy_sync_data() -> list[dict]:
    """Loads in legacy sync data as dataframes, performs necessary
    transformations and conversions, and them returns a dictionary
    where the key is the table name and the value is the converted dataframe.
    """
    sync_data_dict: dict[str, pd.DataFrame] = load_legacy_sync_data()
    legacy_sync_data_df: pd.DataFrame = sync_data_dict["sync_data"]
    legacy_user_data: list[dict] = get_user_data_from_legacy_df(legacy_sync_data_df) # noqa
    converted_user_data: list[dict] = convert_legacy_user_data_to_new_format(legacy_user_data) # noqa
    users_df = pd.DataFrame(converted_user_data)

    return [
        {
            "table_name": "users",
            "df": users_df,
            "upsert": False
        }
    ]


def load_legacy_author_phase_data() -> list[dict]:
    all_messages_users_fp = os.path.join(
        LEGACY_AUTHOR_PHASE_DATA_DIR, "all_messaged_users.csv"
    )
    df = pd.read_csv(all_messages_users_fp)
    return df.to_dict("records")


def load_legacy_observer_phase_data() -> list[dict]:
    observer_phase_df_list: list[pd.DataFrame] = []
    for subreddit_dir in os.listdir(LEGACY_OBSERVER_PHASE_DATA_DIR):
        for filename in os.listdir(
            os.path.join(LEGACY_OBSERVER_PHASE_DATA_DIR, subreddit_dir)
        ):
            full_fp = os.path.join(
                LEGACY_OBSERVER_PHASE_DATA_DIR, subreddit_dir, filename
            )
            df = pd.read_csv(full_fp)
            observer_phase_df_list.append(df)
    return pd.concat(observer_phase_df_list).to_dict("records")


def convert_legacy_author_phase_data(
    existing_user_to_message_status_df: pd.DataFrame
) -> pd.DataFrame:
    print("Converting legacy author phase data...")
    legacy_author_phase_data: list[dict] = load_legacy_author_phase_data()
    subset_legacy_author_phase_data = [
        {
            "user_id": user_data["author_id"],
            "author_screen_name": user_data["author_screen_name"],
            "comment_id": user_data["post_id"],
            "phase": "author"
        }
        for user_data in legacy_author_phase_data
    ]

    # if there are nulls in the author information, we can hydrate
    hydrated_legacy_author_phase_data: list[dict] = []
    for author_phase_data in subset_legacy_author_phase_data:
        author_name = author_phase_data["author_screen_name"]
        author_id = author_phase_data["user_id"]
        if pd.isna(author_name) and pd.isna(author_id):
            print("Row is missing both author ID and name, we can't add this data to our DB. Skipping...") # noqa
            continue
        author_info = hydrate_author_information(
            author_name=author_name, author_id=author_id
        )
        if pd.isna(author_info["name"]) or pd.isna(author_info["id"]):
            print("Unable to hydrate author information. We can't add this to our DB...")  # noqa
            print(f"Name: {author_info['name']}, ID: {author_info['id']}")
            continue
        hydrated_legacy_author_phase_data.append(
            {
                "user_id": author_info["id"],
                "author_screen_name": author_info["name"],
                "comment_id": author_phase_data["comment_id"],
                "phase": author_phase_data["phase"]
            }
        )

    # dedupe hydrated_legacy_author_phase_data on user_id
    seen_user_ids = set()
    deduped_legacy_author_phase_data = []
    duplicate_legacy_author_phase_data = 0
    for data in hydrated_legacy_author_phase_data:
        if data["user_id"] not in seen_user_ids:
            deduped_legacy_author_phase_data.append(data)
            seen_user_ids.add(data["user_id"])
        else:
            duplicate_legacy_author_phase_data += 1
    if duplicate_legacy_author_phase_data > 0:
        print(f"Removed {duplicate_legacy_author_phase_data} duplicate legacy author phase data.") # noqa

    # remove data that we already have in the `user_to_message_status` table.
    existing_user_ids = existing_user_to_message_status_df["user_id"].tolist()
    deduped_legacy_author_phase_data = [
        data
        for data in deduped_legacy_author_phase_data
        if data["user_id"] not in existing_user_ids
        and data["author_screen_name"] not in DENYLIST_AUTHORS
    ]

    # hydrate legacy data with default values
    converted_legacy_author_data = [
        {
            **default_user_to_message_status_values,
            **legacy_data
        }
        for legacy_data in deduped_legacy_author_phase_data
    ]
    df = pd.DataFrame(converted_legacy_author_data)
    print("Finished converting legacy author phase data.")
    return df


def convert_legacy_observer_phase_data(
    existing_user_to_message_status_df: pd.DataFrame
) -> pd.DataFrame:
    print("Converting legacy observer phase data...")
    legacy_observer_phase_data: list[dict] = load_legacy_observer_phase_data()
    subset_legacy_observer_phase_data = [
        {
            "user_id": user_data["author_id"],
            "author_screen_name": user_data["author_name"],
            "comment_id": user_data["post_id"],
            "comment_text": user_data["post_body"],
            "phase": "observer"
        }
        for user_data in legacy_observer_phase_data
    ]

    # if there are nulls in the author information, we can hydrate
    hydrated_legacy_observer_phase_data: list[dict] = []
    for observer_phase_data in subset_legacy_observer_phase_data:
        author_name = observer_phase_data["author_screen_name"]
        author_id = observer_phase_data["user_id"]
        if pd.isna(author_name) and pd.isna(author_id):
            print("Row is missing both author ID and name, we can't add this data to our DB. Skipping...") # noqa
            continue
        author_info = hydrate_author_information(
            author_name=author_name, author_id=author_id
        )
        if pd.isna(author_info["name"]) or pd.isna(author_info["id"]):
            print("Unable to hydrate author information. We can't add this to our DB...")  # noqa
            print(f"Name: {author_info['name']}, ID: {author_info['id']}")
            continue
        hydrated_legacy_observer_phase_data.append(
            {
                "user_id": author_info["id"],
                "author_screen_name": author_info["name"],
                "comment_id": observer_phase_data["comment_id"],
                "comment_text": observer_phase_data["comment_text"],
                "phase": observer_phase_data["phase"]
            }
        )

    # dedupe hydrated_legacy_observer_phase_data on user_id
    seen_user_ids = set()
    deduped_legacy_observer_phase_data = []
    duplicate_legacy_observer_phase_data = 0
    for data in hydrated_legacy_observer_phase_data:
        if data["user_id"] not in seen_user_ids:
            deduped_legacy_observer_phase_data.append(data)
            seen_user_ids.add(data["user_id"])
        else:
            duplicate_legacy_observer_phase_data += 1
    if duplicate_legacy_observer_phase_data > 0:
        print(f"Removed {duplicate_legacy_observer_phase_data} duplicate legacy observer phase data.") # noqa

    # remove data that we already have in the `user_to_message_status` table.
    existing_user_ids = existing_user_to_message_status_df["user_id"].tolist()
    deduped_legacy_observer_phase_data = [
        data
        for data in deduped_legacy_observer_phase_data
        if data["user_id"] not in existing_user_ids
        and data["author_screen_name"] not in DENYLIST_AUTHORS
    ]

    converted_legacy_observer_data = [
        {
            **default_user_to_message_status_values,
            **legacy_data
        }
        for legacy_data in deduped_legacy_observer_phase_data
    ]
    df = pd.DataFrame(converted_legacy_observer_data)
    print("Finished converting legacy observer phase data.")
    return df


def convert_legacy_messaging_data() -> list[dict]:
    """Add legacy author/observer phase data to our `user_to_message_status`
    table. This is so that we don't message users who we've already messaged
    in the past.
    
    We want to upsert these so that so that we can record users who we may have
    already synced but in reality we already messaged in the past.
    """
    existing_user_to_message_status_df = load_table_as_df(
        table_name="user_to_message_status"
    )
    legacy_author_phase_df = convert_legacy_author_phase_data(
        existing_user_to_message_status_df
    )
    legacy_observer_phase_df = convert_legacy_observer_phase_data(
        existing_user_to_message_status_df
    )
    legacy_messaging_df = pd.concat(
        [legacy_author_phase_df, legacy_observer_phase_df]
    )

    # dedupe legacy_messaging_id on user_id, since it's technically possible to
    # have accidentally DMed someone in both the author and observer phases in
    # the old pipeline (don't think we did an explicit check on this).
    legacy_messaging_df = legacy_messaging_df.drop_duplicates(subset=["user_id"]) # noqa

    # we upsert in case we've already synced a user who we actually already
    # DMed in a previous phase.
    user_to_message_status_lst = [
        {
            "table_name": "user_to_message_status",
            "df": legacy_messaging_df,
            "upsert": True
        }
    ]

    # check if any of the user ids is not in the `users` table. If there are
    # any, add to the `users` table.
    print("Checking to see if any legacy author/observer phase users are not in the `users` table.") # noqa
    users_df = load_table_as_df("users")
    legacy_messaging_author_info = [
        {
            "name": user_data["author_screen_name"],
            "id": user_data["user_id"]
        }
        for _, user_data in legacy_messaging_df.iterrows()
    ]

    existing_user_names = users_df["name"].tolist()
    existing_user_ids = users_df["id"].tolist()
    missing_user_info: list[dict] = []
    missing_user_names = set()
    missing_user_ids = set()
    for legacy_author_info in legacy_messaging_author_info:
        if (
            legacy_author_info["name"] not in existing_user_names
            or legacy_author_info["id"] not in existing_user_ids
            and legacy_author_info["name"] not in DENYLIST_AUTHORS
            and legacy_author_info["name"] not in missing_user_names
            and legacy_author_info["id"] not in missing_user_ids
        ):
            missing_user_info.append(legacy_author_info)
            missing_user_names.add(legacy_author_info["name"])
            missing_user_ids.add(legacy_author_info["id"])

    if len(missing_user_info) > 0:
        print(f"Adding {len(missing_user_info)} legacy users to the `users` table...") # noqa
        default_values = {
            col: None
            for col in users_df.columns
            if col not in legacy_messaging_author_info[0].keys()
        }

        missing_users_df = pd.DataFrame([
            {**legacy_data, **default_values}
            for legacy_data in missing_user_info
        ])

        # add `users` table updates to beginning of output list so that the
        # missing users are added to the `users` table before we try to write
        # the legacy data to the `user_to_message_status` table.
        user_list = [
            {
                "table_name": "users",
                "df": missing_users_df,
                "upsert": False
            }
        ]
    else:
        user_list = []

    output_list = user_list + user_to_message_status_lst

    return output_list


def load_legacy_messages_received_data_as_df() -> pd.DataFrame:
    fp = os.path.join(LEGACY_MESSAGES_RECEIVED_DIR, "messages_received.csv")
    df = pd.read_csv(fp)
    df = df.drop_duplicates(subset=["id"])
    return df


def convert_legacy_messages_received_data() -> list[dict]:
    print("Converting legacy messages received data.")
    legacy_messages_received_df: pd.DataFrame = load_legacy_messages_received_data_as_df() # noqa
    existing_users_df = load_table_as_df("users")
    existing_user_to_message_status_df = load_table_as_df("user_to_message_status") # noqa
    output = []

    joined_legacy_messages_received_df = legacy_messages_received_df.merge(
        existing_user_to_message_status_df,
        how="left",
        left_on="author_id",
        right_on="user_id",
    )

    # rows with a valid `user_id` exist in `user_to_message_status` so we can
    # hydrate those accordingly.
    hydrated_legacy_messages_received_df = joined_legacy_messages_received_df[
        joined_legacy_messages_received_df["user_id"].notnull()
    ]

    # pull relevant cols for `messages_received` table
    hydrated_legacy_messages_received_df = (
        hydrated_legacy_messages_received_df[messages_received_table_fields]
    )

    # those missing user_id don't exist in `user_to_message_status` table so we
    # need to possibly add those.
    legacy_messages_missing_from_user_msg_status_table_df = (
        joined_legacy_messages_received_df[
            joined_legacy_messages_received_df["user_id"].isnull()
        ]
    )

    if len(legacy_messages_missing_from_user_msg_status_table_df) > 0:
        # for messages missing from `user_to_message_status` table, we need to (1)
        # pull the fields for which we do have info for, and (2) hydrate the fields
        # that we don't have info for. Once we do this, we can build dfs for both
        # updating the `user_to_message_status` table as well as the
        # `messages_received` table.
        message_ids_of_missing_messages: list[str] = (
            legacy_messages_missing_from_user_msg_status_table_df["id"].tolist()
        )
        messages_missing_from_user_msg_status_table_df = (
            legacy_messages_received_df[
                legacy_messages_received_df["id"].isin(
                    message_ids_of_missing_messages
                )
            ]
        )
        messages_missing_from_user_msg_status_table_df = (
            messages_missing_from_user_msg_status_table_df[message_columns_to_extract] # noqa
        )
        # we need to hydrate the author_screen_name given the author_id.  We can
        # likely also assume that the phase is "author" (imperfect, and will lead
        # to some observer phase conflicts, but we already added most, if not all,
        # the observer phase data to the `user_to_message_status` table, so for 
        # these missing values it's unlikely to come from the observer phase). For
        # the other missing values (e.g. comment_id, comment_text, dm_text), we can
        # just impute None. First, we get the data in the form necessary for the
        # `messages_received` table. Then, we use this information to transform the
        # data into a format ready for the `user_to_message_status` table.
        missing_messages_formatted_for_messages_received_table: list[dict] = []
        for data in messages_missing_from_user_msg_status_table_df.to_dict("records"): # noqa
            author_id = data["author_id"]
            author_info = hydrate_author_information(
                author_name=None, author_id=author_id
            )
            author_screen_name = author_info["name"]
            if pd.isna(author_screen_name):
                print(f"Unable to hydrate author screen name for author id {author_id}. Skipping...")
                continue
            hydrated_data = {
                "id": data["id"],
                "author_id": author_id,
                "author_screen_name": author_screen_name,
                "phase": "author",
                "body": data["body"],
                "created_utc": data["created_utc"],
                "created_utc_string": data["created_utc_string"],
                "comment_id": None,
                "comment_text": None,
                "dm_text": None,
                "synctimestamp": data["synctimestamp"]
            }
            missing_messages_formatted_for_messages_received_table.append(hydrated_data) # noqa

        manually_hydrated_legacy_messages_received_df = (
            pd.DataFrame(
                missing_messages_formatted_for_messages_received_table
            )
        )

        # now, take the messages that were missing from the
        # `user_to_message_status` table but were hydrated and then transform it
        # into a format to be added to the `user_to_message_status` table.
        missing_messages_formatted_for_user_to_message_status_table: list[dict] = [] # noqa
        for message in missing_messages_formatted_for_messages_received_table:
            combined_data = {
                **default_user_to_message_status_values,
                **{
                    "user_id": message["author_id"],
                    "author_screen_name": message["author_screen_name"],
                    "comment_id": message["comment_id"],
                    "comment_text": message["comment_text"],
                    "phase": message["phase"]
                }
            }
            missing_messages_formatted_for_user_to_message_status_table.append(combined_data) # noqa

        if len(missing_messages_formatted_for_user_to_message_status_table) > 0: # noqa
            missing_user_to_message_status_df = pd.DataFrame(missing_messages_formatted_for_user_to_message_status_table) # noqa
        else:
            missing_user_to_message_status_df = pd.DataFrame(
                [], columns=existing_user_to_message_status_df.columns
            )

    else:
        manually_hydrated_legacy_messages_received_df = pd.DataFrame(
            [], columns=hydrated_legacy_messages_received_df.columns
        )
        missing_user_to_message_status_df = pd.DataFrame(
            [], columns=existing_user_to_message_status_df.columns
        )

    # check if there are authors with messages and their author id isn't in the
    # `users` table. If any, then create a new df with the information about
    # the authors and pass it to the output.
    legacy_message_user_ids = legacy_messages_received_df["author_id"].tolist()
    existing_user_ids = existing_users_df["id"].tolist()
    missing_message_user_ids: list[str] = [
        message_user_id for message_user_id in legacy_message_user_ids
        if message_user_id not in existing_user_ids
    ]

    if len(missing_message_user_ids) > 0:
        print("Adding missing users to the `users` table...")
        missing_users_hydrated_info = [
            hydrate_author_information(
                author_name=None, author_id=message_user_id
            )
            for message_user_id in missing_message_user_ids
        ]
        default_user_values = {
            col: None
            for col in existing_users_df.columns
            if col not in missing_users_hydrated_info[0].keys()
        }
        missing_users_info = [
            {**default_user_values, **info}
            for info in missing_users_hydrated_info
        ]
        missing_users_df = pd.DataFrame(missing_users_info)
    else:
        print("No messages have user IDs that are not in the `users` table.")
        missing_users_df = pd.DataFrame(
            [], columns=existing_users_df.columns
        )

    # double-check that all users that are being added to the
    # `user_to_message_status` df are also either in the `users` table or the
    # `missing_users` df.
    missing_user_to_message_status_user_ids = missing_user_to_message_status_df["user_id"].tolist() # noqa
    missing_users_user_ids = missing_users_df["id"].tolist()
    missing_user_to_message_status_user_ids_not_in_users_table = [
        user_id for user_id in missing_user_to_message_status_user_ids
        if user_id not in existing_user_ids
        and user_id not in missing_users_user_ids
    ]
    if len(missing_user_to_message_status_user_ids_not_in_users_table) > 0:
        print(
            "There are {count} users being added to the `user_to_message_status` table that are not in the `users` table.".format( # noqa
                count=len(missing_user_to_message_status_user_ids_not_in_users_table) # noqa
            )
        )
        print("This shouldn't be the case.")

    # now that any necessary hydration is complete, export the dfs.
    messages_received_df = pd.concat([
        hydrated_legacy_messages_received_df,
        manually_hydrated_legacy_messages_received_df
    ])

    if len(missing_users_df) == 0:
        print("From the messages received data, no users to add to the `users` table") # noqa
    if len(missing_user_to_message_status_df) == 0:
        print("From the messages received data, no users to add to the `user_to_message_status` table") # noqa
    if len(messages_received_df) == 0:
        print("From the messages received data, no messages to add to the `messages_received` table") # noqa

    output.append({
        "table_name": "users",
        "df": missing_users_df,
        "upsert": False
    })

    output.append({
        "table_name": "user_to_message_status",
        "df": missing_user_to_message_status_df,
        "upsert": False
    })

    output.append({
        "table_name": "messages_received",
        "df": messages_received_df,
        "upsert": False
    })

    return output


def convert_legacy_data() -> None:
    """Converts legacy data to new format and writes to DB.

    Dump raw data as .csv, but write in a new file directory so we know which
    writes were from the legacy syncs and which are from this iteration of
    the pipeline.
    """
    #legacy_sync_data: list[dict] = convert_legacy_sync_data()
    #legacy_messaging_data: list[dict] = convert_legacy_messaging_data() # noqa
    legacy_sync_data = []
    legacy_messaging_data = []
    legacy_messages_received_data: list[dict] = convert_legacy_messages_received_data() # noqa
    list_of_legacy_data_to_write: list[dict] = (
        legacy_sync_data + legacy_messaging_data
        + legacy_messages_received_data
    )
    for table_info_map in list_of_legacy_data_to_write:
        table_name = table_info_map["table_name"]
        df = table_info_map["df"]
        upsert_bool = table_info_map.get("upsert", False)
        if len(df) > 0:
            print(f"Dumping legacy data to {table_name}...")
            write_df_to_database(df=df, table_name=table_name, upsert=upsert_bool)
            dump_df_to_csv(df=df, table_name=f"legacy_{table_name}")
            print(f"Finished dumping legacy data to {table_name}")
        else:
            print(f"No legacy data to dump to {table_name}.")


if __name__ == "__main__":
    convert_legacy_data()
