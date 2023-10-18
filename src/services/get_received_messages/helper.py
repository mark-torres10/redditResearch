"""Loads messages from inbox.

Manages message retrieval for both author and observer phase.
"""
import pandas as pd
from praw.models.reddit.message import Message, SubredditMessage

from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_table_as_df, write_df_to_database
)
from lib.helper import add_enrichment_fields, is_json_serializable
from lib.reddit import init_api_access
from services.sync_single_subreddit.transformations import (
    object_specific_enrichments
)

table_name = "messages_received"
api = init_api_access()

table_fields = [
    "id", "author_id", "author_screen_name", "phase", "body", "created_utc",
    "created_utc_string", "comment_id", "comment_text", "dm_text",
    "synctimestamp"
]
message_columns_to_extract = [
    "id", "author_id", "body", "created_utc", "created_utc_string",
    "synctimestamp"
]
user_to_message_status_select_fields = [
    "user_id", "author_screen_name", "phase", "comment_id", "comment_text",
    "dm_text"
]


def is_valid_message_type(message) -> bool:
    """We only want messages that we get directly from other users. We don't
    want messages that come from a moderator or is an automated message
    from a subreddit (class == praw.models.reddit.message.SubredditMessage).
    We want messages from users (class == praw.models.reddit.message.Message).
    """
    return (
        isinstance(message, Message)
        and not isinstance(message, SubredditMessage)
    )


def get_message_data(message: Message) -> dict:
    """Given a `message` object, get the data that we want from it."""
    message_dict = {}
    print(f"Getting information for message with id={message.id}")
    for field, value in message.__dict__.items():
        if is_json_serializable(value):
            message_dict[field] = value
    # synctimestamp, created_utc_string are added to the message object here.
    message_dict = add_enrichment_fields(message_dict)
    # author_id is added here. It exists as author_fullname but we actually
    # need it as an ID.
    object_specific_enrichments_dict = object_specific_enrichments(message)
    message_dict = {**message_dict, **object_specific_enrichments_dict}
    return message_dict


# TODO: figure out how to do time-based filter so that we only load messages
# that are new since the last time we ran this script.
# NOTE: looks like it's possible to pass in an id (e.g., `t4_1y1185x`) and
# get any messages received after that one?
# https://www.reddit.com/dev/api/#GET_message_inbox
def get_messages() -> pd.DataFrame:
    """Get DMs that we received on Reddit.
    
    We only want DMs that we receive in response to our initial author/observer
    phase DMs. We don't want DMs that are automated moderator messages, for
    example.

    We don't collapse messages and dedupe across author_id since in the case
    where we get multiple DMs from a user, we don't know which one to use. This
    will get resolved during annotation.
    """
    all_messages_received: list[Message] = [
        msg for msg in api.inbox.messages(limit=None)
    ]
    messages_received = [
        msg.replies[0] for msg in all_messages_received
        if len(msg.replies) > 0 and is_valid_message_type(msg)
    ]
    message_dicts_list = [get_message_data(msg) for msg in messages_received]
    # drop duplicates on "id" field (shouldn't happen, but edge case where this
    # can happen sometime and duplicate messages are returned via API)
    seen_ids = set()
    deduped_message_dicts_list: list[dict] = []
    for message_dict in message_dicts_list:
        if message_dict["id"] not in seen_ids:
            seen_ids.add(message_dict["id"])
            deduped_message_dicts_list.append(message_dict)
    messages_df = pd.DataFrame(deduped_message_dicts_list)
    print(f"Collected {len(messages_df)} messages.")
    return messages_df


def handle_received_messages() -> None:
    # get DMs received.
    messages_received_df = get_messages()

    # check table of DMs already received, filter out DMs received
    # by that table.
    messages_received_table_exists = check_if_table_exists(table_name)
    if messages_received_table_exists:
        print("Filtering received messages by timestamp of the most recent data collection run.") # noqa
        select_fields = ["MAX(synctimestamp)"]
        where_filter = ""
        most_recent_timestamp = load_table_as_df(
            table_name=table_name,
            select_fields=select_fields,
            where_filter=where_filter
        )
        most_recent_timestamp = most_recent_timestamp.iloc[0][0]
        # filter dms_received by most_recent_timestamp. If the DM was received
        # more recently than the last time that we appended data to the
        # messages_received table, then we want to keep it.
        created_utc_list = messages_received_df["created_utc"].tolist()
        created_utc_isoformat_list = [
            pd.to_datetime(utc_float, unit="s").isoformat()
            for utc_float in created_utc_list
        ]
        filter_list: list[bool] = [
            isoformat_timestamp > most_recent_timestamp
            for isoformat_timestamp in created_utc_isoformat_list
        ]
        messages_received_df = messages_received_df[filter_list]
        if len(messages_received_df) == 0:
            print("No new messages received. Exiting.")
            return

    # hydrate the received messages with information on the DM that was
    # initially sent. Join the DMs with the information in the
    # `user_to_message_status` table, so that for each DM we get we also get
    # information about the original message that was sent.
    messages_received_df = messages_received_df[message_columns_to_extract]
    author_ids_string = ', '.join(
        f"'{author}'" for author in messages_received_df["author_id"]
    )
    user_to_message_status_where_filter = f"WHERE user_id IN ({author_ids_string})" # noqa
    user_to_message_status_df = load_table_as_df(
        table_name="user_to_message_status",
        select_fields=user_to_message_status_select_fields,
        where_filter=user_to_message_status_where_filter
    )

    hydrated_dms_received = messages_received_df.merge(
        user_to_message_status_df,
        how="inner",
        left_on="author_id",
        right_on="user_id"
    )

    # warn if there are author_id values in messages_received_df that aren't
    # in the hydrated_dms_received df.
    messages_received_author_ids = set(messages_received_df["author_id"])
    hydrated_dms_received_author_ids = set(hydrated_dms_received["author_id"])
    author_ids_not_in_hydrated_dms_received = (
        messages_received_author_ids - hydrated_dms_received_author_ids
    )
    if author_ids_not_in_hydrated_dms_received:
        print(
            f"WARNING: There are {len(author_ids_not_in_hydrated_dms_received)} "
            "author_ids in messages_received_df that are not in the "
            "hydrated_dms_received df. These should be equal since any inbound"
            "messages that we get should only be from users who we DMed first."
        )
        print("These author_ids are:")
        print(author_ids_not_in_hydrated_dms_received)
    hydrated_dms_received = hydrated_dms_received[table_fields]
    # write to DB.
    dump_df_to_csv(df=hydrated_dms_received, table_name=table_name)
    write_df_to_database(df=hydrated_dms_received, table_name=table_name)

    print("Completed getting received messages.")

if __name__ == "__main__":
    handle_received_messages()
