import pandas as pd

from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    load_query_as_df, load_table_as_df,
    return_statuses_of_user_to_message_status_table, write_df_to_database
)
from lib.helper import (
    BASE_REDDIT_URL, convert_utc_timestamp_to_datetime_string
)

DEFAULT_RECENCY_FILTER = ""
DEFAULT_NUMBER_OF_OBSERVERS_PER_COMMENT = 10
DEFAULT_OBSERVER_LIMIT = 200
DEFAULT_COMMENT_LIMIT = 20
OBSERVER_DM_SUBJECT_LINE = "Yale Researchers Looking to Learn More About Your Beliefs" # noqa
OBSERVER_DM_SCRIPT = """
Hi {name},

I'm a researcher at Yale University, and my research group is interested in
how people express themselves on social media. Would you like to answer a
few questions to help us with our research? Your response will remain
anonymous.

The following message was posted in the {subreddit_name} subreddit on {date}:

{post}

(link {permalink})

Please answer the following:

1. How outraged did you think the message author was on a 1-7 scale?
(1 = not at all, 4 = somewhat, 7 = very)

2. How happy did you think the message author was on a 1-7?
(1 = not at all, 4 = somewhat, 7 = very)

You can simply respond with one answer per line such as:
5
1
""" # noqa
OBSERVER_PHASE_MESSAGE_IDENTIFIER_STRING = (
    "How outraged did you think the message author was on a 1-7 scale"
)

table_name = "comment_to_observer_map"


def map_comments_to_observers(
    valid_comments_df: pd.DataFrame, valid_observers_df: pd.DataFrame
) -> pd.DataFrame:
    """Maps comments to observers.
    
    Returns df with two columns, both PKs: (comment_id, user_id

    There should never be a case where we map a comment to a user who was in
    the author phase (assuming that the `get_valid_possible_observers` service
    works as intended), so we should only be DMing new people who we assigned
    to be a part of the observer phase.
    """
    comment_to_observer_map: dict[str, list[str]] ={} # should be 1 to many, 1 comment to list of observers # noqa
    
    comment_ids = valid_comments_df["comment_id"].tolist()
    observer_ids = valid_observers_df["user_id"].tolist()

    for comment_id in comment_ids:
        comment_to_observer_map[comment_id] = []
    
    # add observers to comments in a round robin fashion
    idx = 0
    while idx < len(observer_ids):
        for comment_id in comment_ids:
            if idx >= len(observer_ids):
                break
            comment_to_observer_map[comment_id].append(observer_ids[idx])
            idx += 1

    comment_observer_tuples = [
        (comment_id, user_id)
        for comment_id, observer_ids in comment_to_observer_map.items()
        for user_id in observer_ids
    ]
    df = pd.DataFrame(
        comment_observer_tuples, columns=["comment_id", "user_id"]
    )
    return df


def create_observer_phase_message(row: pd.Series) -> str:
    return OBSERVER_DM_SCRIPT.format(
        name=row["author_screen_name"],
        date=convert_utc_timestamp_to_datetime_string(
            row["created_utc"]
        ),
        subreddit=row["subreddit_name_prefixed"],
        post=row["comment_text"],
        permalink="".join([BASE_REDDIT_URL, row["permalink"]])
    )


def match_observers_to_comments() -> list[dict]:
    # TODO: should a recency filter be added at some point?
    # TODO: maybe sort by comment date and just get the most recent?
    valid_comments_df = load_table_as_df(
        table_name="comments_available_to_evaluate_for_observer_phase",
        select_fields=["comment_id"],
        where_filter="",
        order_by_clause="ORDER BY created_utc DESC",
        limit_clause=f"LIMIT {DEFAULT_COMMENT_LIMIT}"
    )
    valid_observers_df = load_table_as_df(
        table_name="user_to_message_status",
        select_fields=["user_id"],
        where_filter="WHERE message_status = 'pending_message' AND phase = 'observer'" # noqa
    )
    print(f"Mapping {len(valid_comments_df)} comments to {len(valid_observers_df)} observers") # noqa

    # map comments to observers
    comment_to_observer_df = map_comments_to_observers(
        valid_comments_df=valid_comments_df,
        valid_observers_df=valid_observers_df
    )
    dump_df_to_csv(df=comment_to_observer_df, table_name=table_name)
    write_df_to_database(df=comment_to_observer_df, table_name=table_name)

    # after mapping comments to observers, hydrate with additional comment and
    # user information so that the messages can be sent.
    hydrated_df_query = f"""
        SELECT
            u.user_id, u.message_status, u.phase, c.id as comment_id,
            c.body as comment_text, c.author as author_screen_name,
            c.created_utc, c.subreddit_name_prefixed, c.permalink
        FROM {table_name}
        INNER JOIN user_to_message_status u
        ON {table_name}.user_id = u.user_id
        INNER JOIN comment c
        ON {table_name}.comment_id = c.id
        WHERE u.message_status = 'pending_message'
        AND u.phase = 'observer'
    """
    hydrated_observer_phase_df = load_query_as_df(query=hydrated_df_query)
    hydrated_observer_phase_df["dm_text"] = [
        create_observer_phase_message(row)
        for _, row in hydrated_observer_phase_df.iterrows()
    ]
    # dump to .csv, upsert to DB (so that, for example, users who were not DMed
    # before will have their statuses updated.)
    dump_df_to_csv(
        df=hydrated_observer_phase_df,
        table_name="user_to_message_status"
    )
    write_df_to_database(
        df=hydrated_observer_phase_df,
        table_name="user_to_message_status",
        upsert=True
    )
    return_statuses_of_user_to_message_status_table()
    user_to_message_list = [
        {
            "author_screen_name": author_screen_name,
            "user_id": user_id,
            "comment_id": comment_id,
            "comment_text": comment_text,
            "message_subject": OBSERVER_DM_SUBJECT_LINE,
            "message_body": direct_message,
            "phase": "observer"
        }
        for (
            user_id, comment_id, comment_text, author_screen_name,
            direct_message
        )
        in zip(
            hydrated_observer_phase_df["user_id"],
            hydrated_observer_phase_df["comment_id"],
            hydrated_observer_phase_df["comment_text"],
            hydrated_observer_phase_df["author_screen_name"],
            hydrated_observer_phase_df["dm_text"]
        )
    ]
    return user_to_message_list
