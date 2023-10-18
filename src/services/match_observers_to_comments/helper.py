import datetime

import pandas as pd

from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_query_as_df, load_table_as_df,
    return_statuses_of_user_to_message_status_table, write_df_to_database
)
from lib.helper import (
    BASE_REDDIT_URL, convert_utc_timestamp_to_datetime_string
)
from services.match_observers_to_comments.constants import (
    DEFAULT_COMMENT_LIMIT, DEFAULT_NUMBER_OF_OBSERVERS_PER_COMMENT,
    DEFAULT_OBSERVER_LIMIT, OBSERVER_DM_SCRIPT, OBSERVER_DM_SUBJECT_LINE,
    table_name
)
from services.message_users.constants import table_fields


def map_observers_to_comments(
    valid_comments_df: pd.DataFrame,
    valid_observers_df: pd.DataFrame,
    num_comments_to_map: int,
    num_observers_to_map: int
) -> pd.DataFrame:
    """Maps observers to comments.
    
    Returns df with two columns, both PKs: (comment_id, user_id)

    There should never be a case where we map a comment to a user who was in
    the author phase (assuming that the `get_valid_possible_observers` service
    works as intended), so we should only be DMing new people who we assigned
    to be a part of the observer phase.

    `num_observers_to_map` is going to be equal to `num_comments_to_map` times
    the number of observers we want per comment.
    """
    # should be 1 to many, 1 comment to list of observers # noqa    
    comment_ids = valid_comments_df["comment_id"].tolist()[:num_comments_to_map] # noqa
    observer_ids = valid_observers_df["user_id"].tolist()[:num_observers_to_map] # noqa
    print(f"Mapping {len(comment_ids)} comments with {len(observer_ids)} observers in a round-robin fashion") # noqa
    comment_to_observer_map: dict[str, list[str]] = {
        comment_id: []
        for comment_id in comment_ids
    }
    
    # add observers to comments in a round robin fashion
    idx = 0
    while idx < num_observers_to_map:
        for comment_id in comment_ids:
            if idx >= num_observers_to_map:
                break
            comment_to_observer_map[comment_id].append(observer_ids[idx])
            idx += 1

    # unpack comment: list of observers to [(comment, observer)] tuples
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


def create_observer_phase_payloads(user_to_message_status_df: pd.DataFrame) -> list[dict]: # noqa
    """Creates payloads for observer phase messages.

    Returns list of dicts, each dict is a payload for the message_users
    service.
    """
    user_to_message_list = []
    for _, row in user_to_message_status_df.iterrows():
        user_to_message_list.append({
            "author_screen_name": row["author_screen_name"],
            "user_id": row["user_id"],
            "comment_id": row["comment_id"],
            "comment_text": row["comment_text"],
            "message_subject": OBSERVER_DM_SUBJECT_LINE,
            "message_body": row["dm_text"],
            "phase": "observer"
        })
    return user_to_message_list


# TODO: should a recency filter be added at some point?
# TODO: maybe sort by comment date and just get the most recent?
def match_observers_to_comments() -> None:
    # grab comments to match to observers. Don't include any comments or any
    # observers who already exist in the comment_to_observer_map table, since
    # these comments and observers have already been matched accordingly.
    comment_to_observer_table_exists =  check_if_table_exists(table_name)
    comments_where_filter = f"""
        WHERE comment_id NOT IN (
            SELECT
                comment_id
            FROM {table_name}
        )
    """ if comment_to_observer_table_exists else ""
    observers_where_subfilter = f"""
        AND user_id NOT IN (
            SELECT
                user_id
            FROM {table_name}
        )
    """ if comment_to_observer_table_exists else ""
    observers_where_filter = f"""
        WHERE
            message_status = 'pending_message'
            AND phase = 'observer'
            {observers_where_subfilter}
    """
    valid_comments_df = load_table_as_df(
        table_name="comments_available_to_evaluate_for_observer_phase",
        select_fields=["comment_id"],
        where_filter=comments_where_filter,
        order_by_clause="ORDER BY comment_id ASC", # added to make sure we get same comments each time
        limit_clause=f"LIMIT {DEFAULT_COMMENT_LIMIT}"
    )
    valid_observers_df = load_table_as_df(
        table_name="user_to_message_status",
        select_fields=["user_id"],
        where_filter=observers_where_filter,
        order_by_clause="ORDER BY user_id ASC", # added to make sure we get same observers each time
        limit_clause=f"LIMIT {DEFAULT_OBSERVER_LIMIT}"
    )

    num_comments = len(valid_comments_df)
    num_observers = len(valid_observers_df)
    if num_comments == 0 or num_observers == 0:
        if num_comments == 0:
            print("No comments to map, skipping mapping...")
        if num_observers == 0:
            print("No observers to map, skipping mapping...")
    else:
        # map comments to observers. We want to take the comments that we have
        # and assign a fixed number of observers per comment. This assumes that
        # we have many more observers than we do comments to observe, which
        # will be the case.
        num_comments_to_map = min(
            num_comments, DEFAULT_COMMENT_LIMIT
        )
        num_observers_to_map = num_comments_to_map * DEFAULT_NUMBER_OF_OBSERVERS_PER_COMMENT # noqa
        print(f"Mapping {num_observers_to_map} observers to {num_comments_to_map} comments...") # noqa
        comment_to_observer_df = map_observers_to_comments(
            valid_comments_df=valid_comments_df,
            valid_observers_df=valid_observers_df,
            num_comments_to_map=num_comments_to_map,
            num_observers_to_map=num_observers_to_map
        )
        dump_df_to_csv(df=comment_to_observer_df, table_name=table_name)
        write_df_to_database(df=comment_to_observer_df, table_name=table_name)

    # after mapping comments to observers, hydrate with additional comment and
    # user information so that the messages can be sent.
    hydrated_df_query = f"""
        SELECT
            u.user_id, u.message_status, u.phase, c.id as comment_id,
            c.body as comment_text, c.author_screen_name as author_screen_name,
            c.created_utc, c.subreddit_name_prefixed, c.permalink
        FROM {table_name} t
        INNER JOIN user_to_message_status u
        ON t.user_id = u.user_id
        INNER JOIN comments c
        ON t.comment_id = c.id
        WHERE u.message_status = 'pending_message'
        AND u.phase = 'observer'
    """
    hydrated_observer_phase_df = load_query_as_df(query=hydrated_df_query)
    hydrated_observer_phase_df["last_update_timestamp"] = (
        datetime.datetime.utcnow().isoformat()
    )
    hydrated_observer_phase_df["last_update_step"] = "match_observers_to_comments" # noqa
    hydrated_observer_phase_df["dm_text"] = [
        create_observer_phase_message(row)
        for _, row in hydrated_observer_phase_df.iterrows()
    ]
    # TODO: check dm_text
    user_to_message_status_df = hydrated_observer_phase_df[table_fields]
    # dump to .csv, upsert to DB (so that, for example, users who were not DMed
    # before will have their statuses updated.)
    dump_df_to_csv(
        df=user_to_message_status_df,
        table_name="user_to_message_status"
    )
    write_df_to_database(
        df=user_to_message_status_df,
        table_name="user_to_message_status",
        upsert=True
    )
    return_statuses_of_user_to_message_status_table()

    number_of_new_users_to_message = (
        user_to_message_status_df["message_status"] == "pending_message"
    ).sum()
    print(f"Marked {number_of_new_users_to_message} users as pending message for observer phase.")  # noqa
    print("Completed matching observers to comments.")
