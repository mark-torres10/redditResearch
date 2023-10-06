import datetime
import random
from typing import List, Optional

import pandas as pd

from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_table_as_df, write_df_to_database
)

DENYLIST_AUTHORS = ["AutoModerator"]
LABEL_COL = "label"
TO_MESSAGE_COL = "to_message"
AUTHOR_DM_SUBJECT_LINE = "Yale Researchers Looking to Learn More About Your Beliefs"
AUTHOR_DM_SCRIPT = """
    Hi {name},

    My research group is interested in how people express themselves on social
    media. Would you like to answer a few questions to help us with our
    research? Your response will remain anonymous. 

    You posted the following message on {date} in the {subreddit} subreddit:

    {post}

    (link {permalink})

    Take a moment to think about what was happening at the time you posted.
    Think about who you were interacting with online, and what you were reading
    about on Reddit. Please answer the following regarding how you felt at the
    moment you posted:

    1. How outraged did you feel on a 1-7 scale? (1 = not at all, 4 = somewhat, 7 = very)
    2. How happy did you feel on a 1-7 scale? (1 = not at all, 4 = somewhat, 7 = very)
    3. How outraged do you think your message will appear to others (1 = not at all, 4 = somewhat, 7 = very)
    4. RIGHT NOW how outraged are you about the topic you posted about (1 = not at all, 4 = somewhat, 7 = very)

    You can simply respond with one answer per line such as:
    5
    1
    3
    4
""" # noqa

AUTHOR_PHASE_MESSAGE_IDENTIFIER_STRING = (
    "Take a moment to think about what was happening at the time you posted."
)

table_name = "user_to_message_status"
table_fields = [
    "user_id", "message_status", "last_update_timestamp", "phase",
    "comment_id", "comment_text", "dm_text", "author_screen_name"
]


def balance_posts(labels: pd.Series, min_count: int) -> List[int]:
    """Balance which of the rows in the `labels` series to label.
    
    This will return a binary list of 0s and 1s where:
        0 = do not message
        1 = message
    Such that if this list is zipped against the `labels` series, then the
    number of rows in the "labels" series with a "labels" value of 0
    that have a "to_label" value of 1 equal the number of rows in the "labels"
    series with a "labels" value of 1 that have a "to_label" value of 1.

    This means that we should message an equal number of the rows that have
    labels = 0 as we do rows that have labels = 1.
    """
    # determine whether the 0s or the 1s is smaller. Assign all those as
    # to message
    labels_list = labels.tolist()
    min_label = 1 if sum(labels_list) == min_count else 0
    
    to_message_lst = [0] * len(labels_list)

    max_label_idx_lst = []

    # all the rows with the min_label should be messaged.
    for idx, label in enumerate(labels_list):
        if label == min_label:
            to_message_lst[idx] = 1
        else:
            max_label_idx_lst.append(idx)

    # shuffle the max_label_idx_lst, take the first [:min_count] labels
    random.shuffle(max_label_idx_lst)
    max_labels_idxs_to_message = max_label_idx_lst[:min_count]
    for idx in max_labels_idxs_to_message:
        to_message_lst[idx] = 1
    
    return to_message_lst


def determine_which_posts_to_message(
    labeled_data: pd.DataFrame,
    balance_strategy: Optional[str] = "equal"
) -> pd.DataFrame:
    """Given a df with labeled data, determine which comments/posts should be
    messaged.
    
    We do this by using a balance strategy (by default, "equal"). In the
    "equal" strategy, we message an equal number of data labeled 0s and 1s.
    This means that the number of 0s and 1s will be set as
    min(num_zeros, num_ones), the minimum count of the two labels.
    """
    label_col = labeled_data[LABEL_COL]
    if balance_strategy == "equal":
        min_count = label_col.value_counts().min()
    to_message_list = balance_posts(label_col, min_count)
    labeled_data[TO_MESSAGE_COL] = to_message_list

    print(
        f"""
            Number of posts: {len(to_message_list)}\n
            Number to DM: {sum(to_message_list)}
        """
    )

    return labeled_data


def create_author_phase_message(row: pd.Series) -> str:
    return AUTHOR_DM_SCRIPT.format(
        name=row["author"],
        date=row["created_utc"],
        subreddit=row["subreddit_name_prefixed"],
        post=row["body"],
        permalink=row["permalink"]
    )


def init_user_to_message_status_table() -> None:
    """Create the initial version of the `user_to_message_status` table based
    on the `users` table.
    """
    print(f"Initializing the {table_name} table with `users` table...")
    default_values = {
        "message_status": "not_messaged",
        "last_update_timestamp": None,
        "phase": None,
        "comment_id": None,
        "comment_text": None,
        "dm_text": None
    }
    users_df = load_table_as_df(
        table_name="users",
        select_fields=["id", "name"],
        where_filter=""
    )
    init_values = [
        {**row, **default_values}
        for _, row in users_df.iterrows()
        if row["name"] not in DENYLIST_AUTHORS
    ]
    df = pd.DataFrame(init_values)
    dump_df_to_csv(df=df, table_name=table_name)
    write_df_to_database(df=df, table_name=table_name)
    print(f"Finished initializing the {table_name} table with `users` table.")


def determine_who_to_message() -> list[dict]:
    # get previously messaged users, if table exists.
    user_to_message_status_table_exists = check_if_table_exists(table_name)
    if not user_to_message_status_table_exists:
        init_user_to_message_status_table()
    # load classified comments, but filter out comments whose authors have not
    # been messaged yet.
    select_fields = ["*"]
    where_filter = """
        WHERE author_id NOT IN (
            SELECT
                user_id
            FROM user_to_message_status
            WHERE message_status = 'messaged'
        )
    """ if user_to_message_status_table_exists else ""
    classified_comments_df = load_table_as_df(
        table_name="classified_comments",
        select_fields=select_fields,
        where_filter=where_filter
    )
    # deduplicate comments we only have 1 comment per author_id.
    classified_comments_df = classified_comments_df.drop_duplicates(
        subset=["author_id"]
    )
    # balance comments (ratio of 1:1 for outrage/not outrage)
    balanced_classified_comments_df = determine_which_posts_to_message(
        labeled_data=classified_comments_df, balance_strategy="equal"
    )
    # add fields to table to match desired user_to_message_status table format.
    balanced_classified_comments_df["user_id"] = (
        balanced_classified_comments_df["author_id"]
    )
    # new column of "message_status", mark as "not_messaged" if the value of
    # the to_message column is 0, else "pending_message" if 1.
    balanced_classified_comments_df["message_status"] = (
        balanced_classified_comments_df.apply(
            lambda row: "not_messaged" if row["to_message"] == 0 else "pending_message", # noqa
            axis=1
        )
    )
    balanced_classified_comments_df["last_update_timestamp"] = (
        datetime.datetime.utcnow().isoformat()
    )
    balanced_classified_comments_df["phase"] = "author"
    balanced_classified_comments_df["comment_id"] = (
        balanced_classified_comments_df["id"]
    )
    balanced_classified_comments_df["comment_text"] = (
        balanced_classified_comments_df["body"]
    )
    balanced_classified_comments_df["dm_text"] = [
        create_author_phase_message(row)
        if row["message_status"] == "pending_message"
        else "NOT MESSAGED"
        for _, row in balanced_classified_comments_df.iterrows()
    ]
    
    # update the user_to_message_status table with the updated message status
    # of each user. At this stage, we have taken comments whose authors have
    # not been messaged yet and then updated the status of those authors, if we
    # indeed have assigned them to be messaged.
    user_to_message_status_df = balanced_classified_comments_df[table_fields] # noqa

    # dump to .csv, upsert to DB (so that, for example, users who were not DMed
    # before will have their statuses updated.)
    dump_df_to_csv(
        df=user_to_message_status_df,
        table_name=table_name
    )
    write_df_to_database(
        df=user_to_message_status_df, table_name=table_name
    )

    number_of_new_users_to_message = (
        user_to_message_status_df["message_status"] == "pending_message"
    ).sum()
    print(f"Marked {number_of_new_users_to_message} new users as pending message.")  # noqa
    
    # pass on payloads to messaging service.
    user_to_message_list = [
        {
            "author_screen_name": author_screen_name,
            "message_subject": AUTHOR_DM_SUBJECT_LINE,
            "message_body": direct_message
        }
        for (author_screen_name, direct_message)
        in zip(
            user_to_message_status_df["author_screen_name"],
            user_to_message_status_df["dm_text"]
        )
    ]
    return user_to_message_list
