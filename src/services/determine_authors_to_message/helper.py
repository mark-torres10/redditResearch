import datetime
import random
from typing import List, Optional

import pandas as pd

from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_table_as_df, write_df_to_database
)
from services.message_users.helper import table_fields, table_name

DENYLIST_AUTHORS = ["AutoModerator", "PoliticsModeratorBot"]
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

table_fields = [
    "user_id", "message_status", "last_update_timestamp", "last_update_step",
    "phase", "comment_id", "comment_text", "dm_text", "author_screen_name"
]


def determine_number_of_people_to_message(
    total_num_users: int,
    default_max_num_users_to_message: int,
    max_num_assign_to_message: Optional[int] = None,
    max_ratio_assign_to_message: Optional[float] = None,
) -> int:
    """Determine the number of people to assign to message
    
    By default, we will message an equal number of users with label 0 and label
    1, but we need to determine the number total to message. We can pass in
    several possible configurations. We can't exceed the max number of users to
    message, since this will be determined by (2 * (which label has the fewest users)),
    and we need this limit to strictly message an equal number of users for
    each label. However, we can message fewer people than that as well, based
    on either an absolute number of people to message or a ratio of people to
    message
    """
    if max_num_assign_to_message and max_ratio_assign_to_message:
        raise ValueError(
            "Cannot specify both `max_num_assign_to_message` and "
            "`max_ratio_assign_to_message`."
        )
    max_num_users_to_message = default_max_num_users_to_message
    if max_num_assign_to_message:
        max_num_users_to_message = min(
            max_num_assign_to_message, max_num_users_to_message
        )
    elif max_ratio_assign_to_message:
        max_num_users_to_message = min(
            int(total_num_users * max_ratio_assign_to_message),
            max_num_users_to_message
        )
    return max_num_users_to_message


def balance_posts(
    labels_list: list[int],
    num_people_to_message: int
) -> List[int]:
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

    Args:
        :labels_list: list[int]: list of the labels
    """
    # determine whether the 0s or the 1s is smaller. Assign all those as
    # to message
    num_zeros_assigned = 0
    num_ones_assigned = 0
    max_to_assign_per_label = num_people_to_message // 2

    # shuffle labels inplace
    random.shuffle(labels_list)

    # assign to message
    to_message_lst: list[int] = []

    for label in labels_list:
        if label == 0 and num_zeros_assigned < max_to_assign_per_label:
            to_message_lst.append(1)
            num_zeros_assigned += 1
        elif label == 1 and num_ones_assigned < max_to_assign_per_label:
            to_message_lst.append(1)
            num_ones_assigned += 1
        else:
            to_message_lst.append(0)

    return to_message_lst


def determine_which_posts_to_message(
    labeled_data: pd.DataFrame,
    max_num_assign_to_message: Optional[int] = None,
    max_ratio_assign_to_message: Optional[float] = None
) -> pd.DataFrame:
    """Given a df with labeled data, determine which comments/posts should be
    messaged.
    
    We do this by using a balance strategy (by default, "equal"). In the
    "equal" strategy, we message an equal number of data labeled 0s and 1s.
    This means that the number of 0s and 1s will be set as
    min(num_zeros, num_ones), the minimum count of the two labels. However, we
    can also add a maximum number of users to message, or a maximum ratio of
    users to message.
    """
    if max_num_assign_to_message and max_ratio_assign_to_message:
        raise ValueError(
            "Cannot specify both `max_num_assign_to_message` and "
            "`max_ratio_assign_to_message`."
        )
    label_col: pd.Series = labeled_data[LABEL_COL]
    min_label_count = label_col.value_counts().min()
    default_max_num_users_to_message = 2 * min_label_count
    num_people_to_message = determine_number_of_people_to_message(
        total_num_users=len(label_col),
        default_max_num_users_to_message=default_max_num_users_to_message,
        max_num_assign_to_message=max_num_assign_to_message,
        max_ratio_assign_to_message=max_ratio_assign_to_message
    )
    labels_list = label_col.tolist()
    to_message_list = balance_posts(
        labels_list=labels_list, num_people_to_message=num_people_to_message
    )
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
        name=row["author_screen_name"],
        date=row["created_utc"],
        subreddit=row["subreddit_name_prefixed"],
        post=row["body"],
        permalink=row["permalink"]
    )


def init_user_to_message_status_table() -> None:
    """Create the initial version of the `user_to_message_status` table based
    on the `users` table.
    """
    print(f"Initializing the {table_name} table with data from the `users` table...") # noqa
    default_values = {
        "message_status": "not_messaged",
        "last_update_step": None,
        "last_update_timestamp": None,
        "phase": None,
        "comment_id": None,
        "comment_text": None,
        "dm_text": None
    }
    users_df = load_table_as_df(
        table_name="users",
        select_fields=["id AS user_id", "name AS author_screen_name"],
        where_filter=""
    )
    init_values = [
        {**{key: item for (key, item) in row.items()}, **default_values}
        for _, row in users_df.iterrows()
        if row["author_screen_name"] not in DENYLIST_AUTHORS
    ]
    df = pd.DataFrame(init_values)
    dump_df_to_csv(df=df, table_name=table_name)
    write_df_to_database(df=df, table_name=table_name)
    print(f"Finished initializing the {table_name} table with `users` table.")


def load_pending_author_phase_messages() -> pd.DataFrame:
    """Returns any author_phase users that are pending message but haven't been
    messaged yet."""
    return load_table_as_df(
        table_name=table_name,
        select_fields=["*"],
        where_filter="""
            WHERE phase = 'author' AND message_status = 'pending_message'
        """
    )


# TODO: add maximum "to-message" count, so we only assign up to a certain
# number of people as "to-message", making sure that we have some left for the
# observer phase.
def get_new_author_phase_messages(
    classified_comments_df: pd.DataFrame,
    max_num_assign_to_message: Optional[int] = None,
    max_ratio_assign_to_message: Optional[float] = None
) -> pd.DataFrame:
    """Given a df of classified comments that have not been assigned to the
    auhot phase yet, return a df that has assigned them to the author phase as
    well as added the extra fields needed for sending messages.
    """
    # balance comments (ratio of 1:1 for outrage/not outrage)
    balanced_classified_comments_df = determine_which_posts_to_message(
        labeled_data=classified_comments_df,
        max_num_assign_to_message=max_num_assign_to_message,
        max_ratio_assign_to_message=max_ratio_assign_to_message
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
    balanced_classified_comments_df["last_update_step"] = "determine_authors_to_message" # noqa
    balanced_classified_comments_df["phase"] = "author"
    balanced_classified_comments_df["comment_id"] = (
        balanced_classified_comments_df["id"]
    )
    balanced_classified_comments_df["comment_text"] = (
        balanced_classified_comments_df["body"]
    )
    balanced_classified_comments_df["dm_text"] = [
        create_author_phase_message(row)
        for _, row in balanced_classified_comments_df.iterrows()
    ]
    return balanced_classified_comments_df


def create_author_phase_messages(
    user_to_message_status_df: pd.DataFrame
) -> list[dict]:
    user_to_message_list = [
        {
            "author_screen_name": author_screen_name,
            "user_id": user_id,
            "comment_id": comment_id,
            "comment_text": comment_text,
            "message_subject": AUTHOR_DM_SUBJECT_LINE,
            "message_body": direct_message,
            "phase": "author"
        }
        for (
            user_id, comment_id, comment_text, author_screen_name,
            direct_message
        )
        in zip(
            user_to_message_status_df["user_id"],
            user_to_message_status_df["comment_id"],
            user_to_message_status_df["comment_text"],
            user_to_message_status_df["author_screen_name"],
            user_to_message_status_df["dm_text"]
        )
    ]
    return user_to_message_list


def determine_who_to_message(
    batch_size: Optional[int] = None,
    use_only_pending_author_phase_messages: Optional[bool] = False,
    max_num_assign_to_message: Optional[int] = None,
    max_ratio_assign_to_message: Optional[float] = None
) -> list[dict]:
    if use_only_pending_author_phase_messages:
        print(f"Using only pending author phase messages...")
        user_to_message_status_df = load_pending_author_phase_messages()
        if batch_size:
            print(f"Limiting the number of users to message to {batch_size}...") # noqa
            user_to_message_status_df = user_to_message_status_df.head(batch_size) # noqa
        user_to_message_list = create_author_phase_messages(
            user_to_message_status_df
        )
        return user_to_message_list

    user_to_message_status_table_exists = check_if_table_exists(table_name)
    if not user_to_message_status_table_exists:
        print(f"{table_name} doesn't exist. Creating new table.")
        init_user_to_message_status_table()
    # load classified comments, but filter out comments whose authors have not
    # been messaged yet.
    select_fields = ["*"]
    where_filter = """
        WHERE author_id NOT IN (
            SELECT
                user_id
            FROM user_to_message_status
            WHERE message_status != 'not_messaged'
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

    # assign comments and users to author phase and add any fields necessary
    # for sending the messages
    balanced_classified_comments_df = get_new_author_phase_messages(
        classified_comments_df=classified_comments_df,
        max_num_assign_to_message=max_num_assign_to_message,
        max_ratio_assign_to_message=max_ratio_assign_to_message
    )

    # update the user_to_message_status table with the updated message status
    # of each user. At this stage, we have taken comments whose authors have
    # not been messaged yet and then updated the status of those authors, if we
    # indeed have assigned them to be messaged.
    user_to_message_status_df = balanced_classified_comments_df[table_fields] # noqa

    # add any author-phase users that are pending message but haven't been
    # messaged yet.
    users_pending_message_df = load_pending_author_phase_messages()

    if batch_size:
        print(f"Limiting the number of users to message to {batch_size}...") # noqa
        users_pending_message_df = users_pending_message_df.head(batch_size)

    if len(users_pending_message_df) > 0:
        print(f"Appending {len(users_pending_message_df)} users, with pending messages, to the list of users to message...") # noqa
        user_to_message_status_df = pd.concat(
            [user_to_message_status_df, users_pending_message_df]
        )

    # dump to .csv, upsert to DB (so that, for example, users who were not DMed
    # before will have their statuses updated.)
    dump_df_to_csv(df=user_to_message_status_df, table_name=table_name)
    write_df_to_database(
        df=user_to_message_status_df, table_name=table_name, upsert=True
    )

    number_of_new_users_to_message = (
        user_to_message_status_df["message_status"] == "pending_message"
    ).sum()
    print(f"Marked {number_of_new_users_to_message} new users as pending message.")  # noqa

    user_to_message_list = create_author_phase_messages(
        user_to_message_status_df
    )
    return user_to_message_list
