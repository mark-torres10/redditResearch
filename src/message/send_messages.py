"""Given a list of users to message, send them a message.

Example usage:
    python send_messages.py 2023-03-20_1438
"""
import os
import re
import sys
import time


from lib.reddit import init_api_access
from lib.redditLogging import RedditLogger
from message import constants, manage_previously_messaged_users

import pandas as pd
import praw

from message import helper

logger = RedditLogger(name=__name__)

PREVIOUSLY_MESSAGED_USERS_FILENAME = os.path.join(
    constants.MESSAGES_ROOT_PATH, constants.ALL_MESSAGED_USERS_FILENAME
)

def create_message_body(
    name: str, date: str, post: str, subreddit: str, permalink: str
) -> str:
    """Creates message body."""
    return helper.AUTHOR_DM_SCRIPT.format(
        name=name, date=date, post=post, subreddit=subreddit,
        permalink=permalink
    )


def send_message(
    api: praw.Reddit, user: str, subject: str, body: str
) -> None:
    """Send a message to a user."""
    api.redditor(user).message(subject, body)


def catch_rate_limit_and_sleep(e: praw.exceptions.RedditAPIException) -> None:
    """Catch rate limit exception.
    
    Parses time to wait form the exception string and sleeps for that long.
    Example rate limit exception string:
    "Looks like you've been doing that a lot. Take a break for 2 minutes before
    trying again."
    """
    if e.error_type == "RATELIMIT":
        rate_limit_message = e.message
        number = re.search(r'\b(\d+)\b', rate_limit_message)
        try:
            wait_time_minutes = int(number.group(0))
            print(
                f"Hit rate limit, sleeping for {wait_time_minutes} minutes"
            )
            # sleep for wait time, then wait 30 seconds to send message
            time.sleep(wait_time_minutes * 60)
            time.sleep(1 * 30)
        except Exception:
            print(
                "Unable to parse rate limit message {rate_limit_message}".format(
                    rate_limit_message=rate_limit_message
                )
            )
            return
    else:
        return

if __name__ == "__main__":
    # update list of users who have already been messaged.
    manage_previously_messaged_users.dump_all_previously_messaged_users_as_csv() # noqa
    previously_messaged_users_df = pd.read_csv(
        PREVIOUSLY_MESSAGED_USERS_FILENAME
    )
    previously_messaged_author_screen_names_list = (
        previously_messaged_users_df["author_screen_name"].tolist()
    )
    # load data with who to message.
    load_timestamp = sys.argv[1]
    timestamp_dir = os.path.join(
        constants.MESSAGES_ROOT_PATH, load_timestamp
    )
    if not os.path.exists(timestamp_dir):
        logger.error(f"Labeled data timestamp directory {load_timestamp} does not exist")
        sys.exit(1)

    labeled_data_filepath = os.path.join(
        timestamp_dir, constants.WHO_TO_MESSAGE_FILENAME
    )
    labeled_data = pd.read_csv(labeled_data_filepath)
    api = init_api_access()
    has_been_messaged_col = []

    for row_tuple in labeled_data.iterrows():
        idx, row = row_tuple

        id = row["id"]
        author_screen_name = row["author_screen_name"]
        body = row["body"]
        permalink = row["permalink"]
        full_link = helper.transform_permalink_to_link(permalink)

        should_message_flag = row[constants.TO_MESSAGE_COL]

        if author_screen_name in previously_messaged_author_screen_names_list:
            print(f"Author {author_screen_name} has been messaged before. Skipping...") # noqa
            has_been_messaged_col.append(1)
            continue

        if should_message_flag == 1:
            try:
                message_body = create_message_body(
                    name=author_screen_name,
                    date=row["created_utc_string"],
                    post=body,
                    subreddit=row["subreddit_name_prefixed"],
                    permalink=full_link
                )
                send_message(
                    api=api, user=author_screen_name,
                    subject=helper.AUTHOR_DM_SUBJECT_LINE, body=message_body
                )
                has_been_messaged_col.append(1)
            except Exception as e:
                # catch rate limit, skip this row, and hopefully succeed on
                # messaging the next row
                # TODO: build retry so it tries messaging this row again.
                print(
                    "Unable to message row {row} with id {id} for reason {e}".format( # noqa
                        row=idx, id=id, e=e
                    )
                )
                has_been_messaged_col.append(0)
                catch_rate_limit_and_sleep(e)
                continue
        else:
            has_been_messaged_col.append(0)

    num_to_message_possible = labeled_data[constants.TO_MESSAGE_COL].sum()

    print(
        "Messaged {sum_messaged} out of {sum_possible} total possible".format(
            sum_messaged=sum(has_been_messaged_col),
            sum_possible=num_to_message_possible
        )
    )
    
    labeled_data[constants.HAS_BEEN_MESSAGED_COL] = has_been_messaged_col

    output_filepath = os.path.join(
        constants.MESSAGES_ROOT_PATH, load_timestamp,
        constants.SENT_MESSAGES_FILENAME
    )

    labeled_data.to_csv(output_filepath)

    logger.info(
        f"Done sending messages for data labeled on timestamp {load_timestamp}"
    )
