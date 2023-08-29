"""Given a list of users to message, send them a message.

Functionality depends on whether this is used for the author or for the
observer phase of the project.
"""
import argparse
import os
import re
import sys
import time
from typing import List

from get_responses import (
    constants as observer_constants,
    send_messages as observer_send_messages
)
from lib.helper import transform_permalink_to_link
from lib.reddit import init_api_access
from lib.redditLogging import RedditLogger
from message import (
    constants as author_constants,
    helper,
    manage_previously_messaged_users,
)

import pandas as pd
import praw


logger = RedditLogger(name=__name__)
api = init_api_access()


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


def send_author_phase_dms(timestamp: str) -> None:
    # update list of users who have already been messaged.
    manage_previously_messaged_users.dump_all_previously_messaged_users_as_csv() # noqa
    previously_messaged_users_df = pd.read_csv(
        os.path.join(
            author_constants.MESSAGES_ROOT_PATH,
            author_constants.ALL_MESSAGED_USERS_FILENAME
        )
    )
    previously_messaged_author_screen_names_list: List = (
        previously_messaged_users_df["author_screen_name"].tolist()
    )
    # load data with who to message.
    timestamp_dir = os.path.join(
        author_constants.MESSAGES_ROOT_PATH, timestamp
    )
    if not os.path.exists(timestamp_dir):
        logger.error(
            f"Labeled data timestamp directory {timestamp} does not exist"
        )
        sys.exit(1)

    labeled_data_filepath = os.path.join(
        timestamp_dir, author_constants.WHO_TO_MESSAGE_FILENAME
    )

    labeled_data: pd.DataFrame = pd.read_csv(labeled_data_filepath)
    has_been_messaged_col = []

    for row_tuple in labeled_data.iterrows():
        idx, row = row_tuple

        id = row["id"]
        author_screen_name = row["author_screen_name"]
        body = row["body"]
        permalink = row["permalink"]
        full_link = transform_permalink_to_link(permalink)

        should_message_flag = row[author_constants.TO_MESSAGE_COL]

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

    num_to_message_possible = (
        labeled_data[author_constants.TO_MESSAGE_COL].sum()
    )

    print(
        "Messaged {sum_messaged} out of {sum_possible} total possible".format(
            sum_messaged=sum(has_been_messaged_col),
            sum_possible=num_to_message_possible
        )
    )
    
    labeled_data[author_constants.HAS_BEEN_MESSAGED_COL] = has_been_messaged_col # noqa

    output_filepath = os.path.join(
        author_constants.MESSAGES_ROOT_PATH, timestamp,
        author_constants.SENT_MESSAGES_FILENAME
    )

    labeled_data.to_csv(output_filepath)

    logger.info(
        f"Done sending messages for data labeled on timestamp {timestamp}"
    )


def send_observer_phase_dms(timestamp: str, subreddit: str) -> None:
    try:
        if subreddit in observer_constants.SUBREDDITS_ROOT_PATH:
            try:
                observer_send_messages.message_observers_for_subreddit(
                    api=api, subreddit_prefixed=subreddit, timestamp=timestamp
                )
            except Exception as e:
                print(e)
        elif subreddit == "all":
            for subreddit_prefixed in observer_constants.SUBREDDITS_TO_OBSERVE:
                try:
                    observer_send_messages.message_observers_for_subreddit(
                        api=api, subreddit_prefixed=subreddit_prefixed,
                        timestamp=timestamp
                    )
                except Exception as e:
                    print(e)
        else:
            raise ValueError(
                f"Invalid argument passed in for `subreddit`: {subreddit}"
            )
        print(
            f"Completed sending DMs for observer phase for timestamp {timestamp}" # noqa
        )
    except Exception as e:
        print(f"Unable to complete sending DMs for observer phase: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script for sending DMs to users on Reddit."
    )
    parser.add_argument(
        "--phase", type=str, required=True, help="Phase (author/observer)"
    )
    parser.add_argument(
        "--timestamp", type=str, required=True, help="Timestamp of data."
    )
    parser.add_argument(
        "--subreddit", type=str, help="Subreddit for observer phase"
    )
    args = parser.parse_args()
    if args.phase == "author":
        send_author_phase_dms(timestamp=args.timestamp)
    elif args.phase == "observer":
        send_observer_phase_dms(
            timestamp=args.timestamp, subreddit=args.subreddit
        )
    else:
        raise ValueError(
            f"Invalid phase passed in: {args.phase} (must be author/observer)"
        )
