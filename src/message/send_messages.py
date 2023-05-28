"""Given a list of users to message, send them a message.

Example usage:
    python send_messages.py 2023-03-20_1438
"""
import os
import sys


from lib.reddit import init_api_access
from lib.redditLogging import RedditLogger
from message.constants import (
    MESSAGES_ROOT_PATH, SENT_MESSAGES_FILENAME, WHO_TO_MESSAGE_FILENAME
)

import pandas as pd
import praw

from message import helper

logger = RedditLogger(name=__name__)

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


if __name__ == "__main__":
    # load data with who to message.
    load_timestamp = sys.argv[1]

    timestamp_dir = os.path.join(MESSAGES_ROOT_PATH, load_timestamp)

    if not os.path.exists(timestamp_dir):
        logger.error(f"Labeled data timestamp directory {load_timestamp} does not exist")
        sys.exit(1)

    labeled_data_filepath = os.path.join(
        timestamp_dir, WHO_TO_MESSAGE_FILENAME
    )
    labeled_data = pd.read_csv(labeled_data_filepath)

    api = init_api_access()

    breakpoint()

    has_been_messaged_col = []

    for idx, row in enumerate(labeled_data.iterrows()):
        id_col = [""] # TODO: add correct ID column
        if row[helper.TO_MESSAGE_COL] == 1:
            try:
                # send_message(api, )
                has_been_messaged_col.append(1)
            except Exception as e:
                logger.info(
                    "Unable to message row {row} with id {id} for reason {e}".format( # noqa
                        row=idx, id=row[id_col], e=e
                    )
                )
                has_been_messaged_col.append(0)
                continue
    
    labeled_data[helper.HAS_BEEN_MESSAGED_COL] = has_been_messaged_col

    output_filepath = os.path.join(
        MESSAGES_ROOT_PATH, load_timestamp, SENT_MESSAGES_FILENAME
    )

    labeled_data.to_csv(output_filepath)

    logger.info(
        f"Done sending messages for data labeled on timestamp {load_timestamp}"
    )
