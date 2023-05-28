"""
Determines which users to message.

Example usage:
    python determine_who_to_message.py 2023-03-20_1438
"""
import os
import sys

import pandas as pd

from lib.redditLogging import RedditLogger
from message.constants import MESSAGES_ROOT_PATH, WHO_TO_MESSAGE_FILENAME
from message.helper import determine_which_posts_to_message

from ml.constants import LABELED_DATA_FILENAME, ML_ROOT_PATH

logger = RedditLogger(name=__name__)

if __name__ == "__main__":
    # load labeled data
    load_timestamp = sys.argv[1]

    timestamp_dir = os.path.join(ML_ROOT_PATH, load_timestamp)

    if not os.path.exists(timestamp_dir):
        logger.error(f"Labeled data timestamp directory {load_timestamp} does not exist")
        sys.exit(1)

    labeled_data_filepath = os.path.join(timestamp_dir, LABELED_DATA_FILENAME)
    labeled_data = pd.read_csv(labeled_data_filepath)

    # balance messages (ratio of 1:1 for outrage/not outrage)
    labeled_data = determine_which_posts_to_message(
        labeled_data=labeled_data, balance_strategy="equal"
    )

    # dump results of messaging
    output_directory = os.path.join(MESSAGES_ROOT_PATH, load_timestamp, '')
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    output_filepath = os.path.join(output_directory, WHO_TO_MESSAGE_FILENAME)
    labeled_data.to_csv(output_filepath)

    print(
        f"Done determining who to message, for data labeled on timestamp {load_timestamp}" # noqa
    )
