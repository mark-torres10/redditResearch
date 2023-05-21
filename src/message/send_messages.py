import os
import sys

import pandas as pd

from lib.helper import CODE_DIR
from lib.redditLogging import RedditLogger
from message.constants import MESSAGES_ROOT_PATH, OUTPUT_FILENAME
from message.handle_messages import send_message
from message.helper import (
    HAS_BEEN_MESSAGED_COL, TO_MESSAGE_COL, determine_which_posts_to_message
)
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
    labeled_data = pd.read_json(labeled_data_filepath, lines=True)

    api = "" # TODO: instantiate access to Reddit API.

    # balance messages (ratio of 1:1 for outrage/not outrage)
    labeled_data = determine_which_posts_to_message(
        labeled_data=labeled_data, balance_strategy="equal"
    )

    has_been_messaged_col = []

    for idx, row in enumerate(labeled_data.iterrows()):
        id_col = [""] # TODO: add correct ID column
        if row[TO_MESSAGE_COL] == 1:
            try:
                send_message(api, )
                has_been_messaged_col.append(1)
            except Exception as e:
                logger.info(
                    "Unable to message row {row} with id {id} for reason {e}".format( # noqa
                        row=idx, id=row[id_col], e=e
                    )
                )
                has_been_messaged_col.append(0)
                continue
    
    labeled_data[HAS_BEEN_MESSAGED_COL] = has_been_messaged_col

    # dump results of messaging
    output_filepath = os.path.join(
        MESSAGES_ROOT_PATH, load_timestamp, OUTPUT_FILENAME
    )

    labeled_data.to_csv(output_filepath)

    logger.info(
        f"Done sending messages for data labeled on timestamp {load_timestamp}"
    )
