import os
import sys

import pandas as pd

from lib.helper import ROOT_DIR
from lib.logging import RedditLogger
from message.handle_messages import send_message
from message.helper import (
    HAS_BEEN_MESSAGED_COL, TO_MESSAGE_COL, determine_which_posts_to_message
)
from sync.constants import SYNC_METADATA_FILENAME, SYNC_RESULTS_FILENAME

logger = RedditLogger(name=__name__)

SYNC_ROOT_PATH = os.path.join(ROOT_DIR, "sync")

MESSAGES_ROOT_PATH = os.path.join(ROOT_DIR, "messages")

OUTPUT_FILENAME = "posts_to_message_status.csv"

if __name__ == "__main__":
    # load classified files
    load_timestamp = sys.argv[1]

    timestamp_dir = os.path.join(SYNC_ROOT_PATH, load_timestamp)

    if not os.path.exists(timestamp_dir):
        logger.error(f"Timestamp directory {load_timestamp} does not exist")
        sys.exit(1)
    
    metadata_filepath = os.path.join(timestamp_dir, SYNC_METADATA_FILENAME)
    sync_data_filepath = os.path.join(timestamp_dir, SYNC_RESULTS_FILENAME)

    metadata: pd.DataFrame
    sync_data: pd.DataFrame

    metadata = pd.read_csv(metadata_filepath)
    sync_data = pd.read_json(sync_data_filepath, lines=True)

    api = "" # TODO: instantiate access to Reddit API.

    # balance messages (ratio of 1:1 for outrage/not outrage)
    sync_data = determine_which_posts_to_message(
        sync_data_df=sync_data, balance_strategy="equal"
    )

    has_been_messaged_col = []

    for idx, row in enumerate(sync_data.iterrows()):
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
    
    sync_data[HAS_BEEN_MESSAGED_COL] = has_been_messaged_col

    # dump results of messaging
    output_filepath = os.path.join(
        MESSAGES_ROOT_PATH, load_timestamp, OUTPUT_FILENAME
    )

    sync_data.to_csv(output_filepath)

    logger.info(
        f"Done sending messages for data synced on timestamp {load_timestamp}"
    )
