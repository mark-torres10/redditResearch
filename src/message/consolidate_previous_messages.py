"""Given all the messages that we've send, consolidate all the .csv
files into a master set of messages that have been sent."""
import os

import pandas as pd

from message.constants import (
    CONSOLIDATED_MESSAGES_FILE_NAME, MESSAGES_ROOT_PATH, SENT_MESSAGES_FILENAME
)

fields_to_get = [
    "id", "author_id", "body", "permalink", "created_utc_string", "label",
    "subreddit_name_prefixed"
]


def main():
    ids = []
    author_ids = []
    bodies = []
    permalinks = []
    created_utc_strings = []
    labels = []
    subreddits = []

    for _, dirnames, _ in os.walk(MESSAGES_ROOT_PATH):
        for timestamp_dir in dirnames:
            full_directory = os.path.join(MESSAGES_ROOT_PATH, timestamp_dir)
            if SENT_MESSAGES_FILENAME in os.listdir(full_directory):
                full_fp = os.path.join(full_directory, SENT_MESSAGES_FILENAME)
                previous_message_status_df = pd.read_csv(full_fp)
                cols = previous_message_status_df.columns
                is_valid_df = True
                # if any of the fields in fields_to_get arent in the set of
                # fields in the df, skip the df.
                for field in fields_to_get:
                    if field not in cols:
                        print(f"Not valid file, skipping... : {full_fp}")
                        is_valid_df = False
                if not is_valid_df:
                    continue
                for row_tuple in previous_message_status_df.iterrows():
                    _, row = row_tuple
                    ids.append(row["id"])
                    author_ids.append(row["author_id"])
                    bodies.append(row["body"])
                    permalinks.append(row["permalink"])
                    created_utc_strings.append(row["created_utc_string"])
                    labels.append(row["label"])
                    subreddits.append(row["subreddit_name_prefixed"])

    consolidated_df = pd.DataFrame(
        zip(
            ids, author_ids, bodies, permalinks, created_utc_strings, labels,
            subreddits
        ),
        columns=fields_to_get
    )

    consolidated_df.to_csv(
        os.path.join(MESSAGES_ROOT_PATH, CONSOLIDATED_MESSAGES_FILE_NAME)
    )
               

if __name__ == "__main__":
    main()
