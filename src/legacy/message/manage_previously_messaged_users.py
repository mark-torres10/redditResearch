"""Script to manage users who have already been messaged."""
import os
from typing import Optional

import pandas as pd

from message.constants import (
    ALL_MESSAGED_USERS_FILENAME, MESSAGES_ROOT_PATH, SENT_MESSAGES_FILENAME
)

DEFAULT_MESSAGED_USERS_FILEPATH = os.path.join(
    MESSAGES_ROOT_PATH, ALL_MESSAGED_USERS_FILENAME
)

def get_all_users_who_have_been_messaged() -> pd.DataFrame:
    """Loops through all the directories in the root path, loads in the
    'user_to_has_received_messages.csv' file, and adds the information of
    those users to a pandas dataframe if they've been messaged (as indicated
    by the `has_been_messaged` flag).
    """
    users_who_have_been_messaged: list[dict] = []

    for _, dirnames, _ in os.walk(MESSAGES_ROOT_PATH):
        for timestamp_dir in dirnames:
            full_directory = os.path.join(MESSAGES_ROOT_PATH, timestamp_dir)
            if SENT_MESSAGES_FILENAME in os.listdir(full_directory):
                full_fp = os.path.join(full_directory, SENT_MESSAGES_FILENAME)
                previous_message_status_df = pd.read_csv(full_fp)
                for row_tuple in previous_message_status_df.iterrows():
                    _, row = row_tuple
                    has_been_messaged_flag = row["has_been_messaged"]
                    if has_been_messaged_flag == 1:
                        output_dict = {
                            "author_id": row["author_id"],
                            "author_screen_name": row["author_screen_name"],
                            "post_id": row["id"],
                            "post_created_utc_string": (
                                row["created_utc_string"]
                            )
                        }
                        users_who_have_been_messaged.append(output_dict)
    
    return pd.DataFrame(users_who_have_been_messaged)


def dump_all_previously_messaged_users_as_csv(
    output_filepath: Optional[str] = DEFAULT_MESSAGED_USERS_FILEPATH
) -> None:
    """Dump all previously messaged users as a CSV file."""
    previously_messaged_users_df = get_all_users_who_have_been_messaged()
    previously_messaged_users_df.to_csv(output_filepath, index=False)


if __name__ == "__main__":
    dump_all_previously_messaged_users_as_csv()
