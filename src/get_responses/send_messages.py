"""Given the list of users to message for the observer phase, send the
messages."""
import ast
import os
import sys

import pandas as pd
import praw

from get_responses import constants
from get_responses.determine_who_to_message import strip_prefix_from_subreddit
from lib.reddit import init_api_access

api = init_api_access()


def message_observers_for_subreddit(
    api: praw.Reddit, subreddit_prefixed: str, timestamp: str
) -> None:
    """For a given subreddit, get the list of users who need to be messaged
    and then send them messages."""
    subreddit_stripped = strip_prefix_from_subreddit(subreddit_prefixed)
    subreddit_users_to_message_dir = os.path.join(
        constants.SUBREDDITS_ROOT_PATH, subreddit_stripped
    )
    filename = f"users_to_message_{timestamp}.csv"
    df = pd.read_csv(
        os.path.join(subreddit_users_to_message_dir, filename)
    )

    breakpoint()

    has_been_messaged_col = []

    # should be able to just take the first value in this column, as they
    # all have the same value (it was done like this for convenience sake,
    # to save 1 additional join or filtering step - developer's choice)
    users_to_message = ast.literal_eval(df["users_to_message_list"][0])

    # TODO: why is to_message all 0s? This is wrong.
    for row_tuple in df.iterrows():
        idx, row = row_tuple
        # TODO: add logic for sending DMs
        # TODO: don't connect API to this until the rest of the code
        # actually works, so we don't waste sending extra DMs or 
        # we don't exceed the API limits.
    
    # dump results to new path
    subreddit_export_dir = os.path.join(
        constants.MESSAGED_OBSERVERS_PATH, subreddit_stripped
    )
    if not os.path.exists(subreddit_export_dir):
        os.mkdir(subreddit_export_dir)

    df["has_been_messaged_col"] = has_been_messaged_col

    filepath = os.path.join(
        subreddit_export_dir, f"users_messaged_{timestamp}.csv"
    )

    df.to_csv(filepath)
    print(
        f"Completed sending DMs to {sum(has_been_messaged_col)} out of "
        f"{df.shape[0]} users we chose in the {subreddit_prefixed} subreddit"
    )
    return


if __name__ == "__main__":
    timestamp = sys.argv[1]
    # TODO: update list of users who have already been messaged.

    # TODO: get users who have already been messaged.

    # load data with who to message.
    has_been_messaged_col = []

    for subreddit_prefixed in constants.SUBREDDITS_TO_OBSERVE:
        try:
            message_observers_for_subreddit(
                api=api, subreddit_prefixed=subreddit_prefixed,
                timestamp=timestamp
            )
        except Exception as e:
            print(e)
            breakpoint()

    print(
        f"Completed sending DMs for observer phase for timestamp {timestamp}"
    )
