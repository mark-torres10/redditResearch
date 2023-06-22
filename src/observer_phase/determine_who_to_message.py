"""
Determines which users to message.

Example usage:
    python determine_who_to_message.py 2023-03-20_1438

We take posts for which we've received scores from the authors. We then send
messages to observers in the same subreddit to see what they would rate
those same posts.

Similar in implementation to `src.message.determine_who_to_message.py`, but
for the observer phase.

TODO: consolidate this and other script.
"""
import os
import sys


if __name__ == "__main__":
    # load data of messages that were sent to users.
    load_timestamp = sys.argv[1]

    # timestamp_dir = os.path.join(ML_ROOT_PATH, load_timestamp)