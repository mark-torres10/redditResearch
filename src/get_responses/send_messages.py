"""Given the list of users to message for the observer phase, send the
messages.

Example usage:

# to message all the subreddits that we curated
python send_messages.py 2023-07-09_1614 all

# to message a single subreddit
python send_messages.py 2023-07-09_1614 r/politics
"""
import os
import sys

import pandas as pd
import praw

from get_responses import constants
from get_responses.determine_who_to_message import strip_prefix_from_subreddit
from lib.helper import transform_permalink_to_link
from lib.reddit import init_api_access
from message.send_messages import catch_rate_limit_and_sleep, send_message

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

    has_been_messaged_col = []

    for row_tuple in df.iterrows():
        idx, row = row_tuple
        # TODO: add logic for sending DMs
        # TODO: don't connect API to this until the rest of the code
        # actually works, so we don't waste sending extra DMs or 
        # we don't exceed the API limits.
        try:
            subject = constants.OBSERVER_DM_SUBJECT_LINE
            message_body = constants.OBSERVER_DM_SCRIPT.format(
                name=row["author_name"],
                subreddit_name=row["subreddit_name_prefixed"],
                date=row["created_utc_string"],
                post=row["post_body"],
                permalink=transform_permalink_to_link(row["post_permalink"])
            )
            send_message(
                api=api,
                user=row["author_name"],
                subject=subject,
                body=message_body
            )
            has_been_messaged_col.append(1)
        except Exception as e:
            # catch rate limit, skip this row, and hopefully succeed on
            # messaging the next row
            # TODO: build retry so it tries messaging this row again.
            print(
                "Unable to message row {row} with user id {id} for reason {e}".format( # noqa
                    row=idx, id=row["author_id"], e=e
                )
            )
            has_been_messaged_col.append(0)
            catch_rate_limit_and_sleep(e)
            continue
    
    num_to_message_possible = df.shape[0]
    print(
        "Messaged {sum_messaged} out of {sum_possible} total possible".format(
            sum_messaged=sum(has_been_messaged_col),
            sum_possible=num_to_message_possible
        )
    )

    # dump results to new path
    if not os.path.exists(constants.MESSAGED_OBSERVERS_PATH):
        os.mkdir(constants.MESSAGED_OBSERVERS_PATH)

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
    subreddit = sys.argv[2]
    # TODO: update list of users who have already been messaged.
    # TODO: get users who have already been messaged.

    try:
        if subreddit in constants.SUBREDDITS_TO_OBSERVE:
            try:
                message_observers_for_subreddit(
                    api=api, subreddit_prefixed=subreddit,
                    timestamp=timestamp
                )
            except Exception as e:
                print(e)
        elif subreddit == "all":
            for subreddit_prefixed in constants.SUBREDDITS_TO_OBSERVE:
                try:
                    message_observers_for_subreddit(
                        api=api, subreddit_prefixed=subreddit_prefixed,
                        timestamp=timestamp
                    )
                except Exception as e:
                    print(e)
        else:
            raise ValueError(f"Invalid argument passed in for `subreddit`: {subreddit}")

        print(
            f"Completed sending DMs for observer phase for timestamp {timestamp}"
        )
    except Exception as e:
        print(f"Unable to complete sending DMs for observer phase: {e}")
