"""Given the subreddits present in the validated messages that we've received
from the author phase, determine who to message for the observer phase.

We can randomly message an active subset of the subreddit.
"""
import datetime
import os
from typing import List

import pandas as pd
import praw

from get_responses import constants
from lib.reddit import init_api_access
from message import constants as message_constants

CURRENT_TIME_STR = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H%M")

api = init_api_access()

previously_messaged_users_df = pd.read_csv(
    os.path.join(
        message_constants.MESSAGES_ROOT_PATH,
        message_constants.ALL_MESSAGED_USERS_FILENAME
    )
)
previously_messaged_user_ids = (
    previously_messaged_users_df["author_id"].tolist()
)

def strip_prefix_from_subreddit(subreddit_prefixed: str) -> str:
    """Strip the "r/" prefix from the subreddit."""
    return subreddit_prefixed.replace("r/", "")


def get_users_in_subreddit(
    api: praw.Reddit, subreddit: str, num_users: int
) -> List[str]:
    """Get list of users in the subreddit.
    
    Skip over people who we messaged in the previous phase of the study.

    We can't directly get people who follow the subreddit, since this isn't
    an endpoint in the API. The best that we can do is iterate through the
    comments and use that as a proxy approximation that the people who leave
    comments are active participants in the subreddit and would therefore be
    able to provide good scores for our datasets.
    """
    subreddit = api.subreddit(subreddit)
    users_lst = []
    num_users_added = 0
    for comment in subreddit.comments(limit=None):
        if not hasattr(comment, "author_fullname"):
            continue
        user_id = comment.author_fullname
        if (
            user_id not in previously_messaged_user_ids
            and num_users_added < num_users
        ):
            users_lst.append(user_id)
            num_users_added += 1
    return users_lst


if __name__ == "__main__":
    # organize the "hydrated_validated_responses.csv" file by subreddit.
    df = pd.read_csv(
        os.path.join(
            constants.RESPONSES_ROOT_PATH,
            constants.HYDRATED_VALIDATED_RESPONSES_FILENAME
        )
    )

    # get only the subreddits for us to consider on the first pass.
    df_subreddits = df[
        df["subreddit_name_prefixed"].isin(constants.SUBREDDITS_TO_OBSERVE)
    ]
    
    # for each subreddit, pick 4 validated responses that we want observers
    # to respond to. Set the "to_message" column to 1 for these posts.
    df_subreddits["to_message"] = 0

    for subreddit in df_subreddits["subreddit_name_prefixed"].unique():
        subreddit_indices = df[df['subreddit_name_prefixed'] == subreddit].index # noqa
        df.loc[
            subreddit_indices[:constants.NUM_POSTS_PER_SUBREDDIT_TO_OBSERVE],
            'to_message'
        ] = 1
        df.loc[
            subreddit_indices[constants.NUM_POSTS_PER_SUBREDDIT_TO_OBSERVE:],
            'to_message'
        ] = 0

    subreddits_without_prefixes = [
        strip_prefix_from_subreddit(subreddit)
        for subreddit in df_subreddits["subreddit_name_prefixed"].unique()
    ]

    # for each subreddit, get a random list of 50 users in the subreddit
    # who have been active in the past 48 hours. Filter out anyone who
    # we have messaged previously. Add this list as a column in the dataset.
    subreddit_prefixed_to_user_id_list = {
        subreddit_prefixed: get_users_in_subreddit(
            api=api,
            subreddit=strip_prefix_from_subreddit(subreddit_prefixed),
            num_users=constants.NUM_SUBREDDIT_USERS_TO_FETCH
        )
        for subreddit_prefixed
        in df_subreddits["subreddit_name_prefixed"].unique()
    }

    subreddits_prefixed_list = (
        df_subreddits["subreddit_name_prefixed"].tolist()
    )
    users_to_message_list = [
        subreddit_prefixed_to_user_id_list[subreddit_prefixed]
        for subreddit_prefixed in subreddits_prefixed_list
    ]
    df_subreddits["users_to_message_list"] = users_to_message_list

    # create a new df for each subreddit.
    subreddit_to_df_map = {
        subreddit_prefixed: df_subreddits[
            df_subreddits["subreddit_name_prefixed"] == subreddit_prefixed
        ]
        for subreddit_prefixed in subreddits_prefixed_list
    }

    if not os.path.exists(constants.SUBREDDITS_ROOT_PATH):
        os.mkdir(constants.SUBREDDITS_ROOT_PATH)

    # dump each new df into a .csv file, whose name contains the subreddit
    # name as well as a timestamp.

    # in a new file, for each of these .csv files we would create the
    # message to send and then message the users.

    for subreddit_prefixed, subreddit_df in subreddit_to_df_map.items():
        subreddit_name = strip_prefix_from_subreddit(subreddit_prefixed)
        subreddit_dir = os.path.join(
            constants.SUBREDDITS_ROOT_PATH, subreddit_name
        )
        if not os.path.exists(subreddit_dir):
            os.mkdir(subreddit_dir)
        full_filepath = os.path.join(
            subreddit_dir, f"users_to_message_{CURRENT_TIME_STR}.csv"
        )
        subreddit_df.to_csv(full_filepath)

    print("Completed determining which users to message, per subreddit.")
