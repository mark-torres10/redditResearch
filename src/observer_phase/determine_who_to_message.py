"""Given the subreddits present in the validated messages that we've received
from the author phase, determine who to message for the observer phase.

We can randomly message an active subset of the subreddit.
"""
import os
from typing import Dict, List

import pandas as pd
import praw

from get_responses import constants
from lib.helper import CURRENT_TIME_STR, strip_prefix_from_subreddit
from lib.reddit import init_api_access
from message import constants as message_constants

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

hydrated_author_messages_df = pd.read_csv(
    os.path.join(
        constants.RESPONSES_ROOT_PATH,
        constants.HYDRATED_VALIDATED_RESPONSES_FILENAME
    )
)

def select_author_messages_for_observers_to_respond(
    df_subreddits: pd.DataFrame
) -> pd.DataFrame:
    # for each subreddit, pick 4 validated responses that we want observers
    # to respond to. Set the "to_message" column to 1 for these posts.
    df_subreddits["to_message"] = 0

    for subreddit in df_subreddits["subreddit_name_prefixed"].unique():
        subreddit_indices = df_subreddits[
            df_subreddits['subreddit_name_prefixed'] == subreddit
        ].index
        df_subreddits.loc[
            subreddit_indices[:constants.NUM_POSTS_PER_SUBREDDIT_TO_OBSERVE],
            'to_message'
        ] = 1
        df_subreddits.loc[
            subreddit_indices[constants.NUM_POSTS_PER_SUBREDDIT_TO_OBSERVE:],
            'to_message'
        ] = 0
    
    return df_subreddits


def get_users_in_subreddit(
    api: praw.Reddit, subreddit: str, num_users: int
) -> List[Dict[str, str]]:
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
        if (
            not hasattr(comment, "author_fullname")
            or not hasattr(comment, "author")
        ):
            continue
        author_id = comment.author_fullname
        author_name = comment.author.name
        if (
            author_id not in previously_messaged_user_ids
            and num_users_added < num_users
        ):
            users_lst.append(
                {
                    "subreddit_user_id": author_id,
                    "subreddit_user_name": author_name
                }
            )
            num_users_added += 1
        if num_users_added > num_users:
            break
    return users_lst


def assign_subreddit_users_to_author_message_map(
    users_in_subreddit: List[Dict[str ,str]]
) -> List[Dict[str, str]]:
    """Maps the users to message with the author message/comment/post that we
    want them to score.
    
    Returns as list of dictionaries, which we can transform into a pandas df.

    The `users_in_subreddit` is a list of dictionaries where each dictionary
    is of the format:
        {
            "subreddit_user_id": id of the person in the subreddit
            "subreddit_user_name": screen name of the user
        }
    
    We loop through the list of the users in the subreddit and assign them to
    one of the author messages/comments/posts that we have. The output of this
    is a list of dictionaries, where each dictionary is a combination of
    one user and one author message for that user to observe/respond to.
    """
    output_lst = []
    idx = 0
    for user in users_in_subreddit:
        row_to_use = hydrated_author_messages_df.loc[idx, :]
        output_lst.append(
            {
                "subreddit_user_id": user["subreddit_user_id"],
                "subreddit_user_name": user["subreddit_user_name"],
                "id": row_to_use["id"],
                "body": row_to_use["body"],
                "permalink": row_to_use["permalink"],
                "created_utc_string": row_to_use["created_utc_string"],
                "label": row_to_use["label"],
                "score": row_to_use["score"],
                "subreddit_name_prefixed": row_to_use["subreddit_name_prefixed"] # noqa
            }
        )
        # update iterator
        idx += 1
        if idx >= constants.NUM_POSTS_PER_SUBREDDIT_TO_OBSERVE:
            idx = 0
    
    return output_lst


def get_observers_to_message_per_subreddit(
    df_subreddits: pd.DataFrame
) -> Dict[str, List[Dict]]:
    """For each subreddit, have a list of the observers/users in the subreddit
    that we can message in order for them to respond to an author's
    comment/post.
    """

    # for each subreddit, get a random list of 50 users in the subreddit
    # who have been active in the past 48 hours. Filter out anyone who
    # we have messaged previously. Add this list as a column in the dataset.
    subreddit_prefixed_to_user_id_list: Dict[str, List[Dict[str, str]]] = {
        subreddit_prefixed: get_users_in_subreddit(
            api=api,
            subreddit=strip_prefix_from_subreddit(subreddit_prefixed),
            num_users=constants.NUM_SUBREDDIT_USERS_TO_FETCH
        )
        for subreddit_prefixed
        in df_subreddits["subreddit_name_prefixed"].unique()
    }

    map_subreddit_to_observers: Dict[str, List[Dict]] = {
        subreddit_prefixed: assign_subreddit_users_to_author_message_map(
            users_in_subreddit=(
                subreddit_prefixed_to_user_id_list[subreddit_prefixed]
            )
        )
        for subreddit_prefixed
        in df_subreddits["subreddit_name_prefixed"].unique()
    }

    return map_subreddit_to_observers


def dump_observers_per_subreddit(
    map_subreddit_to_observers: List[str, Dict[str, str]]
) -> None:
    """For each subreddit, create a new df that has all the observers to
    message as well as to which author message/post they should respond to.

    Then, dump these dataframes as .csv files.
    """
    if not os.path.exists(constants.SUBREDDITS_ROOT_PATH):
        os.mkdir(constants.SUBREDDITS_ROOT_PATH)

    for (subreddit_prefixed, users_to_message_dict_list) in (
        map_subreddit_to_observers.items()
    ):
        subreddit_name = strip_prefix_from_subreddit(subreddit_prefixed)
        subreddit_dir = os.path.join(
            constants.SUBREDDITS_ROOT_PATH, subreddit_name
        )
        if not os.path.exists(subreddit_dir):
            os.mkdir(subreddit_dir)
        full_filepath = os.path.join(
            subreddit_dir, f"users_to_message_{CURRENT_TIME_STR}.csv"
        )
        export_df = pd.DataFrame(users_to_message_dict_list)
        export_df.to_csv(full_filepath)


if __name__ == "__main__":
    df_subreddits = hydrated_author_messages_df[
        hydrated_author_messages_df["subreddit_name_prefixed"].isin(
            constants.SUBREDDITS_TO_OBSERVE
        )
    ]
    
    df_subreddits = (
        select_author_messages_for_observers_to_respond(df_subreddits)
    )

    subreddits_without_prefixes = [
        strip_prefix_from_subreddit(subreddit)
        for subreddit in df_subreddits["subreddit_name_prefixed"].unique()
    ]

    map_subreddit_to_observers = get_observers_to_message_per_subreddit(
        df_subreddits
    )

    dump_observers_per_subreddit(map_subreddit_to_observers)

    print("Completed determining which users to message, per subreddit.")
