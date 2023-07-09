"""After we have the messages from poster, get the messages to send
to observers."""
import os
from typing import Dict

import pandas as pd
import praw

from get_responses import constants
from get_responses.retrieve_messages import get_message_obj_from_id
from lib.reddit import init_api_access
from message import constants as message_constants

api = init_api_access()

PREVIOUS_CONSOLIDATED_MESSAGES_FILENAME = os.path.join(
    message_constants.MESSAGES_ROOT_PATH,
    message_constants.CONSOLIDATED_MESSAGES_FILE_NAME
)

previous_messages_df = pd.read_csv(PREVIOUS_CONSOLIDATED_MESSAGES_FILENAME)

map_users_to_post_id_and_date = {
    author_id: {
        "author_id": author_id,
        "post_id": post_id,
        "post_body": post_body,
        "post_permalink": post_permalink,
        "created_utc_string": created_utc_string,
        "label": label,
        "subreddit_name_prefixed": subreddit_name_prefixed
    }
    for (
        author_id, post_id, post_body, post_permalink, created_utc_string,
        label, subreddit_name_prefixed
    )
    in zip(
        previous_messages_df["author_id"].tolist(),
        previous_messages_df["id"].tolist(),
        previous_messages_df["body"].tolist(),
        previous_messages_df["permalink"].tolist(),
        previous_messages_df["created_utc_string"].tolist(),
        previous_messages_df["label"].tolist(),
        previous_messages_df["subreddit_name_prefixed"].tolist(),
    )
}


def load_valid_previous_messages() -> pd.DataFrame:
    """Load the IDs of messages that we previously labeled and confirmed were
    valid."""
    previously_labeled_ids = []
    scores = []
    load_dir = constants.VALIDATED_RESPONSES_ROOT_PATH
    for filepath in os.listdir(load_dir):
        df = pd.read_csv(os.path.join(load_dir, filepath))
        ids = df["message_id"].tolist()
        scores = df["scores"].tolist()
        valid_responses_flags = df["is_valid_response"].tolist()
        valid_ids = [
            id_ for id_, flag in zip(ids, valid_responses_flags) if  flag == 1
        ]
        valid_scores = [
            score for score, flag in zip(scores, valid_responses_flags)
            if  flag == 1
        ]
        previously_labeled_ids.extend(valid_ids)
        scores.extend(valid_scores)

    return pd.DataFrame(
        zip(previously_labeled_ids, scores), columns=["message_id", "scores"]
    )


def get_author_id_for_message(api: praw.Reddit, message_id: str):
    """In our initial code to get the messages that we receive, we didn't
    record the IDs of the users who sent us each message.
    
    In this step, we want to recreate the message object and get the
    associated user who sent us the message.
    """
    obj = get_message_obj_from_id(api, message_id)
    author_id = obj.author_fullname
    return author_id



def hydate_message_with_post_and_subreddit_information(
    message_info: Dict
) -> Dict:
    """Given the user and message ID, we can hydrate with post and
    subreddit information.
    """
    author_id = message_info["author_id"]
    post_info = map_users_to_post_id_and_date.get(author_id, {})
    if not post_info:
        print(f"Author ID {author_id} not found in dataset. Skipping...")
        return
    return {**message_info, **post_info}


def hydrate_message(api: praw.Reddit, message_id: str, score: str):
    author_id = get_author_id_for_message(api, message_id)
    message_info = {
        "author_id": author_id,
        "score": score
    }
    message_with_hydrated_info = (
        hydate_message_with_post_and_subreddit_information(message_info)
    )
    return message_with_hydrated_info


def hydrate_messages(
    api: praw.Reddit, message_id_list: str, score_list: str
) -> Dict:
    return [
        hydrate_message(api, message_id, score)
        for (message_id, score) in zip(message_id_list, score_list)
    ]


def dump_hydrated_messages_to_csv(hydrated_messages: Dict) -> None:
    df = pd.DataFrame(hydrated_messages)
    df.to_csv(
        os.path.join(
            constants.RESPONSES_ROOT_PATH,
            constants.HYDRATED_VALIDATED_RESPONSES_FILENAME
        )
    )


def main():
    previous_labeled_data_df = load_valid_previous_messages()
    previously_labeled_ids = previous_labeled_data_df["message_id"].tolist()
    previous_scores = previous_labeled_data_df["scores"].tolist()
    hydrated_messages = hydrate_messages(
        api, previously_labeled_ids, previous_scores
    )
    dump_hydrated_messages_to_csv(hydrated_messages)


if __name__ == "__main__":
    main()
