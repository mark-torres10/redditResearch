"""Get the messages that we've received from users.

From this, we can determine which users have messaged us back as well as which
posts have self-reported scores from their respective authors.

We want to record the message, message ID, the author, our original message,
the date that our DM was sent, and the date that we received the response.
"""
import datetime
import os
import re
from typing import Dict, List, Tuple

import pandas as pd
import praw
from praw.models.reddit.message import Message

from get_responses import constants
from lib.reddit import init_api_access
from lib.helper import convert_utc_timestamp_to_datetime_string

ALL_VALIDATED_RESPONSES_FILEPATH = os.path.join(
    constants.VALIDATED_RESPONSES_ROOT_PATH,
    constants.ALL_VALIDATED_RESPONSES_FILENAME
)
CURRENT_TIME_STR = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H%M")
SESSION_VALIDATED_RESPONSES_FILEPATH = os.path.join(
    constants.VALIDATED_RESPONSES_ROOT_PATH,
    constants.SESSION_VALIDATED_RESPONSES_FILENAME.format(
        timestamp=CURRENT_TIME_STR
    )
)

api = init_api_access()


def get_previous_labeling_session_data() -> List[Dict]:
    """Load in previous labeling session data. For each session, load
    as a pandas df and then convert to a list of dicts."""
    list_previously_labeled_data = []
    for filename in os.listdir(constants.VALIDATED_RESPONSES_ROOT_PATH):
        if "VALIDATED_RESPONSES_ROOT_FILENAME" in filename:
            df = pd.DataFrame(
                os.path.join(constants.VALIDATED_RESPONSES_ROOT_PATH, filename)
            )
            list_dict = df.to_dict()
            list_previously_labeled_data.extend(list_dict)
    return list_previously_labeled_data


def get_message_obj_from_id(
    api: praw.Reddit, message_id: str
) -> praw.models.reddit.message.Message:
    """Get the message object from the message ID."""
    return api.inbox.message(message_id)


class RedditMessage:
    """Message class to be able to convert the praw Message object into
    the fields that we care about."""    
    def __init__(self, message: praw.models.Message):
        self.message_id = message.id
        self.author_name = message.author.name
        self.author_t2_id = message.author_fullname
        self.subject = message.subject
        self.response_body = message.body
        self.created_utc = message.created_utc
        self.created_utc_string = convert_utc_timestamp_to_datetime_string(
            message.created_utc
        )
        self.replies: List[praw.models.Message] = message.replies

    def __repr__(self):
        return f"Message: {self.response_body}"


    @classmethod
    def check_if_possible_valid_response(self, response: str) -> bool:
        """Uses regex to see if the respnse could likely be valid. If the
        response has a number somewhere in it, it's possible that the response
        could be valid."""
        return bool(re.search(constants.NUMBER_REGEX_PATTERN, response))

    @classmethod
    def convert_to_df(self, messages: List) -> pd.DataFrame:
        message_lst = [message.__dict__() for message in messages]
        return pd.DataFrame(message_lst)


def get_messages_received(api: praw.Reddit) -> List[Message]:
    """Get the messages that we have received from other people.
    
    This assumes that people only DM us if we have DMed them first.
    """
    messages_received = [msg for msg in api.inbox.messages(limit=None)]
    return [
        msg.replies[0] for msg in messages_received if len(msg.replies) > 0
    ]


def load_previously_labeled_ids() -> List[str]:
    previously_labeled_data = []
    load_dir = constants.VALIDATED_RESPONSES_ROOT_PATH
    for filepath in os.listdir(load_dir):
        df = pd.read_csv(os.path.join(load_dir, filepath))
        ids = df["message_id"].tolist()
        previously_labeled_data.extend(ids)

    return previously_labeled_data


def write_labels_to_csv(labels: List[Tuple[str, int, str]]) -> None:
    colnames = ["message_id", "is_valid_response", "scores"]
    df = pd.DataFrame(labels, columns=colnames)
    df.to_csv(SESSION_VALIDATED_RESPONSES_FILEPATH)


def manually_validate_messages(messages: List[Message]) -> List[Message]:
    """Validates the messages that we have received.
    
    Manually QAs the messages and records the ones that have valid responses.

    Writes the IDs
    """
    previously_labeled_data_ids = load_previously_labeled_ids()

    num_messages = len(messages)
    responses_list: List[Tuple[str, int, str]] = []

    for idx, msg in enumerate(messages):
        print('-' * 10)
        print(f"Labeling message {idx} out of {num_messages}")
        # print the body
        print(f"Body: {msg.body}")
        # ask if valid (y/n) or if to exit session
        user_input = ''
        valid_inputs = ['y', 'n', 'e']
        break_session = False
        if msg.id in previously_labeled_data_ids:
            print(f"Previously labeled response, with id {msg.id}, skipping")
            continue
        while user_input not in valid_inputs:
            user_input = input("Is this a valid response? ('y', 'n', or 'e' to exit)\t") # noqa
            if user_input == 'y':
                print("Valid response.")
                scores = input("Please enter their scores (e.g., 1123):\t")
                responses_list.append((msg.id, 1, scores))

            elif user_input == 'n':
                print("Invalid response")
                responses_list.append((msg.id, 0, ''))
            elif user_input == 'e':
                print("Exiting labeling session...")
                break_session = True
            else:
                print(f"Invalid input: {user_input}")
        user_input = ''
        if break_session:
            break

    write_labels_to_csv(responses_list)


if __name__ == "__main__":
    messages_received = get_messages_received(api)
    manually_validate_messages(messages_received)


