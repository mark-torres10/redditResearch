"""Get the messages that we've received from users.

From this, we can determine which users have messaged us back as well as which
posts have self-reported scores from their respective authors.

We want to record the message, message ID, the author, our original message,
the date that our DM was sent, and the date that we received the response.
"""
import datetime
import os
import re
from typing import Dict, List

import pandas as pd
import praw

from get_responses import constants
from lib.reddit import init_api_access
from ml.transformations import convert_utc_timestamp_to_datetime_string

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
        self.possibly_has_valid_response = (
            self.check_if_possible_valid_response(self.response_body)
        )


    def __dict__(self):
        """Converts attributes into dictionary that we can convert into
        a pandas df."""
        pass

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
    

def get_sent_messages(api: praw.Reddit) -> List[RedditMessage]:
    """Get the messages that we've sent to users."""
    sent_messages = [RedditMessage(msg) for msg in api.inbox.sent(limit=None)]
    print(f"Number of sent messages: {len(sent_messages)}")
    return sent_messages


def get_sent_messages_with_responses(api: praw.Reddit) -> List[RedditMessage]:
    """Get the messages that we've sent to users that have responses."""
    sent_messages = get_sent_messages(api)
    return [msg for msg in sent_messages if len(msg.replies) > 0]


def get_messages_with_possibly_valid_responses(
    api: praw.Reddit
) -> List[RedditMessage]:
    """Get the messages that we've sent to users that have responses."""
    sent_messages = get_sent_messages(api)
    return [msg for msg in sent_messages if msg.possibly_has_valid_response]


def get_possibly_valid_responses(
    messages_with_possible_responses: List[RedditMessage]
) -> List[Dict]:
    """Returns a dictionary where the keys indicate the original DM that
    we sent as well as the user's response.
    
    Key is the message ID and value is a dictionary of the original DM, original
    DM object, and the response message."""
    responses = {}
    for msg in messages_with_possible_responses:
        replies = msg.replies
        replies_body_list = []
        for reply in replies:
            replies_body_list.append(reply.body)
        replies_body = '\n'.join(replies_body_list)
        responses[msg.message_id] = {
            "original_message_object": msg,
            "original_dm": msg.response_body,
            "response_dm": replies_body
        }
    return responses


def manually_validate_responses(
    previously_labeled_ids: List[str],
    possibly_valid_responses: List[Dict]
) -> None:
    """Given the list of possibly valid responses, validate those responses
    manually, add to a pandas df, and write to a .csv file."""
    validated_responses = []
    total_responses = len(possibly_valid_responses)
    for idx, (message_id, message_obj) in enumerate(
        possibly_valid_responses.items()
    ):
        print('-' * 5)
        print(f"Validating response {idx} out of {total_responses} responses.")

        message = message_obj["original_message_object"]
        response_dm = message_obj["response_dm"]

        print(f"Response body: {response_dm}")

        if message_id in previously_labeled_ids:
            print(f"Skipping {message_id} since it has already been labeled previously.")  # noqa
            continue
        check = ''
        valid_checks = ['y', 'n', 'q']
        while check not in valid_checks:
            base_response_obj = {
                "message_id": message_id,
                "author_name": message.author_name,
                "author_t2_id": message.author_t2_id,
                "subject": message.subject,
                "response_body": message.response_body,
                "created_utc_string": message.created_utc_string
            }
            check = input("Is this a valid response? (y=yes, n=no, q=quit)")
            if check == 'y':
                validated_score = input(
                    "What are their scores? Give as 4 digit string"
                )
                updated_response_obj = {
                    **base_response_obj,
                    **{
                        "is_valid_response": True,
                        "validated_score": validated_score
                    }
                }
                validated_responses.append(updated_response_obj)
                check = ''
                continue
            if check == 'n':
                updated_response_obj = {
                    **base_response_obj,
                    **{
                        "is_valid_response": False,
                        "validated_score": None
                    }
                }
                validated_responses.append(updated_response_obj)
                print(f"{response_dm} not valid response, skipping...")
                check = ''
                continue
            if check == 'q':
                print(f"Quitting labeling session. QAed {idx} out of {total_responses}") # noqa
                break
            else:
                print(f"Please provide a valid input. {check} not in [{valid_checks}]") # noqa
    print(f"Validated {total_responses} possibly valid responses.")

    # create and dump file of today's validation session.
    print(
        f"Writing to df and dumping to .csv file at {SESSION_VALIDATED_RESPONSES_FILEPATH}" # noqa
    )
    validated_responses_df = pd.DataFrame(validated_responses)
    validated_responses_df.to_csv(SESSION_VALIDATED_RESPONSES_FILEPATH)

    # create and dump file of all validation sessions.
    print(f"Consolidating data from all labeling sessions into one .csv file at {ALL_VALIDATED_RESPONSES_FILEPATH}")
    all_responses = previously_labeled_data + validated_responses
    all_responses_df = pd.DataFrame(all_responses)
    all_responses_df.to_csv(ALL_VALIDATED_RESPONSES_FILEPATH)


if __name__ == "__main__":
    # go through DMs in inbox
    possibly_valid_messages = get_messages_with_possibly_valid_responses(api)
    possibly_valid_responses = get_possibly_valid_responses(
        messages_with_possible_responses=possibly_valid_messages
    )

    # get previously labeled data:
    previously_labeled_data = get_previous_labeling_session_data()
    previously_labeled_ids = [
        data["message_id"] for data in previously_labeled_data
    ]

    # do manual QA
    manually_validate_responses(
        previously_labeled_ids=previously_labeled_ids,
        possibly_valid_responses=possibly_valid_responses
    )
