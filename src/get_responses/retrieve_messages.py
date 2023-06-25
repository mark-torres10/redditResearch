"""Get the messages that we've received from users.

From this, we can determine which users have messaged us back as well as which
posts have self-reported scores from their respective authors.

We want to record the message, message ID, the author, our original message,
the date that our DM was sent, and the date that we received the response.
"""
import os
import re
from typing import List

import pandas as pd
import praw

from get_responses.constants import (
    NUMBER_REGEX_PATTERN, RESPONSES_ROOT_PATH, VALIDATED_RESPONSES_FILENAMES
)
from lib.reddit import init_api_access
from ml.transformations import convert_utc_timestamp_to_datetime_string

DEFAULT_MESSAGED_USERS_FILEPATH = os.path.join(
    RESPONSES_ROOT_PATH, VALIDATED_RESPONSES_FILENAMES
)

api = init_api_access()


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
            self.check_if_possible_valid_response(self.body)
        )


    def __dict__(self):
        """Converts attributes into dictionary that we can convert into
        a pandas df."""
        pass

    @classmethod
    def check_if_possible_valid_response(response: str) -> bool:
        """Uses regex to see if the respnse could likely be valid. If the
        response has a number somewhere in it, it's possible that the response
        could be valid."""
        return bool(re.search(NUMBER_REGEX_PATTERN, response))

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


def manually_validate_responses(
    possibly_valid_responses: List[RedditMessage]
) -> List[RedditMessage]:
    """Given the list of possibly valid responses, validate those responses
    manually, add to a pandas df, and write to a .csv file."""
    validated_responses = []
    total_responses = len(possibly_valid_responses)
    for idx, response in enumerate(possibly_valid_responses):
        print('-' * 5)
        print(f"Validating response {idx} out of {total_responses} responses.")
        print(f"Response body: {response.response_body}")
        check = ''
        valid_checks = ['y', 'n', 'q']
        while check not in valid_checks:
            check = input("Is this a valid response? (y=yes, n=no, q=quit)")
            if check == 'y':
                validated_score = input(
                    "What are their scores? Give as 4 digit string"
                )
                output_obj = {
                    "message_id": response.id,
                    "author_name": response.author_name,
                    "author_t2_id": response.author_t2_id,
                    "subject": response.subject,
                    "response_body": response.response_body,
                    "created_utc_string": response.created_utc_string,
                    "validated_score": validated_score
                }
                validated_responses.append(output_obj)
                check = ''
            if check == 'n':
                print(f"{response.response_body} not valid response, skipping...") # noqa
                check = ''
            if check == 'q':
                print(f"Quitting labeling session. QAed {idx} out of {total_responses}") # noqa
                break
            else:
                print(f"Please provide a valid input. {check} not in [{valid_checks}]") # noqa
    print(f"Validated {total_responses} possibly valid responses.")
    print(
        f"Writing to df and dumping to .csv file at {DEFAULT_MESSAGED_USERS_FILEPATH}" # noqa
    )
    validated_responses_df = pd.DataFrame(validated_responses)
    validated_responses_df.to_csv(DEFAULT_MESSAGED_USERS_FILEPATH)


if __name__ == "__main__":
    possibly_valid_responses = get_messages_with_possibly_valid_responses(api)
    manually_validate_responses(possibly_valid_responses)
