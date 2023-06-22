"""Get the messages that we've received from users.

From this, we can determine which users have messaged us back as well as which
posts have self-reported scores from their respective authors.

We want to record the message, message ID, the author, our original message,
the date that our DM was sent, and the date that we received the response.
"""
from typing import List

import pandas as pd
import praw

from lib.reddit import init_api_access
from ml.transformations import convert_utc_timestamp_to_datetime_string

class RedditMessage:
    """Message class to be able to convert the praw Message object into
    the fields that we care about."""    
    def __init__(self, message: praw.models.Message):
        self.author_name = message.author.name
        self.subject = message.subject
        self.response_body = message.body
        self.created_utc = message.created_utc
        self.created_utc_string = convert_utc_timestamp_to_datetime_string(
            message.created_utc
        )
        # TODO: get info about original message.


    def __dict__(self):
        """Converts attributes into dictionary that we can convert into
        a pandas df."""
        pass

    @classmethod
    def convert_to_df(self, messages: List) -> pd.DataFrame:
        message_lst = [message.__dict__() for message in messages]
        return pd.DataFrame(message_lst)
    

def get_sent_messages(api: praw.Reddit) -> List[RedditMessage]:
    """Get the messages that we've sent to users."""
    sent_messages = [msg for msg in api.inbox.sent()]
    msgs = [msg for msg in api.inbox.stream()]
    breakpoint()


if __name__ == "__main__":
    api = init_api_access()
    inbox = api.inbox.all()
    direct_messages = [
        item for item in inbox if isinstance(item, praw.models.Message)
    ]

    for message in direct_messages:
        print(f"From: {message.author.name}")
        print(f"Subject: {message.subject}")
        print(f"Body: {message.body}")
        print("-----")
        breakpoint()

