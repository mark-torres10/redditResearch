"""Get the messages that we've received from users.

From this, we can determine which users have messaged us back as well as which
posts have self-reported scores from their respective authors.

We want to record the message, message ID, the author, our original message,
the date that our DM was sent, and the date that we received the response.
"""
import os
from typing import Dict, List

import pandas as pd
import praw
from praw.models.reddit.message import Message

from get_responses import constants
from lib.helper import CURRENT_TIME_STR
from lib.reddit import init_api_access
from message.helper import AUTHOR_PHASE_MESSAGE_IDENTIFIER_STRING

api = init_api_access()


def get_messages_received(api: praw.Reddit) -> List[Message]:
    """Get the messages that we have received from other people.
    
    This assumes that people only DM us if we have DMed them first.
    """
    messages_received = [msg for msg in api.inbox.messages(limit=None)]
    return [
        msg.replies[0] for msg in messages_received if len(msg.replies) > 0
    ]


def is_message_from_author_phase(message: Message) -> bool:
    return AUTHOR_PHASE_MESSAGE_IDENTIFIER_STRING in message.body


def is_message_from_observer_phase(message: Message) -> bool:
    return constants.OBSERVER_PHASE_MESSAGE_IDENTIFIER_STRING in message.body


def identify_message_phase(
    messages_received: List[Message]
) -> Dict[str, List[Message]]:
    author_phase_messages: List[Message] = []
    observer_phase_messages: List[Message] = []

    for message in messages_received:
        if is_message_from_author_phase(message):
            author_phase_messages.append(message)
        elif is_message_from_observer_phase(message):
            observer_phase_messages.append(message)
        else:
            print("Message doesn't appear to be from author/observer phases?")
            print(message.body)
    
    return {
        "author": author_phase_messages,
        "observer": observer_phase_messages
    }


def export_messages(phase_to_messages_map: Dict[str, List[Message]]) -> None:
    """Dumps the messages into a .csv file.
    
    Writes to the /author and /observer subdirectories, creates a directory
    based on the timestamp when this script is run, and dumps the messages
    for the given phase into a file called 'all_{phase}_phase_responses.csv',
    where the .csv file will contain all the DMs of the given phase that
    existed at the point in time defined by the timestamp.
    """
    for phase, messages in phase_to_messages_map.items():
        message_ids = [msg.id for msg in messages]
        message_bodies = [msg.body for msg in messages]
        colnames = ["id", "body"]
        df = pd.DataFrame(zip(message_ids, message_bodies), columns=colnames)
        export_filepath = os.path.join(
            constants.RESPONSES_ROOT_PATH, phase, CURRENT_TIME_STR,
            constants.SESSION_VALIDATED_RESPONSES_FILENAME.format(phase=phase)
        )
        df.to_csv(export_filepath)


if __name__ == "__main__":
    messages_received = get_messages_received(api)
    phase_to_messages_map = identify_message_phase(messages_received)
    export_messages(phase_to_messages_map)
