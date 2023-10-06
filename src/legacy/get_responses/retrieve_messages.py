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
from praw.models.reddit.message import Message, SubredditMessage

from get_responses import constants
from lib import helper
from lib.reddit import init_api_access
from message.helper import AUTHOR_PHASE_MESSAGE_IDENTIFIER_STRING

api = init_api_access()
original_outreach_messages_map = {}


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


def is_valid_message_type(message) -> bool:
    """We only want messages that we get directly from other users. We don't
    want messages that come from a moderator or is an automated message
    from a subreddit (class == praw.models.reddit.message.SubredditMessage).
    We want messages from users (class == praw.models.reddit.message.Message).
    """
    return (
        isinstance(message, Message)
        and not isinstance(message, SubredditMessage)
    )


def identify_message_phase(
    messages_received: List[Message]
) -> Dict[str, List[Message]]:
    """Identify the phase (author/observer) that a given message that we have
    received corresponds to.

    We do this by getting the message information from the very first message
    in a given message thread (which should correspond to our initial
    outreach message to the author) and then determining what phase of the
    project the given message corresponds to.
    """
    author_phase_messages: List[Message] = []
    observer_phase_messages: List[Message] = []

    for message in messages_received:
        if not is_valid_message_type(message):
            continue
        original_outreach_message_id = (
            helper.strip_message_obj_prefix_from_message_id(
                message.first_message_name
            )
        )
        original_outreach_message = helper.get_message_obj_from_id(
            api=api, message_id=original_outreach_message_id
        )
        original_outreach_messages_map[original_outreach_message] = {
            "author_screen_name": message.author.name,
            "original_outreach_message_body": original_outreach_message.body
        }
        if is_message_from_author_phase(original_outreach_message):
            author_phase_messages.append(message)
        elif is_message_from_observer_phase(original_outreach_message):
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
        author_ids = [
            helper.strip_redditor_obj_prefix_from_redditor_id(
                msg.author_fullname
            )
            for msg in messages
        ]
        author_screen_names = [
            message["author_screen_name"]
            for message in original_outreach_messages_map.values()
        ]
        created_utc_lst = [msg.created_utc for msg in messages]
        created_utc_string_lst = [
            helper.convert_utc_timestamp_to_datetime_string(created_utc)
            for created_utc in created_utc_lst
        ]
        original_outreach_dm_ids = [
            id for id in original_outreach_messages_map.keys()
        ]
        original_outreach_dm_messages = [
            message["original_outreach_message_body"]
            for message in original_outreach_messages_map.values()
        ]
        colnames = [
            "id", "body", "author_id", "author_screen_name", "created_utc",
            "created_utc_string", "original_outreach_message_id",
            "original_outreach_message_body"
        ]
        df = pd.DataFrame(
            zip(
                message_ids, message_bodies, author_ids, author_screen_names,
                created_utc_lst, created_utc_string_lst,
                original_outreach_dm_ids, original_outreach_dm_messages
            ),
            columns=colnames
        )
        phase_dir = os.path.join(constants.RESPONSES_ROOT_PATH, phase)
        helper.create_or_use_default_directory(phase_dir)
        session_timestamp_dir = os.path.join(
            phase_dir, helper.CURRENT_TIME_STR
        )
        helper.create_or_use_default_directory(session_timestamp_dir)
        export_filepath = os.path.join(
            session_timestamp_dir,
            constants.ALL_RESPONSES_FILENAME.format(phase=phase)
        )
        df.to_csv(export_filepath)

    print(
        "Finished exporting author/observer phase DMs that we've received."
    )


if __name__ == "__main__":
    messages_received = get_messages_received(api)
    phase_to_messages_map = identify_message_phase(messages_received)
    export_messages(phase_to_messages_map)
