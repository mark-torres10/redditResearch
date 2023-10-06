import praw
from praw.models.reddit.message import Message

from get_responses.retrieve_messages import get_messages_received
from lib.reddit import init_api_access

api = init_api_access()


def dm_is_for_author_phase(message: Message) -> bool:
    """Returns True if the message is likely from the author phase."""
    if hasattr(message, "body"):
        return (
            "Take a moment to think about what was happening at the time you posted"
            in message.body
        )
    return False

def dm_is_for_observer_phase(message: Message) -> bool:
    """Returns True if the message is likely from the observer phase."""
    if hasattr(message, "body"):
        return (
            "How outraged did you think the message author was on a 1-7 scale?"
            in message.body
        )
    return False

def get_dm_phase(message: Message) -> str:
    """Returns 'author', 'observer', or 'unclear' for the given DM, corresponding
    to whichever phase it is likely to belong to."""
    if dm_is_for_author_phase(message):
        return "author"
    elif dm_is_for_observer_phase(message):
        return "observer"
    else:
        return "unclear"


def get_parent_of_dm(message: Message) -> Message:
    return message.parent


def get_dms(api: praw.Reddit):
    """Get the DMs from Reddit."""

    # given API access, get all the DMs that have a reply

    # for number of messages with replies
    # messages_with_replies = get_messages_received(api=api)

    # for number of DMs sent in total (no limit based on reply)
    messages_with_replies = [msg for msg in api.inbox.messages(limit=None)]

    # only need parents if looking at replies. Don't need parents when looking at
    # the DMs that we ourselves sent.
    """
    message_parents = [
        get_parent_of_dm(message) for message in messages_with_replies
    ]
    """
    message_parents = messages_with_replies

    message_phases = [
        get_dm_phase(message) for message in message_parents
    ]

    # for all the DMs that have a reply, figure out the DM phase that they belong to
    total = len(message_phases)
    num_authors = sum([phase == "author" for phase in message_phases])
    num_observers = sum([phase == "observer" for phase in message_phases])
    num_other = total - num_authors - num_observers

    # return counts
    print(f"number of authors: {num_authors}")
    print(f"number of observers: {num_observers}")
    print(f"number of other: {num_other}")
    print(f"total: {total}")
    breakpoint()


if __name__ == "__main__":
    get_dms(api=api)