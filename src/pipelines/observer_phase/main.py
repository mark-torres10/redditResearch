"""Manages observer phase of the project. Pulls annotated author-phase
messages, finds valid observers, and then matches messages to observers.

Note: as a first pass we strictly don't enforce observers having to be in the
same subreddit as the original author message. This is because (1) it's likely
that an observer frequents multiple related subreddits and (2) since they're
all political subreddits, they're likely pretty similar already.
"""
from lib.helper import track_function_runtime
from services.get_comments_to_observe.handler import (
    main as get_comments_to_observe
)
from services.get_valid_possible_observers.handler import (
    main as get_valid_possible_observers
)
from services.match_observers_to_comments.handler import (
    main as match_observers_to_comments
)
from services.message_users.handler import main as message_users
from services.message_users.helper import load_pending_message_payloads


@track_function_runtime
def main(event: dict, context: dict) -> None:
    """Main function for observer phase."""
    print("Starting observer phase...")
    message_observers = event.pop("message_observers", False)
    batch_size = event.pop("batch_size", None)
    match_observers_only = event.pop("match_observers_only", False)
    message_observers_only = event.pop("message_observers_only", False)
    if message_observers_only:
        message_observers = True
    if not message_observers_only:
        if not match_observers_only:
            get_comments_to_observe()
            get_valid_possible_observers()
        match_observers_to_comments()
    if message_observers and not match_observers_only:
        user_message_payloads = load_pending_message_payloads(phase="observer")
        # TODO: check for duplication? Filtered out duplicate payloads but that
        # shouldn't exist???
        message_event = {
            "phase": "observer",
            "user_message_payloads": user_message_payloads,
            "batch_size": batch_size
        }
        message_context = {}
        message_users(event=message_event, context=message_context)
    print("Completed observer phase.")


if __name__ == "__main__":
    event = {
        "message_observers_only": True,
        "batch_size": 50
    }
    context = {}
    main(event=event, context=context)
