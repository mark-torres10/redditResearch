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


@track_function_runtime
def main() -> None:
    """Main function for observer phase."""
    print("Starting observer phase...")
    event = {}
    context = {}
    get_comments_to_observe(event, context)
    get_valid_possible_observers(event, context)
    user_message_payloads: list[dict] = match_observers_to_comments(event, context) # noqa
    message_event = {
        "phase": "observer",
        "user_message_payloads": user_message_payloads
    }
    message_context = {}
    message_users(event=message_event, context=message_context)
    print("Completed observer phase.")
