"""Manages author phase of project. Determine who to message, then send the
relevant messages.
"""
from lib.helper import track_function_runtime
from services.determine_authors_to_message.handler import main as determine_authors_to_message # noqa
from services.message_users.handler import main as message_users


@track_function_runtime
def main(send_messages: bool) -> None:
    event = {
        "batch_size": None,
        "use_only_pending_author_phase_messages": False,
        "max_num_assign_to_message": None,
        "max_ratio_assign_to_message": None
    }
    context = {}

    # determine who to message. Each payload has the metadata needed to message
    # the user as well as the DM to send to the user.
    user_message_payloads: list[dict] = determine_authors_to_message(event, context) # noqa

    if send_messages:
        message_event = {
            "phase": "author",
            "user_message_payloads": user_message_payloads
        }
        message_context = {}
        message_users(event=message_event, context=message_context)
    print("Completed author phase.")


if __name__ == "__main__":
    main()
