"""Manages author phase of project. Determine who to message, then send the
relevant messages.
"""
from lib.helper import track_function_runtime
from services.determine_authors_to_message.handler import main as determine_authors_to_message # noqa
from services.message_users.handler import main as message_users


@track_function_runtime
def main(event: dict, context: dict) -> None:
    send_messages = event.pop("send_messages")
    # determine who to message. Each payload has the metadata needed to message
    # the user as well as the DM to send to the user.
    user_message_payloads: list[dict] = determine_authors_to_message(event, context) # noqa

    if send_messages:
        message_event = {
            "phase": "author",
            "user_message_payloads": user_message_payloads,
            "batch_size": event.get("batch_size", None)
        }
        message_context = {}
        print(f"Running message_users with the event {message_event}")
        message_users(event=message_event, context=message_context)
    print("Completed author phase.")


if __name__ == "__main__":
    # TODO: write out what the payload args need to be for the case where we
    # want to do assignment of newly classified users
    # TODO: then, write out what the payload should be once we've done this
    # assignment, the DB is updated, and all we want to do is send the DMs.
    # TODO: add these in the runbook.
    """
    event = {
        "batch_size": 50, # change to 50 later.
        "use_only_pending_author_phase_messages": True,
        "add_pending_author_phase_messages": None,
        "max_num_assign_to_message": None,
        "max_ratio_assign_to_message": None,
        "reassign_unmessaged_users": None,
        "send_messages": True
    }
    """

    # to reassign users that have been messaged, you need a payload like this:
    """
    event = {
        "use_only_pending_author_phase_messages": False,
        "add_pending_author_phase_messages": False,
        "max_num_assign_to_message": 1000,
        "max_ratio_assign_to_message": None,
        "reassign_unmessaged_users": True,
        "send_messages": False 
    }
    """
    # to send messages, you only need a payload like this:
    # note: this assumes that the `determine_authors_to_message` function has
    # been run already.

    event = {
        "batch_size": 50,
        "use_only_pending_author_phase_messages": True,
        "send_messages": True
    }

    context = {}
    main(event=event, context=context)
