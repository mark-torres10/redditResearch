"""Manages fan-out of messaging users, for both author and observer phases.

Takes as input the users to message as well as what to message them. Returns
list of payloads for messaging each individual user.
"""
from services.retry_message_users.handler import main as retry_message_users
from services.message_single_user.handler import main as message_single_user


def main(event: dict, context: dict) -> None:
    payloads = event.get("user_message_payloads", [])
    retry_count = event.get("retry_count", 0)
    successful_messages = []
    messages_to_retry = []
    if not payloads:
        print("No users to message. Exiting...")
        return
    for payload in payloads:
        status = message_single_user(payload, context)
        if status == 0:
            successful_messages.append(payload)
        else: 
            messages_to_retry.append(payload)
    # TODO: Export successful results as .csv, write to DB.
    # handle retry logic, if applicable.
    print(f"After running, need to retry {len(messages_to_retry)} messages.")
    retry_event = {
        "user_message_payloads": messages_to_retry,
        "retry_count": retry_count + 1,
    }
    retry_context = {}
    retry_message_users(event=retry_event, context=retry_context)
    print("Completed messaging users.")
