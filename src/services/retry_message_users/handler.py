"""Manages the collection of which users to retry and then sends these users
and messages to the retry_message_users service.
"""
from services.message_users.handler import main as message_users

MAX_NUMBER_RETRIES = 3


def main(event: dict, context: dict) -> None:
    user_message_payloads = event.get("user_message_payloads", [])
    retry_count = event.get("retry_count", 0)
    if retry_count > MAX_NUMBER_RETRIES:
        print("Exceeded max number of retries. Exiting...")
        return
    # TODO: do any other retry logic or filtering if necessary
    message_users(event=event, context=context)
