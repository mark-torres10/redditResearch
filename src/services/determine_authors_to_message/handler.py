"""Determine which authors to message.

Triggers the `message_users` service with the list of authors and messages.
"""
from services.determine_authors_to_message.helper import determine_who_to_message # noqa

def main(event: dict, context: dict) -> list[dict]:
    payloads = determine_who_to_message(**event)
    return payloads
