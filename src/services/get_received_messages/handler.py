"""Fetches the DMs that we've received from users who have responded to our
messages.

Records and writes to DB the new DMs.
"""
from services.get_received_messages.helper import handle_received_messages


def main(event: dict, context: dict) -> None:
    handle_received_messages()
