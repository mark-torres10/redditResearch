"""Messages a single user.

Takes as input the username of the user to message, and the message
to send to the user.
"""
from typing import Union

from lib.reddit import init_api_access
from services.message_single_user.helper import send_message

api = init_api_access()


def main(event: dict, context: dict) -> Union[int, Exception]:
    """Tries to send a single message. Returns message status."""
    author_screen_name = event["author_screen_name"]
    message_subject = event["message_subject"]
    message_body = event["message_body"]
    try:
        send_message(
            api=api, user=author_screen_name, subject=message_subject,
            body=message_body
        )
        return 0
    except Exception as e:
        return e
