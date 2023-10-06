"""Messages a single user.

Takes as input the username of the user to message, and the message
to send to the user.
"""


def main(event: dict, context: dict) -> int:
    """Tries to send a single message. Returns message status."""
    try:
        return 0
    except Exception as e:
        return -1

