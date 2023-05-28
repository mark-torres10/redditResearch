"""Test DMing with dummy data."""
import praw

from lib.reddit import init_api_access
from message import helper
from message.send_messages import create_message_body, send_message

TEST_USER = "YaleSocResearch"
TEST_SUBJECT = helper.AUTHOR_DM_SUBJECT_LINE
TEST_NAME = TEST_USER
TEST_DATE = "2020-01-01"
TEST_BODY = "This is a great day!"

def test_message(api: praw.Reddit, user: str, subject: str, body: str) -> None:
    """Send a test message to self."""
    message_body = create_message_body(TEST_NAME, TEST_DATE, TEST_BODY)
    send_message(
        api=api,
        user=TEST_USER,
        subject=TEST_SUBJECT,
        body=message_body,
    )


if __name__ == "__main__":
    api = init_api_access()
    message_body = create_message_body(TEST_NAME, TEST_DATE, TEST_BODY)
    user = TEST_USER
    subject = TEST_SUBJECT
    test_message(api, user=user, subject=subject, body=message_body)
    print(f"Message successfully sent to {user}!")
