"""Test DMing with dummy data."""
import time

import pandas as pd
import praw

from lib.reddit import init_api_access
from message import helper
from message.send_messages import create_message_body, send_message

TEST_ID = "1234"
TEST_USER = "mpt97262"
TEST_SUBJECT = helper.AUTHOR_DM_SUBJECT_LINE
TEST_NAME = TEST_USER
TEST_DATE = "2020-01-01"
TEST_BODY = "This is a great day!"
TEST_SUBREDDIT = "r/politics" # TODO: need to add subreddit in the data dumps
TEST_PERMALINK = "https://test-link"

DUMMY_FILENAME = "test_posts_to_who_to_message.csv"

def create_dummy_data() -> None:
    test_data = {
        "id": TEST_ID,
        "author": TEST_USER,
        "author_fullname": TEST_USER,
        "permalink": TEST_PERMALINK,
        "subreddit": TEST_SUBREDDIT,
        "body": TEST_BODY,
        "label": 1,
        "prob": 0.5,
        "to_message_flag": 1
    }

    df = pd.DataFrame([test_data])
    df.to_csv(DUMMY_FILENAME, index=False)


def load_dummy_data() -> pd.DataFrame:
    return pd.read_csv(DUMMY_FILENAME)


def test_message(
    api: praw.Reddit, user: str, subject: str, body: str, subreddit: str,
    permalink: str
) -> None:
    """Send a test message to self."""
    send_message(
        api=api,
        user=user,
        subject=subject,
        body=body
    )


def send_test_message(df: pd.DataFrame):
    num_messages_to_send = df["to_message_flag"].sum()
    print(f"Expecting to send {num_messages_to_send} messages...")
    start = time.time()
    print("Starting DMing...")
    for count, row in enumerate(df.iterrows()):
        print(f"Sending DM {count+1} out of {num_messages_to_send}")
        row_dict = row[1].to_dict()
        if row_dict["to_message_flag"] == 0:
            print("Skipping DMing, to_message_flag is 0.")
            continue
        message_body = create_message_body(
            name=row_dict["author"],
            date=TEST_DATE,
            post=row_dict["body"],
            subreddit=row_dict["subreddit"],
            permalink=row_dict["permalink"]
        )
        test_message(
            api=api,
            user=row_dict["author"],
            subject=TEST_SUBJECT,
            body=message_body,
            subreddit=row_dict["subreddit"],
            permalink=row_dict["permalink"]
        )
        print(f"Message successfully sent to {row_dict['author']}!")
    print(f"Completed sending test messages.")
    end = time.time()
    minutes = (end - start) // 60
    seconds = (end - start) - (60 * minutes)
    print(f"Completed DMing in {minutes} minutes and {seconds} seconds.")

if __name__ == "__main__":
    create_dummy_data()
    df = load_dummy_data()
    api = init_api_access()
    send_test_message(df)
    print(f"Completed sending test messages.")
