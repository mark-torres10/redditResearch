import re
import time

from praw import Reddit
from praw.exceptions import RedditAPIException


def send_message(api: Reddit, user: str, subject: str, body: str) -> None:
    """Send a message to a user."""
    api.redditor(user).message(subject=subject, message=body)


def catch_rate_limit_and_sleep(e: RedditAPIException) -> None:
    """Catch rate limit exception.
    
    Parses time to wait form the exception string and sleeps for that long.
    Example rate limit exception string:
    "Looks like you've been doing that a lot. Take a break for 2 minutes before
    trying again."
    """
    if e.error_type == "RATELIMIT":
        rate_limit_message = e.message
        number = re.search(r'\b(\d+)\b', rate_limit_message)
        try:
            wait_time_minutes = int(number.group(0))
            print(
                f"Hit rate limit, sleeping for {wait_time_minutes} minutes"
            )
            # sleep for wait time, then wait 30 seconds to send message
            time.sleep(wait_time_minutes * 60)
            time.sleep(1 * 30)
        except Exception:
            print(
                "Unable to parse rate limit message {rate_limit_message}".format(
                    rate_limit_message=rate_limit_message
                )
            )
            return
    else:
        return
