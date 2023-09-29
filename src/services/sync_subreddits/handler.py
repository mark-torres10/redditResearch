"""Manages fan-out of subreddit syncs.

Returns list of payloads for syncing each individual subreddit.
"""
from services.sync_single_subreddit.helper import (
    DEFAULT_NUM_THREADS, DEFAULT_NUM_COMMENTS_PER_THREAD
)

# TODO: implement
def get_all_subreddits() -> list[str]:
    return []


def main(event: dict, context: dict) -> list[dict]:
    subreddits = event.get("subreddits", "all")
    num_threads = event.get("num_threads", DEFAULT_NUM_THREADS)
    num_comments_per_thread = event.get(
        "num_comments_per_thread", DEFAULT_NUM_COMMENTS_PER_THREAD
    )
    if subreddits == "all":
        subreddits = get_all_subreddits()
    else:
        subreddits = subreddits.split(',')
    payloads = [
        {
            "subreddit": subreddit,
            "num_threads": num_threads,
            "num_comments_per_thread": num_comments_per_thread
        }
        for subreddit in subreddits
    ]
    return payloads
