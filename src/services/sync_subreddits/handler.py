"""Manages fan-out of subreddit syncs.

Returns list of payloads for syncing each individual subreddit.
"""
from services.sync_single_subreddit.helper import (
    DEFAULT_MAX_COMMENTS, DEFAULT_NUM_THREADS, DEFAULT_THREAD_SORT_TYPE
)

# TODO: implement
def get_all_subreddits() -> list[str]:
    return [
        "Politics", "Conservative", "Liberal"
    ]


def main(event: dict, context: dict) -> list[dict]:
    subreddits = event.get("subreddits", "all")
    num_threads = event.get("num_threads", DEFAULT_NUM_THREADS)
    thread_sort_type = event.get("thread_sort_type", DEFAULT_THREAD_SORT_TYPE)
    max_total_comments = event.get("max_total_comments", DEFAULT_MAX_COMMENTS)
    if subreddits == "all":
        subreddits = get_all_subreddits()
    else:
        subreddits = subreddits.split(',')
    payloads = [
        {
            "subreddit": subreddit,
            "num_threads": num_threads,
            "thread_sort_type": thread_sort_type,
            "max_total_comments": max_total_comments
        }
        for subreddit in subreddits
    ]
    return payloads
