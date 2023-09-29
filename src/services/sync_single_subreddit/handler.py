"""Manages the sync of a single subreddit.

Takes as input the subreddit name, and optionally, the
number of threads and number of comments per thread to sync.
"""
from lib.reddit import init_api_access
from services.sync_single_subreddit.helper import (
    DEFAULT_NUM_THREADS, DEFAULT_NUM_COMMENTS_PER_THREAD,
    sync_comments_from_one_subreddit
)

api = init_api_access()


def main(event: dict, context: dict) -> int:
    subreddit = event["subreddit"]
    num_threads = event.get("num_threads", DEFAULT_NUM_THREADS)
    num_comments_per_thread = event.get(
        "num_comments_per_thread", DEFAULT_NUM_COMMENTS_PER_THREAD
    )
    sync_comments_from_one_subreddit(
        api=api,
        subreddit=subreddit,
        num_threads=num_threads,
        num_comments_per_thread=num_comments_per_thread
    )
    return 0
