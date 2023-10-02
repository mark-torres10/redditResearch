"""Manages the sync of a single subreddit.

Takes as input the subreddit name, and optionally, the
number of threads and number of comments per thread to sync.
"""
from lib.reddit import init_api_access
from services.sync_single_subreddit.helper import (
    DEFAULT_NUM_THREADS, DEFAULT_THREAD_SORT_TYPE,
    sync_comments_from_one_subreddit
)

api = init_api_access()


def main(event: dict, context: dict) -> int:
    subreddit = event["subreddit"]
    num_threads = event.get("num_threads", DEFAULT_NUM_THREADS)
    thread_sort_type = event.get("thread_sort_type", DEFAULT_THREAD_SORT_TYPE)
    sync_comments_from_one_subreddit(
        api=api,
        subreddit=subreddit,
        num_threads=num_threads,
        thread_sort_type=thread_sort_type
    )
    return 0
