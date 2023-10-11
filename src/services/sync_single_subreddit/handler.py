"""Manages the sync of a single subreddit.

Takes as input the subreddit name, and optionally, the
number of threads and number of comments per thread to sync.
"""
from lib.reddit import init_api_access
from services.sync_single_subreddit.helper import (
    DEFAULT_MAX_COMMENTS, DEFAULT_NUM_THREADS, DEFAULT_THREAD_SORT_TYPE,
    sync_comments_from_one_subreddit
)

api = init_api_access()


def main(event: dict, context: dict) -> int:
    subreddit = event["subreddit"]
    num_threads = event.get("num_threads", DEFAULT_NUM_THREADS)
    thread_sort_type = event.get("thread_sort_type", DEFAULT_THREAD_SORT_TYPE)
    max_total_comments = event.get("max_total_comments", DEFAULT_MAX_COMMENTS)
    objects_to_sync = event.get(
        "object_to_sync", ["subreddits", "threads", "comments", "users"]
    )
    sync_comments_from_one_subreddit(
        api=api,
        subreddit=subreddit,
        num_threads=num_threads,
        max_total_comments=max_total_comments,
        thread_sort_type=thread_sort_type,
        objects_to_sync=objects_to_sync
    )
    return 0
