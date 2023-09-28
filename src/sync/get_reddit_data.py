"""Base file for getting Reddit data.

Example run:
    python get_reddit_data.py politics 5 10
"""
import argparse
import copy
import csv
import os
import sys
from typing import Any, Dict, List

import praw
from praw.models.reddit.comment import Comment

from lib import helper
from lib.reddit import init_api_access
from lib.helper import convert_utc_timestamp_to_datetime_string
from sync.constants import (
    API_FIELDS_TO_REMAPPED_FIELDS, SYNC_METADATA_FILENAME,
    SYNC_RESULTS_FILENAME
)
from sync.transformations import MAP_COL_TO_TRANSFORMATION


def write_metadata_file(metadata_dict: Dict[str, Any]) -> None:
    """Writes metadata to a file.

    By default, writes data to a new directory named by the current timestamp.

    Creates a one-row metadata .csv file.
    """
    dir = helper.create_or_use_default_directory()
    file_name = os.path.join(dir, SYNC_METADATA_FILENAME)
    data = [metadata_dict]
    header_names = list(metadata_dict.keys())

    with open(file_name, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header_names)
        writer.writeheader()
        writer.writerows(data)


def transform_fields(comment: Comment, output_dict: Dict) -> Dict:
    """Given the `comment` object and the `output_dict` available,
    transform certain fields and add them the output dictionary to include."""
    transformed_dict = copy.deepcopy(output_dict)

    # columns to rename in the `output` dictionary.
    for api_field, remapped_name in API_FIELDS_TO_REMAPPED_FIELDS.items():
        transformed_dict[remapped_name] = comment[api_field]
    
    # transform certain values
    for (enrichment_col, transformation_dict) in (
        MAP_COL_TO_TRANSFORMATION.items()
    ):
        col_input = transformation_dict["original_col"]
        transform_func = transformation_dict["transform_func"]
        transformed_dict[enrichment_col] = transform_func(comment[col_input])

    return {
        **output_dict,
        **transformed_dict
    }


def process_single_comment(comment: Comment) -> Dict:
    created_utc_string = convert_utc_timestamp_to_datetime_string(
        comment.created_utc
    )
    print(f"Comment: {comment.body}\nCreated at: {created_utc_string}")
    print("-----")
    output_dict = {}
    for field, value in comment.__dict__.items():
        # we want to dump to a .jsonl file eventually, so we want to
        # verify that the value is JSON-serializable.
        if helper.is_json_serializable(value):
            output_dict[field] = value
    output_dict = transform_fields(
        comment=comment, output_dict=output_dict
    )
    return output_dict


def process_comments_from_thread(
    thread: praw.models.reddit.submission.Submission,
    num_comments_per_thread: int
) -> List[Dict]:
    return [
        process_single_comment(comment)
        for comment in thread.comments[:num_comments_per_thread]
    ]


def process_comments_from_threads(
    threads: List[praw.models.reddit.submission.Submission],
    num_comments_per_thread: int
) -> List[Dict]:
    """Given a list of threads, process the comments from each thread."""
    return [
        process_comments_from_thread(thread, num_comments_per_thread)
        for thread in threads
    ]


def sync_comments_from_one_subreddit(
    subreddit: str, num_submissions: int, num_comments_per_thread: int
) -> None:
    """Syncs the comments from one subreddit.
    
    Does so by grabbing threads and looking at the most recent
    comments in a given thread."""
    api = init_api_access()

    subreddit = api.subreddit(subreddit)
    hot_threads = subreddit.hot(limit=num_submissions)
    posts_dict_list = process_comments_from_threads(hot_threads, num_comments_per_thread)

    metadata_dict = {
        "subreddit": subreddit,
        "thread_sort_type": "hot",
        "num_threads": num_submissions,
        "num_posts_per_thread": num_comments_per_thread,
        "num_total_posts_synced": len(posts_dict_list)
    }

    helper.write_list_dict_to_jsonl(posts_dict_list, SYNC_RESULTS_FILENAME)
    write_metadata_file(metadata_dict=metadata_dict)
    print(
        f"Finished syncing data from Reddit for timestamp {helper.CURRENT_TIME_STR}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script for getting data from Reddit."
    )
    parser.add_argument(
        "--subreddit", type=str, required=True, help="Subreddit to sync from"
    )
    parser.add_argument(
        "--num_submissions", type=int, required=False, default=5
    )
    parser.add_argument(
        "--num_comments_per_thread", type=int, required=False, default=10
    )
    args = parser.parse_args()
    sync_comments_from_one_subreddit(
        subreddit=args.subreddit,
        num_submissions=args.num_submissions,
        num_comments_per_thread=args.num_comments_per_thread
    )
