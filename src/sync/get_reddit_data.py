"""Base file for getting Reddit data.

Example run:
    python get_reddit_data.py politics 5 10
"""
import copy
import csv
import os
import sys
from typing import Any, Dict

from praw.models.reddit.comment import Comment

from lib import helper
from lib.reddit import init_api_access
from lib.helper import convert_utc_timestamp_to_datetime_string
from sync.constants import (
    API_FIELDS_TO_REMAPPED_FIELDS, COLS_TO_IDENTIFY_POST,
    SYNC_METADATA_FILENAME, SYNC_RESULTS_FILENAME
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


if __name__ == "__main__":
    subreddit = sys.argv[1]
    num_submissions = int(sys.argv[2])
    num_comments_per_thread = int(sys.argv[3])

    api = init_api_access()

    subreddit = api.subreddit(subreddit)
    hot_threads = subreddit.hot(limit=num_submissions)

    posts_dict_list = []

    for thread in hot_threads:
        for comment in thread.comments[:num_comments_per_thread]:
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
            posts_dict_list.append(output_dict)

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
