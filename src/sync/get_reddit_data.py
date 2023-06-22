"""Base file for getting Reddit data.

Example run:
    python get_reddit_data.py politics 5 10
"""
import csv
import datetime
import json
import os
import sys
from typing import Any, Dict, List, Optional

from lib.redditLogging import RedditLogger
from lib.reddit import init_api_access
from ml.transformations import convert_utc_timestamp_to_datetime_string
from sync.constants import SYNC_METADATA_FILENAME, SYNC_RESULTS_FILENAME

CURRENT_TIME_STR = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H%M")
REDDIT_BASE_URL = "https://www.reddit.com"

logger = RedditLogger(name=__name__)

def is_json_serializable(value):
    try:
        json.dumps(value, cls=json.JSONEncoder)
        return True
    except (TypeError, OverflowError):
        return False

def create_or_use_default_directory(directory: Optional[str] = None) -> str:
    if not directory:
        directory = CURRENT_TIME_STR
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def write_results_to_jsonl(data: List[Dict]) -> None:
    """Writes the results of the request to a JSONL file."""
    dir = create_or_use_default_directory()

    file_name = os.path.join(dir, SYNC_RESULTS_FILENAME)

    with open(file_name, "w") as f:
        for item in data:
            f.write(json.dumps(item))
            f.write("\n")


def write_metadata_file(metadata_dict: Dict[str, Any]) -> None:
    """Writes metadata to a file.

    By default, writes data to a new directory named by the current timestamp.

    Creates a one-row metadata .csv file.
    """
    dir = create_or_use_default_directory()

    file_name = os.path.join(dir, SYNC_METADATA_FILENAME)

    data = [metadata_dict]
    header_names = list(metadata_dict.keys())

    with open(file_name, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header_names)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    subreddit = sys.argv[1]
    num_threads = int(sys.argv[2])
    num_posts_per_thread = int(sys.argv[3])

    api = init_api_access()

    subreddit = api.subreddit(subreddit)
    # TODO: support other types, such as controversial/new, not just "hot"
    hot_threads = subreddit.hot(limit=num_threads)

    posts_dict_list = []

    for thread in hot_threads:
        # Retrieve the top posts in each thread
        for submission in thread.comments[:num_posts_per_thread]:
            print(f"Post: {submission.body}")
            created_utc_string = convert_utc_timestamp_to_datetime_string(
                submission.created_utc
            )
            print(f"Created at: {created_utc_string}")
            print("-----")
            output_dict = {}
            for field, value in submission.__dict__.items():
                if is_json_serializable(value):
                    output_dict[field] = value

            # add modified fields
            if submission.author:
                user_screen_name = api.redditor(submission.author).name.name
                output_dict["author"] = user_screen_name
            else:
                print(f"No author for post with id={submission.id}, likely deleted submission...") # noqa
                continue
    
            posts_dict_list.append(output_dict)

    metadata_dict = {
        "subreddit": subreddit,
        "thread_sort_type": "hot",
        "num_threads": num_threads,
        "num_posts_per_thread": num_posts_per_thread,
        "num_total_posts_synced": len(posts_dict_list)
    }

    write_results_to_jsonl(posts_dict_list)
    write_metadata_file(metadata_dict=metadata_dict)
    print(
        f"Finished syncing data from Reddit for timestamp {CURRENT_TIME_STR}"
    )
