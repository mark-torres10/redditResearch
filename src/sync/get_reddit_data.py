"""Base file for getting Reddit data.

Example run:
    python get_reddit_data.py politics 5 10
"""
import csv
import os
import sys
from typing import Any, Dict

from lib import helper
from lib.reddit import init_api_access
from lib.helper import convert_utc_timestamp_to_datetime_string
from sync.constants import SYNC_METADATA_FILENAME, SYNC_RESULTS_FILENAME


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
            # the author is given by their ID only, we want to hydrate this
            # with the complete author name since we'll need this information
            # when we send DMs.
            if comment.author:
                user_screen_name = api.redditor(comment.author).name.name
                output_dict["author"] = user_screen_name
            else:
                print(
                    f"No author for comment with id={comment.id}"
                    "likely deleted submission..."
                )
                continue
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
