"""Base file for getting Reddit data."""
import csv
import datetime
import json
import os
import requests
from typing import Any, Dict, List, Literal, Optional

from lib.redditLogging import RedditLogger
from lib.reddit import T1, T3
from sync.constants import SYNC_METADATA_FILENAME, SYNC_RESULTS_FILENAME

CURRENT_TIME_STR = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H%M")
REDDIT_BASE_URL = "https://www.reddit.com"

logger = RedditLogger(name=__name__)


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


def get_reddit_data(
    api: requests.Session,
    subreddit: str,
    thread_sort_type: Literal["new", "hot", "controversial"],
    num_threads: int = 5,
    num_posts_per_thread: int = 3,
    output_filepath: Optional[str] = None,
) -> None:
    """Queries Reddit API and returns a dictionary of dictionaries.

    Writes both the JSON of the API result as well as a .csv file containing
    metadata of the request.

    Gets the list of threads in the subreddit, then for each thread, gets the
    most recent comments in that thread.
    """

    logger.info(f"Starting syncing Reddit data for subreddit {subreddit}")
    endpoint = f"{REDDIT_BASE_URL}/r/{subreddit}/{thread_sort_type}.json"
    response = api.get(endpoint, params={"limit": num_threads})

    result_data: List[Dict] = []

    thread_list = response.json()["data"]["children"]
    num_threads = len(thread_list)

    for i, thread in enumerate(thread_list):
        logger.info(f"Processing thread {i + 1} out of {num_threads}")
        t3_obj = T3(thread["data"])
        thread_response = api.get(
            t3_obj.thread_url, params={"limit": num_posts_per_thread}
        )
        try:
            comment_posts = thread_response.json()[1]["data"]["children"]
        except (requests.exceptions.JSONDecodeError, KeyError):
            continue
        num_comments = len(comment_posts)
        for j, comment_post in enumerate(comment_posts):
            logger.info(
                f"Processing comment {j} out ouf {num_comments} comments."
            )  # noqa
            if comment_post["kind"] == "t1":
                t1_obj = T1(comment_post["data"])
                t3_obj.add_comments_to_thread(comments=[t1_obj])
        result_data.append(t3_obj.to_dict())

    num_posts_per_synced_thread = [len(thread["posts"]) for thread in result_data]
    avg_num_comments_synced_per_thread = round(
        sum(num_posts_per_synced_thread) / len(num_posts_per_synced_thread), 1
    )

    metadata_dict = {
        "subreddit": subreddit,
        "thread_sort_type": thread_sort_type,
        "num_threads_to_sync": num_threads,
        "num_threads_actually_synced": len(result_data),
        "num_posts_per_thread_to_sync": num_posts_per_thread,
        "avg_num_comments_actually_synced_per_thread": (
            avg_num_comments_synced_per_thread
        ),
        "output_filepath": output_filepath,
    }

    write_results_to_jsonl(result_data)
    write_metadata_file(metadata_dict=metadata_dict)
    logger.info("Finished syncing data from Reddit.")


if __name__ == "__main__":
    with requests.Session() as api:
        api.headers = {"User-Agent": "Mozilla/5.0"}
        get_reddit_data(
            api=api,
            subreddit="politics",
            thread_sort_type="hot",
            num_threads=4,
            num_posts_per_thread=3,
        )
