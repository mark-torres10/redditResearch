"""Base file for getting Reddit data."""
import csv
import datetime
import json
import logger as log
import os
import requests
from typing import Any, Dict, List, Literal, Optional

from lib.reddit import T1, T3

CURRENT_TIME_STR = datetime.datetime.utcnow().strftime('%Y-%m-%d_%H%M')
REDDIT_BASE_URL = "https://www.reddit.com"

logger = log.getLogger(__name__)

def create_or_use_default_directory(directory: Optional[str] = None) -> None:
    if not directory:
        directory = CURRENT_TIME_STR
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def write_results_to_jsonl(data: List[Dict]) -> None:
    """Writes the results of the request to a JSONL file."""
    dir = create_or_use_default_directory()
    
    file_name = os.path.join(dir, 'results.jsonl')

    with open(file_name, 'w') as f:
        for item in data:
            f.write(json.dumps(item))
            f.write('\n')


def write_metadata_file(metadata_dict: Dict[str, Any]) -> None:
    """Writes metadata to a file.
    
    By default, writes data to a new directory named by the current timestamp.

    Creates a one-row metadata .csv file.
    """
    dir = create_or_use_default_directory()
    
    file_name = os.path.join(dir, 'metadata.csv')

    data = [metadata_dict]
    header_names = list(metadata_dict.keys())

    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header_names)
        writer.writeheader()
        writer.writerows(data)


def get_reddit_data(
    api: requests.Session,
    subreddit: str,
    thread_sort_type: Literal["new", "hot", "controversial"],
    num_threads: int = 5,
    num_posts_per_thread: int = 3,
    output_filepath: Optional[str] = None
):
    """Queries Reddit API and returns a dictionary of dictionaries.
    
    Writes both the JSON of the API result as well as a .csv file containing
    metadata of the request.
    """

    # create an API request that gets the 4 hottest threads in the r/politics
    # subreddit, then gets the 3 latest posts in each thread. Returns a list
    # of dictionaries.
    endpoint = f"{REDDIT_BASE_URL}/r/{subreddit}/{thread_sort_type}.json"
    response = api.get(endpoint, params={"limit": num_threads})

    result_data = []
    for thread in response.json()['data']['children']:
        t3_obj = T3(thread)
        thread_response = api.get(t3_obj.thread_url, params={"limit": num_posts_per_thread})
        for post in thread_response.json()[1]['data']['children']:
            t1_obj = T1(post)
            t3_obj.add_posts_to_thread(comments=[t1_obj])

    metadata_dict = {
        "subreddit": subreddit,
        "thread_sort_type": thread_sort_type,
        "num_threads": num_threads,
        "num_posts_per_thread": num_posts_per_thread,
        "output_filepath": output_filepath,
    }

    write_results_to_jsonl(result_data)
    write_metadata_file(metadata_dict=metadata_dict)
    logger.info("Finished syncing data from Reddit.")

if __name__ == '__main__':
    with requests.Session() as api:
        api.headers = {'User-Agent': 'Mozilla/5.0'}
        get_reddit_data(
            api=api,
            subreddit='politics',
            thread_sort_type='hot',
            num_threads=4,
            num_posts_per_thread=3
        )
