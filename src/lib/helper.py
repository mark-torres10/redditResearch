import datetime
import json
import os
import time
from typing import Dict, List, Optional

import pandas as pd

from lib.reddit import init_api_access

CURRENT_TIME_STR = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H%M")
ROOT_DIR = "/Users/mark/Documents/work/redditResearch/"
CODE_DIR = os.path.join(ROOT_DIR, "src")
BASE_REDDIT_URL = "https://www.reddit.com"

api = init_api_access()


def write_dict_list_to_csv(dict_list: List[Dict], write_path: str) -> None:
    """Given a list of dictionaries, dump the data to .csv."""
    df = pd.DataFrame(dict_list)
    df.to_csv(write_path, index=False)


def read_jsonl_as_list_dicts(filepath: str) -> List[Dict]:
    json_list = []
    with open(filepath, 'r') as file:
        for line in file:
            json_object = json.loads(line)
            json_list.append(json_object)
    return json_list


def track_function_runtime(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        execution_time_seconds = round(end_time - start_time)
        execution_time_minutes = execution_time_seconds // 60
        execution_time_leftover_seconds = execution_time_seconds - (60 * execution_time_minutes)

        print(f"Execution time: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds")

        return result

    return wrapper


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


def transform_permalink_to_link(permalink: str) -> str:
    return BASE_REDDIT_URL + permalink


def write_list_dict_to_jsonl(
    data: List[Dict],
    dir: Optional[str] = None,
    filename: Optional[str] = None
) -> None:
    """Write JSONL """
    if not dir:
        dir = create_or_use_default_directory()

    file_name = os.path.join(dir, filename)

    with open(file_name, "w") as f:
        for item in data:
            f.write(json.dumps(item))
            f.write("\n")


def convert_utc_timestamp_to_datetime_string(utc_time: float) -> str:
    """Given a UTC timestamp, convert to a human-readable date string.
    
    >>> convert_utc_timestamp_to_datetime_string(1679147878.0)
    Sunday, March 19, 2023, at 8:11:18 PM 
    """
    utc_datetime = datetime.datetime.fromtimestamp(utc_time)
    return utc_datetime.strftime("%A, %B %d, %Y, at %I:%M:%S %p")


def get_author_name_from_author_id(author_id: str) -> str:
    try:
        author_name = api.redditor(author_id).name.name 
    except:
        print(
            f"No author for comment for author ID = {author_id}"
            "likely deleted submission..."
        )
        author_name = ''
    return author_name