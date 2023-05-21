import json
import os
from typing import Dict, List

import pandas as pd

ROOT_DIR = "/Users/marktorres/Documents/work/redditResearch/"
CODE_DIR = os.path.join(ROOT_DIR, "src")

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