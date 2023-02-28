from typing import Dict, List

import pandas as pd

ROOT_DIR = "/Users/mark/Documents/work/redditResearch/"

def write_dict_list_to_csv(dict_list: List[Dict], write_path: str) -> None:
    """Given a list of dictionaries, dump the data to .csv."""
    df = pd.DataFrame(dict_list)
    df.to_csv(write_path, index=False)
