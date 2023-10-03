import os
from typing import Optional

import pandas as pd

from lib.helper import CURRENT_TIME_STR

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

def dump_df_to_csv(
    df: pd.DataFrame,
    table_name: str,
    filename: Optional[str] = f"{CURRENT_TIME_STR}.csv"
) -> None:
    """Dumps a pandas df to .csv.
    
    Takes as argument the table name, which will be the folder that the data is
    stored in. The filename is the name of the .csv file. By default, it will
    be determined by the timestamp.
    """
    # create directory for table if it doesn't exist
    table_dir = os.path.join(DATA_DIR, table_name)
    if not os.path.exists(table_dir):
        os.makedirs(table_dir)
    
    # dump df to csv
    df.to_csv(os.path.join(table_dir, filename), index=False)
