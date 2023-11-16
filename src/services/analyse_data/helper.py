"""Analyzes data from the database and returns the results to the user.

Key desired outcomes:
1. Pull basic metrics (e.g., number of comments synced, number of people DMed,
number of results per each phase, etc.)
2. Pull responses.
"""
import os

import pandas as pd

from data.helper import DATA_DIR
from lib.db.sql.helper import load_table_as_df
from lib.helper import CURRENT_TIME_STR


output_dir = os.path.join(DATA_DIR, "analysis")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
author_phase_filename = f"author_phase_data_{CURRENT_TIME_STR}.csv"
author_phase_fp = os.path.join(output_dir, author_phase_filename)


# exclude buggy annotations
filter_for_old_annotations = """
    AND annotation_timestamp NOT IN (
        SELECT DISTINCT annotation_timestamp
        FROM annotated_messages
        ORDER BY annotation_timestamp DESC
        LIMIT 2
    )
"""

# exclude inital study phase results. For these ones, comment_text and dm_text
# will be None since these are the initial messages that were sent out and
# we don't have these records
filter_old_messages = """
    AND comment_id IS NOT NULL
    AND comment_text IS NOT NULL
    AND dm_text IS NOT NULL
"""


def get_author_phase_data() -> pd.DataFrame:
    """Pull author phase data from Postgres DB.
    
    Returns the following columns:
        - author_id
        - comment_id
        - comment
        - dm_text
        - score
    """
    where_filter = f"""
        WHERE phase = 'author'
        AND is_valid_message = 'true'
        {filter_for_old_annotations}
        {filter_old_messages}
    """
    df = load_table_as_df(
        table_name="annotated_messages",
        select_fields=[
            "author_id", "comment_id", "comment_text", "dm_text", "score"
        ],
        where_filter=where_filter
    )
    # break up the scores into the individual components
    scores = df["score"].tolist()
    scores_str_list = [str(int(score)) for score in scores]
    author_outrage = [score[0] for score in scores_str_list]
    author_happy = [score[1] for score in scores_str_list]
    author_meta = [score[2] for score in scores_str_list]
    author_regulation = [score[3] for score in scores_str_list]

    df["author_outrage"] = author_outrage
    df["author_happy"] = author_happy
    df["author_meta"] = author_meta
    df["author_regulation"] = author_regulation
    df = df.drop(columns=["score"])

    return df


def write_author_phase_scores(author_phase_df: pd.DataFrame) -> None:
    pass


def process_updated_author_phase_scores() -> None:
    author_phase_df = get_author_phase_data()
    write_author_phase_scores(author_phase_df)


def analyse_data() -> None:
    author_phase_df = get_author_phase_data()
    #author_phase_df.to_csv(author_phase_fp, index=False)
    print(f"Successfully exported author phase data to {author_phase_fp}")
