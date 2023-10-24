"""Analyzes data from the database and returns the results to the user.

Key desired outcomes:
1. Pull basic metrics (e.g., number of comments synced, number of people DMed,
number of results per each phase, etc.)
2. Pull responses.
"""
import pandas as pd


def get_author_phase_data() -> pd.DataFrame:
    """Pull author phase data from Postgres DB.
    
    Returns the following columns:
        - author_id
        - comment_id
        - comment
        - dm_text
        - score
    """
    return pd.DataFrame()


def analyse_data() -> None:
    pass
