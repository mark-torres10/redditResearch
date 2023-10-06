"""Matches observers with comments that we want them to score.

Assumes that `get_comments_to_observe/` and `get_valid_possible_observers` have
provided valid comments to score as well as valid observers to ask to score.

Matches observers to comments. Then, triggers the `message_users` service with
the list of authors and messages.
"""
import pandas as pd

from data.helper import dump_df_to_csv
from lib.db.sql.helper import load_table_as_df, write_df_to_database

DEFAULT_RECENCY_FILTER = ""
DEFAULT_NUMBER_OF_OBSERVERS_PER_COMMENT = 10

table_name = "comment_to_observer_map"


# TODO: check out src/observer_phase/determine_who_to_message for example
# mapping logic.
def map_comments_to_observers(
    valid_comments_df: pd.DataFrame, valid_observers_df: pd.DataFrame
) -> pd.DataFrame:
    """Maps comments to observers.
    
    Returns df with two columns, both PKs: (comment_id, user_id)
    """
    pass


def main(event: dict, context: dict) -> list[dict]:
    # note: we want to assign 1 observer to 1 comment, but we want to assign
    # 1 comment to many observers.

    # get valid comments and valid observers. Probably can get the df from
    # just the SQL query joining and filtering both tables as necessary.
    valid_comments_df = pd.DataFrame()
    valid_observers_df = pd.DataFrame()

    # then, get a df that assigns X number of users to a comment. Can just
    # have the PK be (comment_id, user_id). Then can write this information
    # to a new table, observer_to_comment
    comment_to_observer_df = map_comments_to_observers(
        valid_comments_df=valid_comments_df,
        valid_observers_df=valid_observers_df
    )
    dump_df_to_csv(df=comment_to_observer_df, table_name=table_name)
    write_df_to_database(df=comment_to_observer_df, table_name=table_name)

    # then, hydrate this with the information that is needed in order to send
    # to the message_users service.
    # NOTE: once the `comment_to_observer_map` table is in the DB, we should be
    # able to do hydration through a series of joins on the related tables. We
    # can then add other information (e.g., creating observer-phase DM) after
    # and then add that as a column.
    hydrated_observer_phase_df = pd.DataFrame()
    payloads = hydrated_observer_phase_df.to_dict("records")
    return payloads
