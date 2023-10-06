from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_table_as_df, write_df_to_database
)

table_name = "comments_available_to_evaluate_for_observer_phase"

def get_comments_to_observe() -> None:
    """Get comments to use for observer scoring.
    
    Loads in annotated messages and references that against comments that have
    already been used for the observer phase. For valid annotated author-phase 
    messages, we want to add those comments to the table of comments to observe
    and then make those all available for the observer phase.
    
    Note that this is an append-only table. We want to make available all the
    possible author-phase comments. The filtering logic for determining which
    comments will actually be used for the observer phase is done in the
    mapping service.
    """
    comments_for_observer_phase_table_exists = check_if_table_exists(table_name) # noqa
    select_fields = [
        "comment_id", "author_id",
        "annotated_messages.id AS annotated_message_id", "comment_text",
        "c.created_utc_string", "is_valid_message"
    ]
    where_filter = f"""
        WHERE comment_id NOT in (
            SELECT comment_id FROM {table_name}
        )
    """ if comments_for_observer_phase_table_exists else ""
    # hydrate comments to observe with information from the comments table.
    # in theory, all comments should exist in the `comments` table.
    join_query = f"""
        INNER JOIN (
            SELECT id, created_utc_string
            FROM comments
        ) AS c
        ON c.id = annotated_messages.comment_id
    """
    comments_to_observe_df = load_table_as_df(
        table_name="annotated_messages",
        select_fields=select_fields,
        join_query=join_query,
        where_filter=where_filter
    )
    # filter out rows whose is_valid_message = False, so we only get rows
    # whose authors provided valid author-phase responses.
    comments_to_observe_df = comments_to_observe_df[
        comments_to_observe_df["is_valid_message"]
    ]

    # write to .csv and DB
    dump_df_to_csv(comments_to_observe_df, table_name)
    write_df_to_database(comments_to_observe_df, table_name)
    print("Completed getting comments to make available for observer phase.")
