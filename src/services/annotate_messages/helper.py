import datetime
from typing import Literal

from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_table_as_df, write_df_to_database
)
table_name = "annotated_messages"


def annotate_message(
    text: str, phase: Literal["author", "observer"]
) -> tuple(bool, str):
    """Annotate a single message.
    
    Return whether it is a valid message and if so, the associated score.
    """
    user_input = ""
    valid_inputs = ['y', 'n']
    while user_input not in valid_inputs:
        print(f"[Phase: {phase}]  Message: {text}")
        user_input = input("Is this a valid message? ('y', 'n', or 'e' to exit)\t") # noqa
        if user_input == 'y':
            print("Valid message.")
            score = ""
            submit_score = 'n'
            while submit_score == 'n':
                score = input("Please enter their score (e.g., 1123):\t")
                submit_score = input("Submit score? Press any key to enter, or 'n' to redo\t") # noqa
            return (True, score)
        elif user_input == 'n':
            print("Invalid message")
            return (False, "")
        else:
            print(f"Invalid input: {user_input}")


def annotate_messages() -> None:
    # load received messages from DB. Filter out those that are already in the
    # `annotated_messages` table.
    annotated_messages_table_exists = check_if_table_exists(table_name)
    select_fields = ["id", "author_id", "body", "dm_text", "phase"]
    where_filter = f"""
        WHERE id NOT IN (
            SELECT id FROM {table_name}
        )
    """ if annotated_messages_table_exists else ""
    messages_to_annotate_df = load_table_as_df(
        table_name="messages_received",
        select_fields=select_fields,
        where_filter=where_filter
    )
    print(f"Annotating {len(messages_to_annotate_df)} messages...")
    annotation_results: list[tuple] = [
        annotate_message(text, phase)
        for text, phase in zip(
            messages_to_annotate_df["body"],
            messages_to_annotate_df["phase"]
        )
    ]
    is_valid_message_lst: list[bool] = [res[0] for res in annotation_results]
    score_lst: list[str] = [res[1] for res in annotation_results]

    messages_to_annotate_df["is_valid_message"] = is_valid_message_lst
    messages_to_annotate_df["score"] = score_lst
    messages_to_annotate_df["annotation_timestamp"] = (
        datetime.datetime.utcnow().isoformat()
    )
    # write the annotated messages to the DB. Should be straight inserts since
    # we only annotated data that we haven't previously annotated before.
    dump_df_to_csv(df=messages_to_annotate_df, table_name=table_name)
    write_df_to_database(df=messages_to_annotate_df, table_name=table_name)
    print(f"Completed annotation for {len(messages_to_annotate_df)} messages.")
