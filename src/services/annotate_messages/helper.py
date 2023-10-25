import datetime
from typing import Literal

import pandas as pd

from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_table_as_df, write_df_to_database
)
table_name = "annotated_messages"


def is_score_a_number(score: str) -> bool:
    try:
        int(score)
        return True
    except ValueError:
        return False


def validate_score(score: str, phase: str) -> bool:
    valid_score_len = 4 if phase == "author" else 2
    return len(score) == valid_score_len and is_score_a_number(score)


def annotate_message(
    text: str, phase: Literal["author", "observer"]
) -> tuple[bool, str, str]:
    """Annotate a single message.
    
    Return whether it is a valid message and if so, the associated score.
    """
    user_input = ""
    valid_inputs = ['y', 'n']
    while user_input not in valid_inputs:
        print(f"[Phase: {phase}]  Message: {text}")
        valid_phase = input("Is the phase valid? y/n:\t")
        while valid_phase == "n":
            phase = input("Please enter the correct phase (author/observer):\t")
            valid_phase = input("Is the phase valid? y/n:\t")
        user_input = input("Is this a valid message? ('y', 'n', or 'e' to exit)\t") # noqa
        if user_input == 'y':
            print("Valid message.")
            score = ""
            submit_score = 'n'
            while submit_score == 'n':
                is_valid_score = False
                while not is_valid_score:
                    score = input("Please enter their score (e.g., 1123):\t")
                    is_valid_score = validate_score(score=score, phase=phase) # noqa
                    if not is_valid_score:
                        print("Invalid score. Please try again.")
                submit_score = input("Submit score? Press any key to enter, or 'n' to redo\t") # noqa
            return (True, score, phase)
        elif user_input == 'n':
            print("Invalid message")
            return (False, "", phase)
        elif user_input == 'e':
            print("Exiting session...")
            return (None, None, None)
        else:
            print(f"Invalid input: {user_input}")


# TODO: need to make sure that there's only 1 valid annotated message per
# comment. So, for example, if a user gives two possible sets of scores, we
# should only take one of them.
def annotate_messages() -> None:
    # load received messages from DB. Filter out those that are already in the
    # `annotated_messages` table.
    annotated_messages_table_exists = check_if_table_exists(table_name)
    select_fields = [
        "id", "author_id", "comment_id", "body", "comment_text", "dm_text",
        "phase"
    ]
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
    if len(messages_to_annotate_df) == 0:
        print("No messages to annotate.")
        return
    # make messages_to_annotate_df unique on author_id. Dedupe by taking
    # the max of the "id" column (not an index). This will guarantee us the
    # latest DM response.
    message_ids_to_annotate: pd.Series = messages_to_annotate_df.groupby(
        "author_id", as_index=False
    ).agg({"id": "max"})["id"]
    messages_to_annotate_df = messages_to_annotate_df[
        messages_to_annotate_df["id"].isin(message_ids_to_annotate)
    ]
    print(f"Annotating {len(messages_to_annotate_df)} messages...")
    annotation_results: list[tuple] = []
    total_num_messages_to_annotate = len(messages_to_annotate_df)
    for idx, (text, phase) in enumerate(zip(messages_to_annotate_df["body"], messages_to_annotate_df["phase"])): # noqa
        if idx % 10 == 0:
            print('-' * 10)
            print(f"Annotating message idx {idx} out of {total_num_messages_to_annotate}") # noqa
            print('-' * 10)
        annotation_result = annotate_message(text, phase)
        if annotation_result[0] is None:
            print(f"Stopping annotation session early, after {len(annotation_results)} annotations") # noqa
            break
        annotation_results.append(annotation_result)

    is_valid_message_lst: list[bool] = [res[0] for res in annotation_results]
    score_lst: list[str] = [res[1] for res in annotation_results]
    phase_lst: list[str] = [res[2] for res in annotation_results]

    # filter messages_to_annotate_df, return only the first x rows
    if len(annotation_results) != len(messages_to_annotate_df):
        messages_to_annotate_df = messages_to_annotate_df.head(
            len(annotation_results)
        )

    messages_to_annotate_df["is_valid_message"] = is_valid_message_lst
    messages_to_annotate_df["score"] = score_lst
    messages_to_annotate_df["phase"] = phase_lst
    messages_to_annotate_df["annotation_timestamp"] = (
        datetime.datetime.utcnow().isoformat()
    )
    # write the annotated messages to the DB. Should be straight inserts since
    # we only annotated data that we haven't previously annotated before.
    dump_df_to_csv(df=messages_to_annotate_df, table_name=table_name)
    write_df_to_database(
        df=messages_to_annotate_df, table_name=table_name, upsert=True
    )
    print(f"Annotated {len(is_valid_message_lst)} messages, with {sum(is_valid_message_lst)} valid messages.") # noqa
    print(f"Completed annotation for {len(messages_to_annotate_df)} messages.")


if __name__ == "__main__":
    annotate_messages()
