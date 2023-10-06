import pandas as pd

from services.get_received_messages.helper import table_name as received_messages_table_name # noqa

table_name = "annotated_messages"


def annotate_message(text: str) -> tuple(bool, str):
    """Annotate a single message.
    
    Return whether it is a valid message and if so, the associated score.
    """
    return False, ""


# TODO: annotate the messages. Annotate messages from the `messages_received`
# table that aren't in the `annotated_messages` table. These should be joined
# by PK. The `annotated_messages` table should just have the ID, message,
# whether or not it's a valid message, and then a string with the score.
def annotate_messages() -> None:
    # load received messages from DB. Filter out those that are already in the
    # `annotated_messages` table.

    messages_to_annotate = pd.DataFrame()
    # annotate the messages.
    annotation_results: list[tuple] = []
    is_valid_message_lst = [res[0] for res in annotation_results]
    score_lst = [res[1] for res in annotation_results]

    # write the annotated messages to the DB.
    pass
