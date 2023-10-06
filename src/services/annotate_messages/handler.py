"""Given the DMs that we've received, annotate the DMs.

Checks to see if the DMs have valid user-reported scores from either the author
or the observer phase. If there are valid scores, write down the valid scores.

Writes the annotated DMs to the DB.
"""
from services.annotate_messages.helper import annotate_messages


def main(event: dict, context: dict) -> None:
    annotate_messages()
