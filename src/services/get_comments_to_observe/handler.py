"""Get comments that we want observers to score.

Assumes that we have comments which the authors gave their outrage scores for.
Also assumes that we've already annotated those comments and figured out which
ones have valid scores. This service takes those comments and determines which
ones we then want our observers to score.
"""
from services.get_comments_to_observe.helper import get_comments_to_observe


def main(event: dict, context: dict) -> None:
    get_comments_to_observe()
