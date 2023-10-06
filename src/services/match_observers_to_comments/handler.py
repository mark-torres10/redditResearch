"""Matches observers with comments that we want them to score.

Assumes that `get_comments_to_observe/` and `get_valid_possible_observers` have
provided valid comments to score as well as valid observers to ask to score.

Matches observers to comments. Then, triggers the `message_users` service with
the list of authors and messages.
"""
def main(event: dict, context: dict) -> list[dict]:
    # note: we want to assign 1 observer to 1 comment, but we want to assign
    # 1 comment to many observers.
    return []
