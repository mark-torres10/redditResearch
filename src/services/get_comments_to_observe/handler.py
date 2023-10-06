"""Get comments that we want observers to score.

Assumes that we have comments which the authors gave their outrage scores for.
Also assumes that we've already annotated those comments and figured out which
ones have valid scores. This service takes those comments and determines which
ones we then want our observers to score.
"""
def main(event: dict, context: dict) -> None:
    # note: presumably after every run, the table is rewritten, since the
    # comments to observe in one run won't be observed in the next run. In this
    # case, the .csv files become the ground source of truth across runs.
    pass
