"""Get metrics and results from author phase."""
import argparse
import os
from typing import Dict, List

import pandas as pd

from analysis import constants
from get_responses.constants import VALIDATED_RESPONSES_ROOT_PATH
from lib.helper import create_or_use_default_directory

def split_author_phase_score_into_categories(
    author_phase_scores: List[str]
) -> Dict[str, List]:
    """We receive the author phase scores as a single string of numbers.
    
    We want to split these scores into the questions that they each correspond
    to. This returns a dictionary where the key corresponds to the question
    type and the value corresponds to the score.
    """
    author_outrage = []
    author_happy = []
    author_meta = []
    author_regulation = []

    for score in author_phase_scores:
        if len(score) != 4:
            print(
                f"Invalid author phase score: {score}, needs to have length==4"
            )
            continue
        author_outrage.extend(int(score[0]))
        author_happy.extend(int(score[1]))
        author_meta.extend(int(score[2]))
        author_regulation.extend(int(score[3]))

    return {
        "author_outrage": author_outrage,
        "author_happy": author_happy,
        "author_meta": author_meta,
        "author_regulation": author_regulation
    }


# TODO: need to modify once we have the "phase" column so that we only load the
# validated author/observer phase responses, if specified.
def get_author_self_reported_scores() -> pd.DataFrame:
    validated_responses_csv_files = [
        file for file in os.listdir(VALIDATED_RESPONSES_ROOT_PATH)
        if file.endswith(".csv")
    ]
    message_ids = []
    author_ids = []
    author_screen_names = []
    author_original_posts = []
    scores = []

    for csv_file in validated_responses_csv_files:
        fp = os.path.join(VALIDATED_RESPONSES_ROOT_PATH, csv_file)
        df = pd.DataFrame(fp)

        # process only the rows that have a valid response
        df = df[df["is_valid_response"] == 1]

        message_ids.extend(df["message_id"].tolist())
        author_ids.extend(df["author_id"].tolist())
        author_screen_names.extend(df["author_screen_name"].tolist())
        author_original_posts.extend(df["body"].tolist())
        scores.extend(df["scores"].tolist())

    author_scores_map = split_author_phase_score_into_categories(scores)
    author_outrage = author_scores_map["author_outrage"]
    author_happy = author_scores_map["author_happy"]
    author_meta = author_scores_map["author_meta"]
    author_regulation = author_scores_map["author_regulation"]

    colnames = [
        "message_id", "author_id", "author_screen_name",
        "author_original_post", "author_outrage", "author_happy",
        "author_meta", "author_regulation"
    ]

    df = pd.DataFrame(
        zip(
            message_ids, author_ids, author_screen_names,
            author_original_posts, author_outrage, author_happy, author_meta,
            author_regulation
        ),
        columns=colnames
    )

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process metrics for author phase of study."
    )
    parser.add_argument(
        "--metric", type="str", required=True, help="Type of metric to obtain."
    )
    args = parser.parse_args()

    if args.metric == "scores":
        df = get_author_self_reported_scores()
        create_or_use_default_directory(constants.AUTHOR_PHASE_SCORES_DIR)
        df.to_csv(constants.AUTHOR_PHASE_SCORES_FILEPATH)
