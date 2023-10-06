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
        score_int = int(score)
        if len(str(score_int)) != 4:
            print(
                f"Invalid author phase score: {score}, needs to have length==4"
            )
            continue
        
        score_outrage = score_int // 1000
        score_int %= 1000

        score_happy = score_int // 100
        score_int %= 100

        score_meta = score_int // 10
        score_int %= 10

        score_regulation = score_int

        author_outrage.append(score_outrage)
        author_happy.append(score_happy)
        author_meta.append(score_meta)
        author_regulation.append(score_regulation)

    return {
        "author_outrage": author_outrage,
        "author_happy": author_happy,
        "author_meta": author_meta,
        "author_regulation": author_regulation
    }


def get_author_self_reported_scores() -> pd.DataFrame:
    validated_responses_csv_files = [
        file for file in os.listdir(VALIDATED_RESPONSES_ROOT_PATH)
        if file.endswith(".csv")
    ]
    message_ids = []
    author_ids = []
    author_screen_names = []
    original_outreach_messages = []
    scores = []

    for csv_file in validated_responses_csv_files:
        fp = os.path.join(VALIDATED_RESPONSES_ROOT_PATH, csv_file)
        df = pd.read_csv(fp)

        # process only the rows that have a valid response
        df = df[df["is_valid_response"] == 1]

        message_ids.extend(df["id"].tolist())
        author_ids.extend(df["author_id"].tolist())
        author_screen_names.extend(df["author_screen_name"].tolist())
        original_outreach_messages.extend(
            df["original_outreach_message_body"].tolist()
        )
        scores.extend(df["scores"].tolist())

    author_scores_map = split_author_phase_score_into_categories(scores)
    author_outrage = author_scores_map["author_outrage"]
    author_happy = author_scores_map["author_happy"]
    author_meta = author_scores_map["author_meta"]
    author_regulation = author_scores_map["author_regulation"]

    colnames = [
        "id", "author_id", "author_screen_name",
        "original_outreach_message_body", "author_outrage", "author_happy",
        "author_meta", "author_regulation"
    ]
    df = pd.DataFrame(
        zip(
            message_ids, author_ids, author_screen_names,
            original_outreach_messages, author_outrage, author_happy,
            author_meta, author_regulation
        ),
        columns=colnames
    )
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process metrics for author phase of study."
    )
    parser.add_argument(
        "--metric", type=str, required=True, help="Type of metric to obtain."
    )
    args = parser.parse_args()

    if args.metric == "scores":
        df = get_author_self_reported_scores()
        create_or_use_default_directory(constants.AUTHOR_PHASE_SCORES_DIR)
        df.to_csv(constants.AUTHOR_PHASE_SCORES_FILEPATH)

    print("Completed gathering author phase metrics")
