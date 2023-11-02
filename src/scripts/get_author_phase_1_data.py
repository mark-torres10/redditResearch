"""
Get author phase data for Study 1.

We previously did this, but only included the DM text. We can pull the actual
post content from the DM text and then add that as a separate column in the df.
"""
import os

import pandas as pd 

from lib.helper import CODE_DIR, CURRENT_TIME_STR

dir_path = os.path.join(CODE_DIR, "data", "analysis")
filename = f"author_phase_study_1_data_{CURRENT_TIME_STR}.csv"
full_fp = os.path.join(dir_path, filename)

legacy_dir_path = os.path.join(
    CODE_DIR, "legacy", "analysis", "author_phase_scores"
)
legacy_filename = "author_phase_scores_2023-08-30_0650.csv"
legacy_full_fp = os.path.join(legacy_dir_path, legacy_filename)


def extract_comment_from_dm(dm_text: str) -> str:
    """Pulls the text between the 
    
    "You posted the following message on {timestamp} in the {subreddit} subreddit:"

    and

    "(link {link})"
    """
    start_pattern = "subreddit:\n"
    end_pattern = "(link"

    start_index = dm_text.find(start_pattern)
    end_index = dm_text.find(end_pattern)

    if start_index != -1 and end_index != -1:
        extracted_text = dm_text[start_index + len(start_pattern):end_index].strip()
        return extracted_text
    else:
        return ""


def extract_comments_from_dms(dm_texts: list[str]) -> list[str]:
    return [
        extract_comment_from_dm(dm_text)
        for dm_text in dm_texts
    ]


def main() -> None:
    # load df
    df = pd.read_csv(legacy_full_fp)
    df["dm_text"] = df["original_outreach_message_body"]
    df.drop("original_outreach_message_body", axis=1, inplace=True)
    dms = df["dm_text"].tolist()
    comments = extract_comments_from_dms(dms)
    df["comment_text"] = comments
    df.to_csv(full_fp, index=False)


if __name__ == "__main__":
    main()
