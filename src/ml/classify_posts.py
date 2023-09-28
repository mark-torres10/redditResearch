"""
Classifies a previously synced set of posts. Dumps the labels in a new
directory.

Loads previous files, transforms the .jsonl to a df, classifies the comment
bodies for each body, adds the classification probs and labels to the df, and
exports this to a .csv file.

Example usage:
    python classify_posts.py 2023-03-20_1438
"""
import argparse
import sys
from typing import Dict, List, Tuple
import os

from keras.models import Model
import pandas as pd

from lib.helper import (
    create_or_use_default_directory, read_jsonl_as_list_dicts,
    track_function_runtime
)
from lib.redditLogging import RedditLogger
from ml.constants import (
    LABEL_COL, LABELED_DATA_FILENAME, ML_ROOT_PATH, PROB_COL
)
from ml.inference import (
    classify_text, load_default_embedding_and_tokenizer
)
from sync.constants import (
    COLS_TO_IDENTIFY_POST, POST_TEXT_COLNAME, SYNC_RESULTS_FILENAME,
    SYNC_ROOT_PATH
)

logger = RedditLogger(name=__name__)


@track_function_runtime
def perform_classifications(
    texts_list: List[str],
    embedding: Model,
    tokenizer: Tuple
) -> Tuple[List[List[int]], List[int]]:
    probs: List[List[int]] = [] # nested list, e.g., [[0.2], [0.5]]
    labels: List[int] = [] # e.g., [0, 1]

    for text in texts_list:
        prob, label = classify_text(text, embedding, tokenizer)
        probs.append(prob)
        labels.append(label)

    return probs, labels


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script for classifying previously synced posts."
    )
    parser.add_argument(
        "--timestamp", type=str, required=True
    )
    args = parser.parse_args()
    load_timestamp = args.timestamp
    timestamp_dir = os.path.join(SYNC_ROOT_PATH, load_timestamp, '')
    if not os.path.exists(timestamp_dir):
        logger.error(
            f"Sync data timestamp directory {timestamp_dir} does not exist"
        )
        sys.exit(1)

    sync_data_filepath = os.path.join(timestamp_dir, SYNC_RESULTS_FILENAME)
    sync_data_jsons = read_jsonl_as_list_dicts(sync_data_filepath)
    sync_data = pd.DataFrame(sync_data_jsons)

    post_dict_subset: List[Dict] = []  # get only cols needed to identify post
    texts_list: List[str] = []  # list of texts to classify

    for row_tuple in sync_data.iterrows():
        idx, row = row_tuple
        if idx % 10 == 0:
            print(
                "Processing row {idx} out of {total}".format(
                    idx=idx, total=len(sync_data)
                )
            )
        output_dict = {}
        for field in COLS_TO_IDENTIFY_POST:
            output_dict[field] = row[field]
        post_dict_subset.append(output_dict)
        texts_list.append(row[POST_TEXT_COLNAME])

    # perform classification
    embedding, tokenizer = load_default_embedding_and_tokenizer()
    probs, labels = perform_classifications(
        texts_list=texts_list, embedding=embedding, tokenizer=tokenizer
    )

    print(f"""
        Label metrics:
            - Number of posts labeled: {len(texts_list)}
            - Number of positive labels: {sum(labels)}
            - Number of negative labels: {len(labels) - sum(labels)}
        """ # noqa
    )

    # export classifications
    for idx, post_dict in enumerate(post_dict_subset):
        post_dict[LABEL_COL] = labels[idx]
        post_dict[PROB_COL] = probs[idx][0]

    labeled_df = pd.DataFrame(post_dict_subset)

    output_directory = os.path.join(ML_ROOT_PATH, load_timestamp, '')
    create_or_use_default_directory(output_directory)

    output_filepath = os.path.join(output_directory, LABELED_DATA_FILENAME)
    labeled_df.to_csv(output_filepath)

    print(f"Done classifying data synced on timestamp {load_timestamp}")
