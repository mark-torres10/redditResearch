"""
Classifies a previously synced set of posts. Dumps the labels in a new
directory.

Example usage:
    python classify_posts.py 2023-03-20_1438
"""
import sys
import time
from typing import Dict, List
import os

import pandas as pd

from lib.helper import read_jsonl_as_list_dicts
from lib.redditLogging import RedditLogger
from ml.constants import (
    LABEL_COL, LABELED_DATA_FILENAME, ML_ROOT_PATH, PROB_COL
)
from ml.inference import (
    classify_text, load_default_embedding_and_tokenizer
)
from ml.transformations import MAP_COL_TO_TRANSFORMATION
from sync.constants import (
    API_FIELDS_TO_REMAPPED_FIELDS, COLS_TO_IDENTIFY_POST, POST_TEXT_COLNAME,
    POSTS_COLNAME, SYNC_RESULTS_FILENAME, SYNC_ROOT_PATH
)

logger = RedditLogger(name=__name__)

if __name__ == "__main__":
    # load previous files
    # transform .jsonl to df
    # given df, get the text field.
    # classify the text field, add as column to df
    # save relevant IDs, text, classification (label, probability) to new file
    load_timestamp = sys.argv[1]
    timestamp_dir = os.path.join(SYNC_ROOT_PATH, load_timestamp, '')

    if not os.path.exists(timestamp_dir):
        logger.error(f"Sync data timestamp directory {timestamp_dir} does not exist")
        sys.exit(1)

    sync_data_filepath = os.path.join(timestamp_dir, SYNC_RESULTS_FILENAME)
    sync_data_jsons = read_jsonl_as_list_dicts(sync_data_filepath)
    sync_data = pd.DataFrame(sync_data_jsons)

    # each row corresponds to a thread. The `posts` field corresponds to the
    # posts in the thread. Let's get these posts
    posts_dict_list: List[List[Dict]]
    posts_dict_list = sync_data[POSTS_COLNAME]

    # for each post, we only want the columns needed to identify them
    post_dict_subset: List[Dict] = []
    texts_list: List[str] = []

    for post_list in posts_dict_list:
        for post in post_list:
            output_dict = {}
            # get necessary fields from raw input
            for col in COLS_TO_IDENTIFY_POST:
                col_name_remapped = API_FIELDS_TO_REMAPPED_FIELDS.get(col, col)
                output_dict[col_name_remapped] = post[col]
            # add any enrichment + supplementary fields, as necessary
            for (enrichment_col, transformation_dict) in (
                MAP_COL_TO_TRANSFORMATION.items()
            ):
                col_input = transformation_dict["original_col"]
                transform_func = transformation_dict["transform_func"]
                output_dict[enrichment_col] = transform_func(post[col_input])
            post_dict_subset.append(output_dict)
            texts_list.append(post[POST_TEXT_COLNAME])

    probs: List[List[int]] = [] # nested list, e.g., [[0.2], [0.5]]
    labels: List[int] = [] # e.g., [0, 1]
    num_text_unable_to_classify = 0

    embedding, tokenizer = load_default_embedding_and_tokenizer()

    label_start_time = time.time()

    for idx, text in enumerate(texts_list):
        try:
            prob, label = classify_text(text, embedding, tokenizer)
            probs.append(prob)
            labels.append(label)
        except Exception as e:
            print("""
                Unable to classify text at position {idx}. Text details:

                Text preview: {text_preview}
                Text length: {text_length}
                """.format(
                    idx=idx, text_preview=text[:50], text_length=len(text)
                )
            )
            probs.append(f"Unable to classify. Error {e}")
            labels.append(0) # NOTE: should this be defaulted to None?
            num_text_unable_to_classify += 1
            continue

    label_end_time = time.time()

    execution_time_seconds = round(label_end_time - label_start_time)

    execution_time_minutes = execution_time_seconds // 60
    execution_time_leftover_seconds = (
        execution_time_seconds - (60 * execution_time_minutes)
    )

    print(
        "Tried to classify {count} posts. Succeeded in {num_success}, failed in {num_fail}".format( # noqa
            count=len(texts_list),
            num_success=len(texts_list)-num_text_unable_to_classify,
            num_fail=num_text_unable_to_classify
        )
    )

    print(f"""
        Label metrics:
            - Number of posts labeled: {len(texts_list)}
            - Number of positive labels: {sum(labels)}
            - Number of negative labels: {len(labels) - sum(labels)}
            - Execution time: {execution_time_minutes} minutes, {execution_time_leftover_seconds} seconds
        """ # noqa
    )

    for idx, post_dict in enumerate(post_dict_subset):
        post_dict[LABEL_COL] = labels[idx]
        post_dict[PROB_COL] = probs[idx][0]

    labeled_df = pd.DataFrame(post_dict_subset)

    output_directory = os.path.join(ML_ROOT_PATH, load_timestamp, '')
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    output_filepath = os.path.join(output_directory, LABELED_DATA_FILENAME)
    labeled_df.to_csv(output_filepath)

    print(f"Done classifying data synced on timestamp {load_timestamp}")
