import sys
import os

import pandas as pd

from lib.logging import RedditLogger
from ml.constants import (
    LABEL_COL, LABELED_DATA_FILENAME, ML_ROOT_PATH, PROB_COL
)
from ml.inference import (
    classify_text, load_default_embedding_and_tokenizer
)
from sync.constants import (
    POST_COLNAME, SYNC_RESULTS_FILENAME, SYNC_ROOT_PATH
)

logger = RedditLogger(name=__name__)

def main():
    pass


if __name__ == "__main__":
    # load previous files
    # transform .jsonl to df
    # given df, get the text field.
    # classify the text field, add as column to df
    # save relevant IDs, text, classification (label, probability) to new file
    load_timestamp = sys.argv[1]
    timestamp_dir = os.path.join(SYNC_ROOT_PATH, load_timestamp)

    if not os.path.exists(timestamp_dir):
        logger.error(f"Sync data timestamp directory {load_timestamp} does not exist")
        sys.exit(1)

    sync_data_filepath = os.path.join(timestamp_dir, SYNC_RESULTS_FILENAME)
    sync_data = pd.read_json(sync_data_filepath)
    text_col = sync_data[POST_COLNAME].tolist()

    probs = []
    labels = []

    embedding, tokenizer = load_default_embedding_and_tokenizer()

    for idx, text in enumerate(text_col):
        try:
            prob, label = classify_text(text, embedding, tokenizer)
            probs.append(prob)
            labels.append(label)
        except Exception as e:
            logger.info("""
                Unable to classify text at position {idx}. Text details:

                Text preview: {text_preview}
                Text length: {text_length}
                """.format(
                    idx=idx, text_preview=text[:50], text_length=len(text)
                )
            )
            probs.append(f"Unable to classify. Error {e}")
            labels.append(0) # NOTE: should this be defaulted to None?
            continue

    sync_data[PROB_COL] = probs
    sync_data[LABEL_COL] = labels

    output_filepath = os.path.join(
        ML_ROOT_PATH, load_timestamp, LABELED_DATA_FILENAME
    )
    sync_data.to_csv(output_filepath)

    logger.info(f"Done classifying data synced on timestamp {load_timestamp}")
