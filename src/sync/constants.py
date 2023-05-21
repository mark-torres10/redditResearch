import os

from lib.helper import ROOT_DIR

SYNC_ROOT_PATH = os.path.join(ROOT_DIR, "sync")

SYNC_METADATA_FILENAME = "metadata.csv"
SYNC_RESULTS_FILENAME = "results.jsonl"

# column that has the post that we need to label
POST_COLNAME = "body"
