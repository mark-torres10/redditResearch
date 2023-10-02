import os

from data.helper import DATA_DIR
from lib.helper import CURRENT_TIME_STR

SYNC_METADATA_FILENAME = "metadata.csv"
SYNC_METADATA_DIR = os.path.join(DATA_DIR, "sync_metadata")
NEW_SYNC_METADATA_FULL_FP = os.path.join(
    SYNC_METADATA_DIR, CURRENT_TIME_STR, SYNC_METADATA_FILENAME
)
