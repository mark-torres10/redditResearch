import os

from sync.transformations import TRANSFORMATION_FIELDS_LIST
from lib.helper import CODE_DIR

SYNC_ROOT_PATH = os.path.join(CODE_DIR, "sync")

SYNC_METADATA_FILENAME = "metadata.csv"
SYNC_RESULTS_FILENAME = "results.jsonl"

# column that has posts in the thread
POSTS_COLNAME = "posts"
# column that has the post that we need to label
POST_TEXT_COLNAME = "body"

# some field names from the Reddit API are ambiguous. Remapping these for
# our purposes. Keys=Reddit API field names, Values=remapped values.
API_FIELDS_TO_REMAPPED_FIELDS = {
    "author": "author_screen_name",
    "author_fullname": "author_id"
}

# columns required to identify the post (e.g., id, author, thread, link). This
# matches the names after the transformations and name remappings in
# `API_FIELDS_TO_REMAPPED_FIELDS` are done.
COLS_TO_IDENTIFY_POST = [
    "id", "author_screen_name", "author_id", "body", "permalink",
    "created_utc", "subreddit_name_prefixed"
]

ALL_COLS = COLS_TO_IDENTIFY_POST + TRANSFORMATION_FIELDS_LIST