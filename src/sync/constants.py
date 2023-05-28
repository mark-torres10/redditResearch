import os

from ml.transformations import TRANSFORMATION_FIELDS_LIST
from lib.helper import CODE_DIR

SYNC_ROOT_PATH = os.path.join(CODE_DIR, "sync")

SYNC_METADATA_FILENAME = "metadata.csv"
SYNC_RESULTS_FILENAME = "results.jsonl"

# column that has posts in the thread
POSTS_COLNAME = "posts"
# column that has the post that we need to label
POST_TEXT_COLNAME = "body"

# columns required to identify the post (e.g., id, author, thread, link)
COLS_TO_IDENTIFY_POST = [
    "id", "author", "author_fullname", "body", "permalink", "created_utc",
    "subreddit_name_prefixed"
]

ALL_COLS = COLS_TO_IDENTIFY_POST + TRANSFORMATION_FIELDS_LIST

# some field names from the Reddit API are ambiguous. Remapping these for
# our purposes. Keys=Reddit API field names, Values=remapped values.
API_FIELDS_TO_REMAPPED_FIELDS = {
    "author": "author_screen_name",
    "author_fullname": "author_id"
}