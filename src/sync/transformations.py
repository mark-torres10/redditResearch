"""Transformations to make to a given column's raw data."""
from datetime import datetime

from lib import helper


MAP_COL_TO_TRANSFORMATION = {
    "created_utc_string": {
        "original_col": "created_utc",
        "transform_func": helper.convert_utc_timestamp_to_datetime_string
    },
    "author_screen_name": {
        "original_col": "author",
        "transform_func": helper.get_author_name_from_author_id
    }
}

TRANSFORMATION_FIELDS_LIST = ["created_utc_string", "author_screen_name"]
