"""Transformations to make to a given column's raw data."""
from datetime import datetime

from lib.helper import convert_utc_timestamp_to_datetime_string


MAP_COL_TO_TRANSFORMATION = {
    "created_utc_string": {
        "original_col": "created_utc",
        "transform_func": convert_utc_timestamp_to_datetime_string
    }
}

TRANSFORMATION_FIELDS_LIST = ["created_utc_string"]
