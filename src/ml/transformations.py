"""Transformations to make to a given column's raw data."""
from datetime import datetime

def convert_utc_timestamp_to_datetime_string(utc_time: float) -> str:
    """Given a UTC timestamp, convert to a human-readable date string.
    
    >>> convert_utc_timestamp_to_datetime_string(1679147878.0)
    Sunday, March 19, 2023, at 8:11:18 PM 
    """
    utc_datetime = datetime.fromtimestamp(utc_time)
    return utc_datetime.strftime("%A, %B %d, %Y, at %I:%M:%S %p")

MAP_COL_TO_TRANSFORMATION = {
    "created_utc_string": {
        "original_col": "created_utc",
        "transform_func": convert_utc_timestamp_to_datetime_string
    }
}

TRANSFORMATION_FIELDS_LIST = ["created_utc_string"]
