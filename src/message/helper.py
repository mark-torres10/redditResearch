import random
from typing import List, Optional

import pandas as pd

from ml.constants import LABEL_COL

AUTHOR_DM_SCRIPT = """
    Hi {name},

    My research group is interested in how people express themselves on social
    media. Would you like to answer a few questions to help us with our
    research? Your response will remain anonymous. 

    You posted the following message on {date}:

    {post}

    Take a moment to think about what was happening at the time you posted.
    Think about who you were interacting with online, and what you were reading
    about on Reddit. Please answer the following regarding how you felt at the
    moment you posted:

    1. How outraged did you feel on a 1-7 scale? (1 = not at all, 4 = somewhat, 7 = very)
    2. How happy did you feel on a 1-7 scale? (1 = not at all, 4 = somewhat, 7 = very)
    3. How outraged do you think your message will appear to others (1 = not at all, 4 = somewhat, 7 = very)
    4. RIGHT NOW how outraged are you about the topic you posted about (1 = not at all, 4 = somewhat, 7 = very)

    You can simply respond with one answer per line such as:
    5
    1
    3
    4
""" # noqa

OBSERVER_DM_SCRIPT = """
    Hi {name},

    I'm a researcher at Yale University, and my research group is interested in
    how people express themselves on social media. Would you like to answer a
    few questions to help us with our research? Your response will remain
    anonymous. 

    The following message was posted in the {subreddit_name} subreddit on {date}:
    
    {post}

    Please answer the following:

    1. How outraged did you think the message author was on a 1-7 scale?
    (1 = not at all, 4 = somewhat, 7 = very)

    2. How happy did you think the message author was on a 1-7?
    (1 = not at all, 4 = somewhat, 7 = very)

    You can simply respond with one answer per line such as:
    5
    1
"""

TO_MESSAGE_COL = "to_message_flag"

HAS_BEEN_MESSAGED_COL = "has_been_messaged"

def balance_posts(labels: List[int], min_count: int) -> List[int]:
    # determine whether the 0s or the 1s is smaller. Assign all those as
    # to message
    min_label = 1 if sum(labels) == min_count else 0
    
    to_message_lst = [0] * len(labels)

    max_label_idx_lst = []

    # all the rows with the min_label should be messaged.
    for idx, label in enumerate(labels):
        if label == min_label:
            to_message_lst[idx] = 1
        else:
            max_label_idx_lst.append(idx)
    
    # shuffle the max_label_idx_lst, take the first [:min_count] labels
    random.shuffle(max_label_idx_lst)
    max_labels_idxs_to_message = max_label_idx_lst[min_count:]
    for idx in max_labels_idxs_to_message:
        to_message_lst[idx] = 1
    
    return to_message_lst


def determine_which_posts_to_message(
    labeled_data: pd.DataFrame,
    balance_strategy: Optional[str] = "equal"
) -> pd.DataFrame:
    """Given a df with labeled data."""
    label_col = labeled_data[LABEL_COL]
    if balance_strategy == "equal":
        min_count = label_col.value_counts().min()
    to_message_list = balance_posts(label_col, min_count)
    labeled_data[TO_MESSAGE_COL] = to_message_list

    return labeled_data
