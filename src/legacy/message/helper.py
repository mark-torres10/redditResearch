import random
from typing import List, Optional

import pandas as pd

from message.constants import TO_MESSAGE_COL
from ml.constants import LABEL_COL

AUTHOR_DM_SUBJECT_LINE = "Yale Researchers Looking to Learn More About Your Beliefs"

AUTHOR_DM_SCRIPT = """
    Hi {name},

    My research group is interested in how people express themselves on social
    media. Would you like to answer a few questions to help us with our
    research? Your response will remain anonymous. 

    You posted the following message on {date} in the {subreddit} subreddit:

    {post}

    (link {permalink})

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

AUTHOR_PHASE_MESSAGE_IDENTIFIER_STRING = (
    "Take a moment to think about what was happening at the time you posted."
)


def balance_posts(labels: pd.Series, min_count: int) -> List[int]:
    """Balance which of the rows in the `labels` series to label.
    
    This will return a binary list of 0s and 1s where:
        0 = do not message
        1 = message
    Such that if this list is zipped against the `labels` series, then the
    number of rows in the "labels" series with a "labels" value of 0
    that have a "to_label" value of 1 equal the number of rows in the "labels"
    series with a "labels" value of 1 that have a "to_label" value of 1.

    This means that we should message an equal number of the rows that have
    labels = 0 as we do rows that have labels = 1.
    """
    # determine whether the 0s or the 1s is smaller. Assign all those as
    # to message
    labels_list = labels.tolist()
    min_label = 1 if sum(labels_list) == min_count else 0
    
    to_message_lst = [0] * len(labels_list)

    max_label_idx_lst = []

    # all the rows with the min_label should be messaged.
    for idx, label in enumerate(labels_list):
        if label == min_label:
            to_message_lst[idx] = 1
        else:
            max_label_idx_lst.append(idx)

    # shuffle the max_label_idx_lst, take the first [:min_count] labels
    random.shuffle(max_label_idx_lst)
    max_labels_idxs_to_message = max_label_idx_lst[:min_count]
    for idx in max_labels_idxs_to_message:
        to_message_lst[idx] = 1
    
    return to_message_lst


def determine_which_posts_to_message(
    labeled_data: pd.DataFrame,
    balance_strategy: Optional[str] = "equal"
) -> pd.DataFrame:
    """Given a df with labeled data, determine which comments/posts should be
    messaged.
    
    We do this by using a balance strategy (by default, "equal"). In the
    "equal" strategy, we message an equal number of data labeled 0s and 1s.
    This means that the number of 0s and 1s will be set as
    min(num_zeros, num_ones), the minimum count of the two labels.
    """
    label_col = labeled_data[LABEL_COL]
    if balance_strategy == "equal":
        min_count = label_col.value_counts().min()
    to_message_list = balance_posts(label_col, min_count)
    labeled_data[TO_MESSAGE_COL] = to_message_list

    print(
        f"""
            Number of posts: {len(to_message_list)}\n
            Number to DM: {sum(to_message_list)}
        """
    )

    return labeled_data
