import pandas as pd
import pytest

from message.helper import balance_posts


def test_balance_posts():
    # should label an equal # of 0s and 1s, each equal to min_count
    labels_more_zeros = pd.Series([0, 1, 1, 0, 0, 1, 1, 0, 0, 0])
    min_count = 4
    to_message_list = balance_posts(labels_more_zeros, min_count)
    
    assert len(to_message_list) == len(labels_more_zeros)
    assert sum(to_message_list) == 2 * min_count

    labels_more_ones = pd.Series([1, 1, 1, 0, 1, 1, 1, 0, 0, 0])
    min_count = 4
    to_message_list = balance_posts(labels_more_ones, min_count)
    
    assert len(to_message_list) == len(labels_more_ones)
    assert sum(to_message_list) == 2 * min_count

    label_all_zeros = pd.Series([0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    min_count = 0
    to_message_list = balance_posts(label_all_zeros, min_count)
    assert len(to_message_list) == len(label_all_zeros)
    assert sum(to_message_list) == 0

    label_all_ones = pd.Series([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    min_count = 0
    to_message_list = balance_posts(label_all_ones, min_count)
    assert len(to_message_list) == len(label_all_ones)
    assert sum(to_message_list) == 0

    
if __name__ == "__main__":
    pytest.main()
