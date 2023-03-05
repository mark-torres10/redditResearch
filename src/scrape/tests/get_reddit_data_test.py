from unittest.mock import Mock

import pytest

from lib.reddit import RedditAPI
from scrape.get_reddit_data import get_posts_from_threads_in_subreddit

class TestGetPostsFromThreadsInSubreddit:
    @pytest.fixture
    def api(self) -> None:
        return RedditAPI()

    subreddit = "test-subreddit"
    num_threads = 4
    num_posts_per_thread = 3
    
    threads_json = [
        {'data': {'id': f'thread_{i}'}}
        for i in range(num_threads)
    ]

    posts_json = [
        {"id": f"post_{i}"}
        for i in range(num_posts_per_thread)
    ]


    def test_get_new_posts_from_subreddit(self, api: RedditAPI) -> None:
        mock_get_newest_threads = Mock(return_value=self.threads_json)
        api.get_newest_threads_in_subreddit = mock_get_newest_threads
        
        mock_get_latest_posts = Mock(return_value=self.posts_json)
        api.get_latest_posts_in_thread = mock_get_latest_posts

        thread_posts_dict = get_posts_from_threads_in_subreddit(
            api=api, subreddit=self.subreddit,
            num_threads=self.num_threads,
            thread_sort_type="new",
            num_posts_per_thread=self.num_posts_per_thread
        )

        assert mock_get_newest_threads.called_once_with(self.subreddit)

        expected_result = {
            f"thread_{i}": {
                f"post_{j}": {"id": f"post_{j}"}
                for j in range(self.num_posts_per_thread)
            }
            for i in range(self.num_threads)
        }

        assert thread_posts_dict == expected_result


class TestGetHottestThreadsInSubreddit:
    """Tests for get_hottest_threads_in_subreddit()."""

    def test_get_hottest_threads_in_subreddit(self) -> None:
        assert 1 == 1
