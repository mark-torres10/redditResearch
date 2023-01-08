"""Base file for getting Reddit data."""
from typing import Dict, List

from lib.reddit import RedditAPI, unpack_t3_res

def get_hottest_threads_in_subreddit(
    reddit_client: RedditAPI, subreddit: str, num_threads: int = 5
):
    res_data = reddit_client.get_hottest_threads_in_subreddit(
        subreddit=subreddit
    )["data"]

    threads_dicts_list = [
        unpack_t3_res(child) for child in res_data["children"]
    ]

    return threads_dicts_list[:num_threads]

def get_latest_threads_in_subreddit(
    reddit_client: RedditAPI, subreddit: str, num_threads: int = 5
):
    res_data = reddit_client.get_newest_threads_in_subreddit(
        subreddit=subreddit
    )["data"]

    threads_dicts_list = [
        unpack_t3_res(child) for child in res_data["children"]
    ]

    return threads_dicts_list[:num_threads]


def get_latest_posts_in_thread(
    reddit_client: RedditAPI, subreddit: str, thread_id: str,
    num_posts: int = 5
):
    res_data = reddit_client.get_latest_posts_in_thread(
        subreddit=subreddit, thread_id=thread_id
    )[1]['data']
    
    posts_dicts_list = [
        unpack_t3_res(child) for child in res_data["children"]
    ]
    
    return posts_dicts_list[:num_posts]

def get_text_of_posts_list(posts_list: List[Dict]) -> List[str]:
    return [res["body"] for res in posts_list]


if __name__ == '__main__':
    reddit_client = RedditAPI()
    subreddit = "politics"
    num_threads = 2
    num_posts = 5
    
    hottest_threads = get_hottest_threads_in_subreddit(
        reddit_client=reddit_client, subreddit=subreddit,
        num_threads=num_threads
    )
    
    hottest_thread = hottest_threads[0]
    hottest_thread_id = hottest_thread["id"]
    
    latest_posts_in_thread = get_latest_posts_in_thread(
        reddit_client=reddit_client, subreddit=subreddit, 
        thread_id=hottest_thread_id, num_posts=num_posts
    )
    breakpoint()