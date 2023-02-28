"""Base file for getting Reddit data."""
from typing import Dict, List, Literal

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


def get_posts_from_threads_in_subreddit(
    api: RedditAPI,
    subreddit: str,
    num_threads: str,
    thread_sort_type: Literal["new", "hot", "controversial"],
    num_posts_per_thread: int
) -> Dict[str, List[Dict]]:
    """Given a subreddit, get the top X threads in that subreddit, as well
    as Y number of posts in each thread.

    Args:
        subreddit (str): The subreddit to get posts from.
        num_threads (str): The number of threads to get.
        thread_sort_type (Literal["new", "hot", "controversial"]): The sort
            type of the threads to get.
        num_posts_per_thread (int): The number of posts to get per thread.

    Returns:
        Dict[List[Dict]]: A dictionary of dictionaries, where the keys
        are the thread IDs, and the values are lists of dictionaries,
        where the keys are the post IDs, and the values are the post
    """
    # Get the list of threads in the subreddit.
    if thread_sort_type == 'new':
        threads = api.get_newest_threads_in_subreddit(subreddit)
    elif thread_sort_type == 'hot':
        threads = api.get_hottest_threads_in_subreddit(subreddit)
    elif thread_sort_type == 'controversial':
        threads = api.get_controversial_threads_in_subreddit(subreddit)
    else:
        raise ValueError(f"Invalid thread_sort_type: {thread_sort_type}")

    # Get the specified number of threads, and get the specified number of posts
    # from each thread.
    thread_posts_dict = {}
    for thread in threads[:num_threads]:
        thread_id = thread['data']['id']
        thread_posts = api.get_latest_posts_in_thread(subreddit, thread_id)[:num_posts_per_thread]
        post_dict = {post['id']: post for post in thread_posts}
        thread_posts_dict[thread_id] = post_dict

    return thread_posts_dict


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
