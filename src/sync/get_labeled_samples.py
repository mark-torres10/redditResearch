from typing import Dict, List

import pandas as pd

from lib.reddit import RedditAPI
from ml.inference import classify_text, load_default_embedding_and_tokenizer
from sync.get_reddit_data import (
    get_hottest_threads_in_subreddit,
    get_latest_posts_in_thread,
)


if __name__ == "__main__":
    """
    Generate a dictionary of labeled samples in the form of:

    {
        threadId: {
            postId: {
                threadBody: "", # whatever the thread says. Can use same key name as what's in the API?
                postBody: "", # whatever the post says. Can use the same key name as what's in the API?
                prob: 0.7,
                label: 1
            }
        }
    }

    Then, convert to a .csv in the form:

    id | threadId | postId | threadBody | postBody | prob | label

    """
    reddit_client = RedditAPI()
    subreddit = "politics"
    num_threads = 5
    num_posts = 40

    hottest_threads = get_hottest_threads_in_subreddit(
        reddit_client=reddit_client, subreddit=subreddit, num_threads=num_threads
    )

    embedding, tokenizer = load_default_embedding_and_tokenizer()

    map_threads_to_posts: Dict[str, Dict] = {}

    for thread in hottest_threads:
        map_threads_to_posts[thread["id"]] = {}  # noqa
        for post in get_latest_posts_in_thread(
            reddit_client=reddit_client,
            subreddit=subreddit,
            thread_id=thread["id"],
            num_posts=num_posts,
        ):
            post_text = post.get("body", "")
            prob, label = classify_text(
                text=post_text, embedding=embedding, tokenizer=tokenizer
            )
            map_threads_to_posts[thread["id"]][post["id"]] = {
                "threadBody": thread["selftext"],
                "postBody": post_text,
                "prob": prob,
                "label": label,
            }

    rows = []

    for thread_id in map_threads_to_posts:
        for post_id in map_threads_to_posts[thread_id]:
            rows.append(
                {
                    "threadId": thread_id,
                    "postId": post_id,
                    **map_threads_to_posts[thread_id][post_id],
                }
            )

    df = pd.DataFrame(rows)

    df.to_csv("labeled_samples.csv")
