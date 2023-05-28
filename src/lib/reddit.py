"""Helper class for interacting with Reddit API."""
from dotenv import load_dotenv
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import praw


load_dotenv(Path("../../.env"))

CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_SECRET")
REDIRECT_URI = os.getenv("REDDIT_REDIRECT_URI")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
# TODO: get lab username + PW at some point
USERNAME = os.getenv("REDDIT_USERNAME")
PASSWORD = os.getenv("REDDIT_PASSWORD")
DEFAULT_USER_AGENT = "Reddit Research v1"

ERROR_STATUS_CODES = [400, 429]


def lazy_load_access_token(func: Callable) -> Callable:
    """Lazy loads an access token for functions that require v1 access
    (which is gated behind OAuth).
    """

    def inner(api_instance) -> Callable:  # type: ignore
        if api_instance.access_token is None:
            api_instance.access_token = api_instance._generate_access_token()
        return func(api_instance)

    return inner

def init_api_access() -> praw.Reddit:
    return praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        refresh_token=REFRESH_TOKEN,
        user_agent=DEFAULT_USER_AGENT,
    )

def authorize_api_access(api: praw.Reddit) -> None:
    """Provides auth for API access.
    
    Documentation: https://praw.readthedocs.io/en/stable/getting_started/authentication.html
    """
    # TODO: update auth url args for future use cases
    scopes = ["identity", "privatemessages"]
    state = "..."
    implicit=False
    duration="permanent"
    try:
        auth_url = api.auth.url(
            scopes=scopes, state=state, implicit=implicit, duration=duration
        )
        print(
            f"Please visit the following URL to authorize the application: {auth_url}"
        )
        unique_key = input("Please paste in the unique key:")
        breakpoint()
        refresh_token = api.auth.authorize(unique_key)
        print(f"Refresh token: {refresh_token}")
        print("Authorized successfully!")
    except Exception as e:
        print(f"Unable to authorize application: {e}")
        raise


class Listing:
    """Wrapper for the 'Listing' resposne object returned by the Reddit API.

    Expected schema:
    {
    "kind": "Listing",
    "data": {
        "modhash": string,
        "dist": integer,
        "children": [
        object,
        object,
        ...
        ],
        "after": string,
        "before": string
    }
    }
    """

    pass


class T1:
    """Wrapper for the 'T1' response object returned by the Reddit API.

    'T1' is the response object from the Reddit API that represents
    a comment. These are the individual comments that people leave on a post.
    """

    def __init__(self, comment_data: Dict[str, Any]) -> None:
        self.id: str = comment_data["id"]
        self.author: str = comment_data["author"]
        self.author_fullname: str = comment_data[
            "author_fullname"
        ]  # id of author, in form `t2_{author id}`
        self.body: str = comment_data["body"]  # body of comment (needs parsing)
        self.body_html: str = comment_data["body_html"]
        self.created_utc: float = comment_data["created_utc"]
        self.parent_id: str = comment_data[
            "parent_id"
        ]  # in the form `t3_{id of subreddit}`
        self.permalink: str = comment_data["permalink"]
        self.replies: Dict[str, Dict] = comment_data["replies"]
        self.children: List[Dict] = comment_data["replies"]["data"]
        self.score: int = comment_data["score"]
        self.subreddit: str = comment_data["subreddit"]
        self.subreddit_id: str = comment_data["subreddit_id"]
        self.subreddit_name_prefixed: str = comment_data["subreddit_name_prefixed"]
        self.upvote_count: int = comment_data["ups"]

    def to_dict(self) -> Dict:
        """Converts object and its attributes to a JSON dict."""
        return {attr: getattr(self, attr) for attr in self.__dict__}


class T3:
    """Wrapper for the 'T3' response object returned by the Reddit API.

    'T3' is the response object from the Reddit API that represents a post,
    such as a link, image, or text post.

    Here, we specify the fields that we want to return when given a T3 response
    object.
    """

    def __init__(self, thread_data: Dict[str, Any]) -> None:
        self.title: str = thread_data["title"]
        self.id: str = thread_data["id"]
        self.author: str = thread_data["author"]
        # marked as "author_fullname" in the API but it's actually their id
        self.author_fullname_id: str = thread_data["author_fullname"]
        self.subreddit: str = thread_data["subreddit"]
        self.subreddit_name_prefixed: str = thread_data["subreddit_name_prefixed"]
        self.subreddit_id: str = thread_data["subreddit_id"]
        self.subreddit_subscribers: int = thread_data["subreddit_subscribers"]
        self.permalink: str = thread_data["permalink"]
        self.upvote_count: int = thread_data["ups"]
        self.upvote_ratio: float = thread_data["upvote_ratio"]
        self.score: int = thread_data["score"]
        self.total_awards_received: int = thread_data["total_awards_received"]
        self.view_count: Optional[int] = thread_data["view_count"]
        self.num_comments: int = thread_data["num_comments"]
        # we need to filter out bot posts; is_robot_indexable=True if it is a bot.
        self.is_robot_indexable: bool = thread_data["is_robot_indexable"]
        self.url: str = thread_data["url"]
        self.thread_url: str = f"{self.url}.json"
        self.posts: List[Dict] = []

    def to_dict(self) -> Dict:
        """Converts object and its attributes to a JSON dict."""
        return {attr: getattr(self, attr) for attr in self.__dict__}

    def add_comments_to_thread(self, comments: List[T1]) -> None:
        """Adds the comments to the thread."""
        self.posts.extend([comment.to_dict() for comment in comments])


def unpack_t3_res(t3_dict: Dict) -> Dict:
    """Unpacks result of type `t3` and returns just the resulting data."""
    return t3_dict["data"]


MAP_RESPONSE_KIND_TO_CLASS = {"t1": T1, "t3": T3, "listing": Listing}
