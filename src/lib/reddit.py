"""Helper class for interacting with Reddit API."""
from dotenv import load_dotenv
import os
from pathlib import Path
import requests
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4


load_dotenv(Path("../../.env"))

CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_SECRET")
REDIRECT_URI = os.getenv("REDDIT_REDIRECT_URI")
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


class RedditAPI:
    """Wrapper class on top of Reddit API."""

    def __init__(self) -> None:
        self.root_url = "https://www.reddit.com/"
        # Reddit API recommends having a state hash to prevent XSRF attacks.
        self.state_uuid = str(uuid4())
        self.auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)

        # TODO: only generate auth token if doing requests that need auth.
        # lots of requests don't need auth requests. Lazy loading the access
        # token should help circumvent rate limits.
        # self.access_token = self._generate_access_token()
        self.access_token = None

    # TODO: cache token to avoid having to get new tokens
    # though, limit is 300 requests per 600 seconds: https://www.reddit.com/r/redditdev/comments/nhvdxk/are_there_any_rate_limits_for_apiv1access_token/
    def _generate_access_token(self) -> Optional[str]:
        """Generate access token needed to access Reddit API.

        Checks if there is an existing and valid access token in the environment
        and if so, uses that. Otherwise, generates new token.

        See https://github.com/reddit-archive/reddit/wiki/OAuth2#application-only-oauth
        """

        headers = {"user-agent": "foobar"}
        post_data = {
            "grant_type": "password",
            "username": USERNAME,
            "password": PASSWORD,
        }
        response = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=self.auth,
            data=post_data,
            headers=headers,
        )
        token_data = response.json()
        if "error" in token_data.keys():
            print(f"Unable to generate token: {token_data}")
            return None
        else:
            return token_data["access_token"]

    def generate_request_header(self, add_access_token: bool = False) -> Dict[str, str]:
        """Generate header needed in request."""
        # need to use our own user-agent so as to avoid using the default
        # python one, see https://www.reddit.com/r/redditdev/comments/3qbll8/429_too_many_requests/
        base_header = {"User-agent": DEFAULT_USER_AGENT}
        if add_access_token:
            header = {**base_header, **{"Authorization": f"bearer {self.access_token}"}}
        else:
            header = base_header
        return header

    def make_request(self, request_type: str, kwargs: Dict[str, Any]) -> Dict:
        """Make a request to the Reddit API."""
        if request_type == "GET":
            response = requests.get(**kwargs)
        elif request_type == "POST":
            response = requests.post(**kwargs)

        if response.status_code in ERROR_STATUS_CODES:
            raise ValueError(
                f"Request unsuccessful: {response.status_code}\t{response.reason}"
            )

        return response.json()

    def get(self, url: str, **kwargs: Dict) -> Dict:
        """Make GET request."""
        func_kwargs = {"url": url, **kwargs}  # type: ignore
        return self.make_request("GET", kwargs=func_kwargs)

    def post(self, url: str, **kwargs: Dict) -> Dict:
        """Make POST request."""
        func_kwargs = {"url": url, **kwargs}  # type: ignore
        return self.make_request("POST", kwargs=func_kwargs)

    @lazy_load_access_token
    def get_own_profile_info(self) -> Dict:
        """Get info about the current user logged in."""
        url = "https://oauth.reddit.com/api/v1/me"
        headers = self.generate_request_header(add_access_token=True)
        return self.get(url=url, headers=headers)

    def search_subreddits(self, query_string: str) -> Dict:
        """Search subreddits that begin with a certain string."""
        url = "https://www.reddit.com/api/search_reddit_names.json"
        params = {"query": query_string}
        headers = self.generate_request_header()
        return self.get(url=url, params=params, headers=headers)

    def get_hottest_threads_in_subreddit(self, subreddit: str) -> Dict:
        url = f"https://www.reddit.com/r/{subreddit}/hot/.json"
        headers = self.generate_request_header()
        return self.get(url=url, headers=headers)

    def get_newest_threads_in_subreddit(self, subreddit: str) -> Dict:
        url = f"https://www.reddit.com/r/{subreddit}/new/.json"
        headers = self.generate_request_header()
        return self.get(url=url, headers=headers)

    def get_controversial_threads_in_subreddit(self, subreddit: str) -> Dict:
        url = f"https://www.reddit.com/r/{subreddit}/controversial/.json"
        headers = self.generate_request_header()
        return self.get(url=url, headers=headers)

    def get_latest_posts_in_thread(self, subreddit: str, thread_id: str) -> Dict:
        url = f"https://www.reddit.com/r/{subreddit}/comments/{thread_id}/.json"  # noqa
        headers = self.generate_request_header()
        return self.get(url=url, headers=headers)


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
