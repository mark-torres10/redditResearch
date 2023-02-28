"""Helper class for interacting with Reddit API."""
from dotenv import load_dotenv
import os
from pathlib import Path
import requests
from typing import Any, Callable, Dict, Optional
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

    def inner(api_instance) -> Callable: # type: ignore
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

    def get_latest_posts_in_thread(self, subreddit: str, thread_id: str) -> Dict:
        url = f"https://www.reddit.com/r/{subreddit}/comments/{thread_id}/.json"  # noqa
        headers = self.generate_request_header()
        return self.get(url=url, headers=headers)


def unpack_t3_res(t3_dict: Dict) -> Dict:
    """Unpacks result of type `t3` and returns just the resulting data."""
    return t3_dict["data"]


if __name__ == "__main__":
    reddit = RedditAPI()
    politics_subreddits = reddit.search_subreddits("politics")
    # token = reddit._generate_access_token()
    # profile = reddit.get_own_profile_info()
    breakpoint()
