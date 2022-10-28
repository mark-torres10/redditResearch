"""Helper class for interacting with Reddit API."""
import base64
from dotenv import load_dotenv
import os
from pathlib import Path
import requests
from typing import Dict, Optional
from uuid import uuid4


load_dotenv(Path("../../.env"))

CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_SECRET")
REDIRECT_URI = os.getenv("REDDIT_REDIRECT_URI")
# TODO: get lab username + PW at some point
USERNAME = os.getenv("REDDIT_USERNAME")
PASSWORD = os.getenv("REDDIT_PASSWORD")

ERROR_STATUS_CODES = [400, 429]


def lazy_load_access_token(func):
    def inner(api_instance):
        if api_instance.access_token is None:
            api_instance.access_token = api_instance._generate_access_token()
        else:
            return func(api_instance)
    return inner

class RedditAPI:
    """Wrapper class on top of Reddit API."""
    def __init__(self):
        self.root_url = "https://www.reddit.com/"
        # Reddit API recommends having a state hash to prevent XSRF attacks.
        self.state_uuid = str(uuid4())
        self.auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
        
        # TODO: only generate auth token if doing requests that need auth.
        # lots of requests don't need auth requests. Lazy loading the access
        # token should help circumvent rate limits.
        #self.access_token = self._generate_access_token()
        self.access_token = None
    
    # TODO: cache token to avoid having to get new tokens
    # though, limit is 300 requests per 600 seconds: https://www.reddit.com/r/redditdev/comments/nhvdxk/are_there_any_rate_limits_for_apiv1access_token/
    def _generate_access_token(self) -> Optional[str]:
        """Generate access token needed to access Reddit API.
        
        Checks if there is an existing and valid access token in the environment
        and if so, uses that. Otherwise, generates new token.
        
        See https://github.com/reddit-archive/reddit/wiki/OAuth2#application-only-oauth
        """
        
        headers = { 'user-agent': "foobar"}
        post_data = {
            "grant_type": "password",
            "username": USERNAME,
            "password": PASSWORD
        }
        response = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=self.auth,
            data=post_data,
            headers=headers
        )
        token_data = response.json()
        if "error" in token_data.keys():
            print(f"Unable to generate token: {token_data}")
            return None
        else:
            return token_data["access_token"]

    def generate_request_header(self) -> Dict[str, str]:
        """Generate header needed in request."""
        return {
            "Authorization": f"bearer {self.access_token}"
        }

    def make_request(self, request_type: str, url: str, headers: Dict):
        """Make a request to the Reddit API."""
        response = requests.get(
            url, headers=headers
        )

        if response.status_code in ERROR_STATUS_CODES:
            raise ValueError(f"Request unsuccessful: {response.status_code}\t{response.reason}")
        
        return response.json()
    
    
    def get(self, url: str, headers: Dict):
        """Make GET request."""
        return self.make_request("GET", url, headers)
    
    def post(self, url, headers=None):
        """Make POST request."""
        return self.make_request("POST", url, headers)

    @lazy_load_access_token
    def get_own_profile_info(self) -> Dict:
        """Get info about the current user logged in."""
        breakpoint()
        url = "https://oauth.reddit.com/api/v1/me"
        headers = self.generate_request_header()
        return self.get(url, headers)

    
    def search_subreddits(self, query_string):
        """Search subreddits that begin with a certain string."""
        


    
    def get_subreddit_threads(self):
        """Get the current threads in a subreddit."""
        pass
    

if __name__ == '__main__':
    reddit = RedditAPI()
    token = reddit._generate_access_token()
    profile = reddit.get_own_profile_info()
    breakpoint()