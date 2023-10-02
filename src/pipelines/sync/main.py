"""Gets raw data from Reddit and writes to Postgres DB."""
from services.sync_single_subreddit.handler import main as sync_single_subreddit # noqa
from services.sync_subreddits.handler import main as sync_subreddits

if __name__ == "__main__":
    event = {"subreddits": "all"}
    context = {}
    payloads = sync_subreddits(event, context)
    for payload in payloads:
        sync_single_subreddit(payload, context)
    print("Completed sync of Reddit data.")
