"""Gets raw data from Reddit and writes to Postgres DB."""
from lib.helper import track_function_runtime
from services.sync_single_subreddit.handler import main as sync_single_subreddit # noqa
from services.sync_subreddits.handler import main as sync_subreddits


@track_function_runtime
def main() -> None:
    event = {'subreddits': 'PoliticalHumor', 'thread_sort_type': 'controversial', 'max_num_threads': 20, 'max_total_comments': 400}
    context = {}
    payloads = sync_subreddits(event, context)
    for payload in payloads:
        sync_single_subreddit(payload, context)
    print("Completed sync of Reddit data.")


if __name__ == "__main__":
    main()
