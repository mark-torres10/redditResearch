"""Given a subreddit, return a list of valid possible observers."""
from services.get_valid_possible_observers.helper import get_valid_possible_observers # noqa


def main() -> None:
    get_valid_possible_observers()
