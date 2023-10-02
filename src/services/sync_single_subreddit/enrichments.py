"""Object-speciific enrichments for Reddit raw data."""
from praw.models.reddit.comment import Comment
from praw.models.reddit.submission import Submission


def object_specific_enrichments(obj) -> dict:
    enrichments = {}
    if isinstance(obj, Comment):
        enrichments["author_screen_name"] = obj.author.name
    elif isinstance(obj, Submission):
        enrichments["author_screen_name"] = obj.author.name
    return enrichments
