"""Object-speciific enrichments for Reddit raw data."""
from praw.models.reddit.comment import Comment
from praw.models.reddit.message import Message

from praw.models.reddit.submission import Submission


def remove_prefix_from_id(id: str) -> str:
    """Some ids from Reddit are prepended with a prefix indicating the object
    type from the API. It looks something like t5_{id}.
    
    We can remove this three-character prefix.
    """  
    return id[3:]


def object_specific_enrichments(obj) -> dict:
    enrichments = {}
    if isinstance(obj, Comment):
        enrichments["author_screen_name"] = obj.author.name
        # a comment is a top level comment if its parent is the thread itself. 
        # otherwise, the parent will be a t1_ comment, meaning that it is a
        # reply to a comment.
        # also, the parent thread is stored in link_id.
        enrichments["top_level_comment"] ="t3_" in obj.parent_id
        enrichments["parent_thread_id"] = remove_prefix_from_id(obj.link_id)
        enrichments["parent_comment_id"] = (
            None if enrichments["top_level_comment"]
            else remove_prefix_from_id(obj.parent_id)
        )
        # example author_fullname value: t2_519wf. These are Redditor IDs.
        enrichments["author_id"] = remove_prefix_from_id(obj.author_fullname)
    elif isinstance(obj, Submission):
        enrichments["author_screen_name"] = obj.author.name
        enrichments["edited"] = float(obj.edited) # sometimes it's a float and sometimes a bool? Unclear why, but casting all to float.
    elif isinstance(obj, Message):
        enrichments["author_id"] = remove_prefix_from_id(obj.author_fullname)

    return enrichments


def field_specific_parsing(obj) -> dict:
    res = {}
    if "subreddit_id" in obj:
        res["subreddit_id"] = remove_prefix_from_id(obj["subreddit_id"])
    if "parent_id" in obj:
        res["parent_id"] = remove_prefix_from_id(obj["parent_id"])
    return res
