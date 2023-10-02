"""Helper utilities for managing single subreddit sync."""
import copy
import csv
from typing import Any, Literal

import pandas as pd
from praw.models.comment_forest import CommentForest
from praw.models.listing.generator import ListingGenerator
from praw.models.reddit.comment import Comment
from praw.models.reddit.redditor import Redditor
from praw.models.reddit.submission import Submission
from praw.models.reddit.subreddit import Subreddit
from praw.reddit import Reddit

from lib.db.sql.helper import write_df_to_database
from lib.helper import (
    CURRENT_TIME_STR, DENYLIST_AUTHORS,
    add_enrichment_fields,
    is_json_serializable,
)
from services.sync_single_subreddit.constants import NEW_SYNC_METADATA_FULL_FP
from services.sync_single_subreddit.enrichments import (
    object_specific_enrichments
)

DEFAULT_NUM_THREADS = 5
DEFAULT_NUM_COMMENTS_PER_THREAD = 10
DEFAULT_THREAD_SORT_TYPE = "hot"


def write_metadata_file(metadata_dict: dict[str, Any]) -> None:
    """Writes metadata to a file.

    By default, writes data to a new directory named by the current timestamp.

    Creates a one-row metadata .csv file.
    """
    data = [metadata_dict]
    header_names = list(metadata_dict.keys())

    with open(NEW_SYNC_METADATA_FULL_FP, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header_names)
        writer.writeheader()
        writer.writerows(data)


def get_subreddit_data(subreddit: Subreddit) -> pd.DataFrame:
    """Given a `subreddit` object, get the subreddit data."""
    subreddit_dict = {}
    # prints name. Also has effect of fetching the subreddit, which
    # normally is only lazy loaded.
    print(f"Getting subreddit data for subreddit {subreddit.display_name} with id={subreddit.id}")
    for field, value in subreddit.__dict__.items():
        if is_json_serializable(value):
            subreddit_dict[field] = value
    subreddit_dict = add_enrichment_fields(subreddit_dict)
    return pd.DataFrame([subreddit_dict])


def get_comment_threads(
    subreddit: Subreddit,
    num_threads: int = DEFAULT_NUM_THREADS,
    thread_sort_type: Literal["hot", "new", "top", "controversial"] = "hot"
) -> list[Submission]:
    """Given a `subreddit` object, get the comment threads."""
    generator: ListingGenerator = None
    if thread_sort_type == "hot":
        generator = subreddit.hot(limit=num_threads)
    elif thread_sort_type == "new":
        generator = subreddit.new(limit=num_threads)
    elif thread_sort_type == "top":
        generator = subreddit.top(limit=num_threads)
    elif thread_sort_type == "controversial":
        generator = subreddit.controversial(limit=num_threads)
    else:
        raise ValueError(f"Unknown thread type: {thread_type}")
    return [thread for thread in generator]


def get_thread_data(thread: Submission) -> dict:
    """Given a `thread` object, get the thread data."""
    thread_dict = {}
    print(f"Getting information for thread with id={thread.id}")
    for field, value in thread.__dict__.items():
        if is_json_serializable(value):
            thread_dict[field] = value
    thread_dict = add_enrichment_fields(thread_dict)
    object_specific_enrichments_dict = object_specific_enrichments(thread)
    thread_dict = {
        **thread_dict,
        **object_specific_enrichments_dict
    }
    return thread_dict


def get_comment_data(comment: Comment) -> dict:
    """Given a `comment` object, get the comment data."""
    comment_dict = {}
    print(f"Getting information for comment with id={comment.id}")
    for field, value in comment.__dict__.items():
        if is_json_serializable(value):
            comment_dict[field] = value
    comment_dict = add_enrichment_fields(comment_dict)
    object_specific_enrichments_dict = object_specific_enrichments(comment)
    comment_dict = {
        **comment_dict,
        **object_specific_enrichments_dict
    }
    return comment_dict


def get_redditor_data(redditor: Redditor) -> dict:
    """Given a `redditor` object, get the redditor data."""
    redditor_dict = {}
    print(f"Getting information for redditor with id={redditor.id}")
    for field, value in redditor.__dict__.items():
        if is_json_serializable(value):
            redditor_dict[field] = value
    redditor_dict = add_enrichment_fields(redditor_dict)
    return redditor_dict


def parse_single_comment_data(
    comment: Comment
) -> tuple(list[dict], list[dict]):
    """Parses a `Comment` comment. Also recursively parses any nested child
    comments.
    
    Returns a tuple of the info for the users and the comments that are in this
    comment as well as any child comments.
    """
    users_list_info: list[dict] = []
    comments_list_info: list[dict] = []

    if comment.author.name in DENYLIST_AUTHORS:
        print(f"Skipping comment by author {comment.author}")
    else:
        author: Redditor = comment.author
        author_info = get_redditor_data(author)
        users_list_info.append(author_info)

        comment_info = get_comment_data(comment)
        comments_list_info.append(comment_info)

        # process replies as well. Each reply is its own Comment instance that
        # can also be processed recursively. The `replies` field will still
        # exist even for comments that don't have any replies.
        replies: CommentForest = comment.replies
        if len(replies) > 0:
            users_info, comments_info = parse_comments_data(replies)
            users_list_info.extend(users_info)
            comments_list_info.extend(comments_info)
            """
            for reply in replies:
                users_info, comments_info = parse_single_comment_data(reply)
                users_list_info.extend(users_info)
                comments_list_info.extend(comments_info)
            """

    return (users_list_info, comments_list_info)


def parse_comments_data(
    comments: CommentForest
) -> tuple(list[dict], list[dict]):
    # loop through all the comments
    # track comment info as well as user info
    # return two dicts
    users_list_info: list[dict] = []
    comments_list_info: list[dict] = []
    
    for comment in comments:
        users_info, comments_info = parse_single_comment_data(comment)
        users_list_info.extend(users_info)
        comments_list_info.extend(comments_info)

    return (users_list_info, comments_list_info)


def parse_comment_thread_data(
    thread: Submission
) -> tuple(dict, list[dict], list[dict]):
    """
    Parses a comment thread. Returns a tuple of dictionaries that contains info
    about the thread, the comments in the thread (and the comments to those
    comments), and the users in that comment thread.
    """
    thread_dict = get_thread_data(thread)

    comments: CommentForest = thread.comments
    users_list_dicts, comments_list_dicts = parse_comments_data(comments)

    return (thread_dict, users_list_dicts, comments_list_dicts)


def get_threads_data(
    threads: list[Submission]
) -> tuple(pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """Given a list of threads, get the thread, comment, and user info for the
    thread, all comments in the thread (and their children comments), and the
    users who were involved/commented in the comment threads.
    
    Returns a tuple of 3 dataframes, one for the thread info, one for info on
    the comments in the thread, and one for the users in the thread.
    """
    parsed_comment_threads_data = [
        parse_comment_thread_data(thread)
        for thread in threads
    ]
    threads_info_list: list[dict] = [data[0] for data in parsed_comment_threads_data]
    users_list_dicts: list[list[dict]] = [data[1] for data in parsed_comment_threads_data]
    comments_info_list: list[list[dict]] = [data[2] for data in parsed_comment_threads_data]

    # unnest the nested lists of users and comments
    unnested_users_list_dicts: list[dict] = []
    for users_list in users_list_dicts:
        unnested_users_list_dicts.extend(users_list)

    unnested_comments_list_dicts: list[dict] = []
    for comments_list in comments_info_list:
        unnested_comments_list_dicts.extend(comments_list)

    full_users_dict = {}

    # dedupe user dictionary.
    for user_dict in unnested_users_list_dicts:
        full_users_dict = {
            **full_users_dict,
            **{
                user_dict["id"]: user_dict
            }
        }

    # fetch only the items in each dictionary and add to a list.
    users_list = [item for item in full_users_dict.items()]

    # export
    threads_df = pd.DataFrame(threads_info_list)
    users_df = pd.DataFrame(users_list)
    comments_df = pd.DataFrame(unnested_comments_list_dicts)

    return (threads_df, users_df, comments_df)


def sync_comments_from_one_subreddit(
    api: Reddit,
    subreddit: str,
    num_threads: int = DEFAULT_NUM_THREADS,
    thread_sort_type: Literal["hot", "new", "top", "controversial"] = "hot"
) -> None:
    """Syncs the comments from one subreddit.
    
    Does so by grabbing threads and looking at the most recent
    comments in a given thread."""
    subreddit = api.subreddit(subreddit)

    subreddit_df = get_subreddit_data(subreddit)
    threads = get_comment_threads(
        subreddit=subreddit,
        num_threads=num_threads,
        thread_sort_type=thread_sort_type
    )
    threads_df, users_df, comments_df = get_threads_data(threads=threads)

    write_df_to_database(
        df=subreddit_df, table_name="subreddits"
    )
    write_df_to_database(
        df=users_df, table_name="users"
    )
    write_df_to_database(
        df=threads_df, table_name="threads"
    )
    write_df_to_database(
        df=comments_df, table_name="comments"
    )

    metadata_dict = {
        "subreddit": subreddit,
        "thread_sort_type": thread_sort_type,
        "num_comment_threads": threads_df.shape[0],
        "num_total_comments": comments_df.shape[0],
        "num_total_users": users_df.shape[0]
    }

    write_metadata_file(metadata_dict=metadata_dict)
    print(
        f"Finished syncing data from Reddit for timestamp {CURRENT_TIME_STR}"
    )
