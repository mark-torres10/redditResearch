"""Helper utilities for managing single subreddit sync."""
import csv
import os
import traceback
from typing import Any, Literal, Union

import pandas as pd
from praw.models.comment_forest import CommentForest
from praw.models.listing.generator import ListingGenerator
from praw.models.reddit.comment import Comment
from praw.models.reddit.more import MoreComments
from praw.models.reddit.redditor import Redditor
from praw.models.reddit.submission import Submission
from praw.models.reddit.subreddit import Subreddit
from praw.reddit import Reddit

from data.helper import dump_df_to_csv
from lib.db.sql.helper import write_df_to_database
from lib.helper import (
    CURRENT_TIME_STR, DENYLIST_AUTHORS,
    add_enrichment_fields,
    is_json_serializable,
)
from services.sync_single_subreddit.constants import (
    new_sync_metadata_dir, NEW_SYNC_METADATA_FULL_FP
)
from services.sync_single_subreddit.transformations import (
    field_specific_parsing, object_specific_enrichments
)

DEFAULT_NUM_THREADS = 5
DEFAULT_THREAD_SORT_TYPE = "hot"
DEFAULT_MAX_COMMENTS = 200


# metadata counters, for QA of syncs
previously_seen_threads = set()
previously_seen_comments = set()
previously_seen_users = set()
total_synced_comments = 0
total_comments = 0
total_users = 0
duplicate_authors = 0
duplicate_comments = 0
skipped_comments = 0
skipped_authors = 0

add_more_comments = True


def write_metadata_file(metadata_dict: dict[str, Any]) -> None:
    """Writes metadata to a file. By default, writes data to a new directory
    named by the current timestamp. Creates a one-row metadata .csv file.
    """
    os.makedirs(new_sync_metadata_dir)
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
        raise ValueError(f"Unknown thread type: {thread_sort_type}")
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
    parse_specific_fields_dict = field_specific_parsing(thread_dict)
    thread_dict = {
        **thread_dict,
        **object_specific_enrichments_dict,
        **parse_specific_fields_dict
    }
    return thread_dict


def get_comment_data(comment: Comment) -> dict:
    """Given a `comment` object, get the comment data."""
    comment_dict = {}
    previously_seen_comments.add(comment.id)
    for field, value in comment.__dict__.items():
        if is_json_serializable(value):
            comment_dict[field] = value
    comment_dict = add_enrichment_fields(comment_dict)
    object_specific_enrichments_dict = object_specific_enrichments(comment)
    parse_specific_fields_dict = field_specific_parsing(comment_dict)
    comment_dict = {
        **comment_dict,
        **object_specific_enrichments_dict,
        **parse_specific_fields_dict
    }
    return comment_dict


def get_redditor_data(redditor: Redditor) -> dict:
    """Given a `redditor` object, get the redditor data."""
    redditor_dict = {}
    previously_seen_users.add(redditor.id)
    for field, value in redditor.__dict__.items():
        if is_json_serializable(value):
            redditor_dict[field] = value
    redditor_dict = add_enrichment_fields(redditor_dict)
    object_specific_enrichments_dict = object_specific_enrichments(redditor)
    parse_specific_fields_dict = field_specific_parsing(redditor_dict)
    redditor_dict = {
        **redditor_dict,
        **object_specific_enrichments_dict,
        **parse_specific_fields_dict
    }

    # if any of "name", "is_employee", "id", "and "has_subscribed" aren't in
    # the author, flag this. This is just a random subset of fields that are in
    # the actual Redditor object but not in the generator version.
    if any(
        field not in redditor_dict
        for field in ["name", "is_employee", "id", "has_subscribed"]
    ):
        print('-' * 10)
        print(f"Redditor dict: {redditor_dict}")
        print("Missing fields in redditor dict...")
        print("Probably means it wasn't yielded correctly into memory...")
        print("Needs QA...")
        print('-' * 10)
    return redditor_dict


def parse_single_comment_data(
    comment: Comment
) -> tuple[list[dict], list[dict]]:
    """Parses a `Comment` comment. Also recursively parses any nested child
    comments.
    
    Returns a tuple of the info for the users and the comments that are in this
    comment as well as any child comments.
    """
    users_list_info: list[dict] = []
    comments_list_info: list[dict] = []

    global total_synced_comments
    global skipped_comments
    global skipped_authors
    global total_users
    global duplicate_authors
    global total_comments
    global duplicate_comments

    if comment.author is None:
        print(f"Comment {comment.id} has no author, meaning it was deleted. Skipping.") # noqa
        skipped_comments += 1
        skipped_authors += 1
    elif comment.author.name in DENYLIST_AUTHORS:
        print(f"Skipping comment by author {comment.author}")
        skipped_comments += 1
        skipped_authors += 1
    else:
        author: Redditor = comment.author
        if author.id not in previously_seen_users:
            author_info = get_redditor_data(author)
            users_list_info.append(author_info)
            total_users += 1
        else:
            print(f"Author with id={author.id} has already been seen. Skipping...") # noqa
            duplicate_authors += 1

        if comment.id not in previously_seen_comments:
            comment_info = get_comment_data(comment)
            comments_list_info.append(comment_info)
            total_comments += 1
        else:
            print(f"Comment with id={comment.id} has already been seen. Skipping...") # noqa
            duplicate_comments += 1

        total_synced_comments += 1

        # process replies as well. Each reply is its own Comment instance that
        # can also be processed recursively. The `replies` field will still
        # exist even for comments that don't have any replies.
        replies: CommentForest = comment.replies
        if len(replies) > 0:
            print(f"Need to process {len(replies)} replies...")
            users_info, comments_info = parse_comments_data(replies)
            users_list_info.extend(users_info)
            comments_list_info.extend(comments_info)

    return (users_list_info, comments_list_info)


def parse_morecomments_data(
    comment: MoreComments
) ->  tuple[list[dict], list[dict]]:
    """Parses a 'MoreComments' comment.
    
    Sometimes instead of getting a `Comment`, we get a `MoreComments` instance,
    which means that we need to load the actual comments first before doing any
    parsing. This loads those comments as a list of comments, then does
    the parsing.

    The `MoreComments` that we see represent the "Load more comments" or
    "Load more data" or "continue this thread" that a user would see while
    scrolling through the Reddit website.
    """
    more_comments: list[Comment] = comment.comments()
    return parse_comments_data(more_comments)


def parse_comments_data(
    comments: Union[CommentForest, list[Comment]],
    max_total_comments: int = DEFAULT_MAX_COMMENTS
) -> tuple[list[dict], list[dict]]:
    total_users_list_info: list[dict] = []
    total_comments_list_info: list[dict] = []

    global total_synced_comments
    global add_more_comments

    if not add_more_comments:
        print(f"Reached max total comments. Skipping...")
        return (total_users_list_info, total_comments_list_info)

    if total_synced_comments % 50 == 0:
        print('-' * 10)
        print(f"Synced comments: {total_synced_comments}")
        print(f"Desired number of comments: {max_total_comments}")
        print(f"Number of previously seen comments: {len(previously_seen_comments)}") # noqa
        print(f"Number of previously seen users: {len(previously_seen_users)}")
        print('-' * 10)

    if total_synced_comments > max_total_comments:
        print(f"Reached max total comments. Skipping...")
        add_more_comments = False
        return (total_users_list_info, total_comments_list_info)

    for comment in comments:
        if total_synced_comments > max_total_comments:
            print(f"Reached max total comments...")
            add_more_comments = False
            break
        if comment.id in previously_seen_comments:
            print(f"Already seen comment with id={comment.id}. Skipping...")
            continue
        if isinstance(comment, Comment):
            users_info, comments_info = parse_single_comment_data(comment)
            total_users_list_info.extend(users_info)
            total_comments_list_info.extend(comments_info)
        elif isinstance(comment, MoreComments):
            parse_morecomments_data(comment)
        else:
            comment_type = type(comment)
            print(f"Unknown comment class: {comment_type}. Skipping...")
            continue
    return (total_users_list_info, total_comments_list_info)


def parse_comment_thread_data(
    thread: Submission,
    max_total_comments: int = DEFAULT_MAX_COMMENTS
) -> tuple[dict, list[dict], list[dict]]:
    """
    Parses a comment thread. Returns a tuple of dictionaries that contains info
    about the thread, the comments in the thread (and the comments to those
    comments), and the users in that comment thread.
    """
    if thread.id in previously_seen_threads:
        print(f"Thread with id={thread.id} has already been seen. Skipping.")
        return ({}, [], [])
    if total_synced_comments > max_total_comments:
        print(f"Reached max total comments. Skipping thread {thread.id}")
        return ({}, [], [])

    thread_dict = get_thread_data(thread)
    previously_seen_threads.add(thread_dict["id"])
    comments: CommentForest = thread.comments
    users_list_dicts, comments_list_dicts = parse_comments_data(
        comments=comments, max_total_comments=max_total_comments
    )

    return (thread_dict, users_list_dicts, comments_list_dicts)


def get_threads_data(
    threads: list[Submission],
    max_total_comments: int = DEFAULT_MAX_COMMENTS
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Given a list of threads, get the thread, comment, and user info for the
    thread, all comments in the thread (and their children comments), and the
    users who were involved/commented in the comment threads.
    
    Returns a tuple of 3 dataframes, one for the thread info, one for info on
    the comments in the thread, and one for the users in the thread.
    """
    parsed_comment_threads_data = [
        parse_comment_thread_data(
            thread=thread, max_total_comments=max_total_comments
        )
        for thread in threads
    ]
    # get only threads that we actually processed, not any threads that we
    # skipped because we already had enough comments.
    threads_info_list: list[dict] = [
        data[0] for data in parsed_comment_threads_data if len(data[0]) > 0
    ]
    users_list_dicts: list[list[dict]] = [data[1] for data in parsed_comment_threads_data]
    comments_info_list: list[list[dict]] = [data[2] for data in parsed_comment_threads_data]

    # unnest the nested lists of users and comments
    unnested_users_list_dicts: list[dict] = []
    for users_list in users_list_dicts:
        unnested_users_list_dicts.extend(users_list)

    unnested_comments_list_dicts: list[dict] = []
    for comments_list in comments_info_list:
        unnested_comments_list_dicts.extend(comments_list)


    # export
    threads_df = pd.DataFrame(threads_info_list)
    users_df = pd.DataFrame(unnested_users_list_dicts)
    comments_df = pd.DataFrame(unnested_comments_list_dicts)

    return (threads_df, users_df, comments_df)


def filter_comments_by_users(
    comments_df: pd.DataFrame,
    users_df: pd.DataFrame
) -> pd.DataFrame:
    """Filters out any comments whose author isn't in our users df.
    
    We want to maintain data integrity and there appears to be some edge cases
    where we get comments but we don't get the corresponding author id.

    So, we filter out any comments whose "author_id" value isn't in the "id"
    column of users_df.
    """
    users_ids = set(users_df["id"].unique())
    comments_df = comments_df[comments_df["author_id"].isin(users_ids)]
    return comments_df


def sync_comments_from_one_subreddit(
    api: Reddit,
    subreddit: str,
    num_threads: int = DEFAULT_NUM_THREADS,
    max_total_comments: int = DEFAULT_MAX_COMMENTS,
    thread_sort_type: Literal["hot", "new", "top", "controversial"] = "hot",
    objects_to_sync: list[str] = ["subreddits", "threads", "users", "comments"]
) -> None:
    """Syncs the comments from one subreddit.
    
    Does so by grabbing threads and looking at the most recent
    comments in a given thread."""
    subreddit = api.subreddit(subreddit)
    subreddit_df = get_subreddit_data(subreddit)   
    
    if len(objects_to_sync) == 1 and objects_to_sync[0] == "subreddits":
        print("Only syncing subreddit data. Skipping comments...")
        print("Dumping updated subreddit data to .csv file and writing to DB...") # noqa
        dump_df_to_csv(df=subreddit_df, table_name="subreddits")
        write_df_to_database(df=subreddit_df, table_name="subreddits")
        metadata_dict = {"subreddit": subreddit, "num_total_comments": 0}
        write_metadata_file(metadata_dict=metadata_dict)
        print(
            f"Finished syncing data from Reddit for timestamp {CURRENT_TIME_STR}" # noqa
        )
        return

    threads = get_comment_threads(
        subreddit=subreddit,
        num_threads=num_threads,
        thread_sort_type=thread_sort_type
    )

    try:
        threads_df, users_df, comments_df = get_threads_data(
            threads=threads,
            max_total_comments=max_total_comments
        )
        comments_df = filter_comments_by_users(
            comments_df=comments_df,
            users_df=users_df
        )
    except Exception as e:
        print(f"Unable to sync reddit data: {e}")
        traceback.print_exc()

    print("Successfully synced data from Reddit. Now writing to DB...")

    try:
        for sync_object in objects_to_sync:
            if sync_object == "subreddits":
                print("Dumping subreddits to .csv, writing to DB...")
                write_df_to_database(df=subreddit_df, table_name="subreddits", upsert=True)
                dump_df_to_csv(df=subreddit_df, table_name="subreddits")
            elif sync_object == "users":
                print("Dumping users to .csv, writing to DB...")
                write_df_to_database(df=users_df, table_name="users", upsert=True)
                dump_df_to_csv(df=users_df, table_name="users")
            elif sync_object == "threads":
                print("Dumping threads to .csv, writing to DB...")
                write_df_to_database(df=threads_df, table_name="threads", upsert=True)
                dump_df_to_csv(df=threads_df, table_name="threads")
            elif sync_object == "comments":
                print("Dumping comments to .csv, writing to DB...")
                write_df_to_database(df=comments_df, table_name="comments", upsert=True)
                dump_df_to_csv(df=comments_df, table_name="comments")

    except Exception as e:
        print(f"unable to write data to database: {e}")
        traceback.print_exc()

    metadata_dict = {
        "subreddit": subreddit,
        "thread_sort_type": thread_sort_type,
        "max_total_comments": max_total_comments,
        "num_comment_threads": threads_df.shape[0],
        "num_total_comments": comments_df.shape[0],
        "num_total_users": users_df.shape[0],
        "num_duplicate_authors": duplicate_authors,
        "num_duplicate_comments": duplicate_comments,
        "num_skipped_comments": skipped_comments,
        "num_skipped_authors": skipped_authors
    }
    write_metadata_file(metadata_dict=metadata_dict)
    print(
        f"Finished syncing data from Reddit for timestamp {CURRENT_TIME_STR}"
    )
