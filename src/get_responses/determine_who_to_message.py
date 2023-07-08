"""Given the subreddits present in the validated messages that we've received
from the author phase, determine who to message for the observer phase.

We can randomly message an active subset of the subreddit.
"""


if __name__ == "__main__":
    # organize the "hydrated_validated_responses.csv" file by subreddit.

    # for each subreddit, pick 4 validated responses that we want observers
    # to respond to.

    # for each subreddit, get a random list of 50 users in the subreddit
    # who have been active in the past 48 hours. Filter out anyone who
    # we have messaged previously. Add this list as a column in the dataset.

    # create a new df for each subreddit.

    # for each of the new dfs, make each row unique on the subreddit name,
    # the post id, and one of the IDs in the list of users to message. Also
    # include the original post body, permalink, and created date.

    # dump each new df into a .csv file, whose name contains the subreddit
    # name as well as a timestamp.

    # in a new file, for each of these .csv files we would create the
    # message to send and then message the users.

    pass