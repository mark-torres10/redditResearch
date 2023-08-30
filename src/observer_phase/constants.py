"""Constants for observer phase of logic."""
import os

from message import constants as message_constants

# number of days ago that the post was created. We want to use relatively
# recent posts.
POST_RECENCY_NUM_DAYS = 3

# number of posts to use from a given sync. 
NUM_POSTS_USED_PER_SYNC = 6

PREVIOUS_CONSOLIDATED_MESSAGES_FILENAME = os.path.join(
    message_constants.MESSAGES_ROOT_PATH,
    message_constants.CONSOLIDATED_MESSAGES_FILE_NAME
)
