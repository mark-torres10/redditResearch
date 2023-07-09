import os

from lib.helper import CODE_DIR

from message.helper import AUTHOR_DM_SUBJECT_LINE, OBSERVER_DM_SUBJECT_LINE

RESPONSES_ROOT_PATH = os.path.join(CODE_DIR, "get_responses")
SUBREDDITS_ROOT_PATH = os.path.join(RESPONSES_ROOT_PATH, "subreddits")
VALIDATED_RESPONSES_ROOT_PATH = os.path.join(
    RESPONSES_ROOT_PATH, "validated_responses_dir"
)
MESSAGED_OBSERVERS_PATH = os.path.join(
    RESPONSES_ROOT_PATH, "messaged_observers"
)
RESEARCH_PHASE_TO_SUBJECT_LINE_MAP = {
    "messaging": AUTHOR_DM_SUBJECT_LINE,
    "observer": OBSERVER_DM_SUBJECT_LINE
}

NUMBER_REGEX_PATTERN = r'\d+|one|two|three|four|five|six|seven|eight|nine|zero'

VALIDATED_RESPONSES_ROOT_FILENAME = "validated_responses"
SESSION_VALIDATED_RESPONSES_FILENAME = (
    f"{VALIDATED_RESPONSES_ROOT_FILENAME}" + "_{timestamp}.csv"
)
ALL_VALIDATED_RESPONSES_FILENAME = "validated_responses_all.csv"
HYDRATED_VALIDATED_RESPONSES_FILENAME = "hydrated_validated_responses.csv"

SUBREDDITS_TO_OBSERVE = ["r/politics", "r/Conservative", "r/Liberal"]
NUM_POSTS_PER_SUBREDDIT_TO_OBSERVE = 4

NUM_SUBREDDIT_USERS_TO_FETCH = 50

OBSERVER_DM_SUBJECT_LINE = "Yale Researchers Looking to Learn More About Your Beliefs" # noqa

OBSERVER_DM_SCRIPT = """
    Hi {name},

    I'm a researcher at Yale University, and my research group is interested in
    how people express themselves on social media. Would you like to answer a
    few questions to help us with our research? Your response will remain
    anonymous.

    The following message was posted in the {subreddit_name} subreddit on {date}:
    
    {post}

    (link {permalink})

    Please answer the following:

    1. How outraged did you think the message author was on a 1-7 scale?
    (1 = not at all, 4 = somewhat, 7 = very)

    2. How happy did you think the message author was on a 1-7?
    (1 = not at all, 4 = somewhat, 7 = very)

    You can simply respond with one answer per line such as:
    5
    1
""" # noqa
