DEFAULT_NUMBER_OF_OBSERVERS_PER_COMMENT = 30
DEFAULT_COMMENT_LIMIT = 150 # number of comments to use for observer phase
DEFAULT_OBSERVER_LIMIT = DEFAULT_NUMBER_OF_OBSERVERS_PER_COMMENT * DEFAULT_COMMENT_LIMIT # noqa
OBSERVER_DM_SUBJECT_LINE = "Yale Researchers Looking to Learn More About Your Beliefs" # noqa
OBSERVER_DM_SCRIPT = """
Hi {name},

My research group at Yale University is interested in how people express \
themselves on social media. Would you like to answer a few questions to help \
us with our research? Your response will remain anonymous.

The following message was posted in the {subreddit} subreddit on {date}:

{post}

(link {permalink})

Please answer the following:

1. How outraged did you think the message author was on a 1-7 scale?
(1 = not at all, 4 = somewhat, 7 = very)

2. How happy did you think the message author was on a 1-7 scale?
(1 = not at all, 4 = somewhat, 7 = very)

You can simply respond with one answer per line such as:
5
1

Thank you for helping us learn more about how people engage with others on \
social media! Please feel free to message us with any questions or if you're \
interested in learning more about this research.
""" # noqa

table_name = "comment_to_observer_map"
